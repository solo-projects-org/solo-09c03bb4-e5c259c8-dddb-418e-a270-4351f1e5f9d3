"""SQLite 访问层。

双实例（多进程）共享同一个数据库文件：
- WAL 模式 + NORMAL 同步，保证多进程一写多读不互斥阻塞；
- 所有写事务使用 BEGIN IMMEDIATE，配合 busy_timeout 将写串行化，
  避免 SQLITE_BUSY 直接打到请求上；
- 每个进程一个连接（uvicorn worker 内的多线程通过 RLock 串行使用）。
"""

from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager

SCHEMA_VERSION = 2

_SCHEMA = """
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id    TEXT PRIMARY KEY,
    station_id    TEXT NOT NULL,
    obs_date      TEXT NOT NULL,
    declared_size INTEGER NOT NULL CHECK (declared_size >= 0),
    declared_sha256 TEXT NOT NULL,
    state         TEXT NOT NULL CHECK (state IN ('UPLOADING', 'PUBLISHED')),
    bytes_received INTEGER NOT NULL DEFAULT 0,
    actual_sha256  TEXT,
    blob_sha256    TEXT,
    row_count      INTEGER,
    publication_id TEXT,
    last_error     TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_receipts_state      ON receipts(state);
CREATE INDEX IF NOT EXISTS idx_receipts_station_day ON receipts(station_id, obs_date);

-- 内容寻址成品：sha256 唯一；ref_count 是引用它的已发布接收数
CREATE TABLE IF NOT EXISTS blobs (
    sha256     TEXT PRIMARY KEY,
    size       INTEGER NOT NULL CHECK (size >= 0),
    ref_count  INTEGER NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 下游同步变更事件流：只追加，永不更新/删除。
-- seq 单调递增即游标；每次发布/撤回在状态变更的同一事务内插入，
-- 崩溃只会两者同时回滚，不会出现“状态变了没事件”或“有事件状态没变”。
-- publication_id 标识一次具体发布：同号删除后重新入湖即使摘要相同，
-- 撤回事件指向旧 publication_id，消费者按序应用不会误撤新件。
CREATE TABLE IF NOT EXISTS changes (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    event          TEXT NOT NULL CHECK (event IN ('PUBLISHED', 'WITHDRAWN')),
    receipt_id     TEXT NOT NULL,
    station_id     TEXT NOT NULL,
    obs_date       TEXT NOT NULL,
    sha256         TEXT NOT NULL,
    row_count      INTEGER NOT NULL,
    publication_id TEXT NOT NULL,
    reason         TEXT NOT NULL CHECK (reason IN ('published', 'receipt_deleted', 'blob_missing')),
    recorded_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 事件一经写入不可变：数据库层禁止更新与删除
CREATE TRIGGER IF NOT EXISTS changes_no_update BEFORE UPDATE ON changes
BEGIN SELECT RAISE(ABORT, 'changes 事件流为只追加，禁止更新'); END;

CREATE TRIGGER IF NOT EXISTS changes_no_delete BEFORE DELETE ON changes
BEGIN SELECT RAISE(ABORT, 'changes 事件流为只追加，禁止删除'); END;

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def insert_change_event(
    conn: sqlite3.Connection,
    *,
    event: str,
    receipt_id: str,
    station_id: str,
    obs_date: str,
    sha256: str,
    row_count: int,
    publication_id: str,
    reason: str,
) -> None:
    """在当前事务内追加一条变更事件（调用方负责与状态变更同事务）。

    事件快照当时的接收号、站号、UTC 日期、SHA-256 与行数；写入后不可变
    （changes_no_update / changes_no_delete 触发器强制只追加）。
    """
    conn.execute(
        "INSERT INTO changes(event, receipt_id, station_id, obs_date, sha256, "
        "row_count, publication_id, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (event, receipt_id, station_id, obs_date, sha256, row_count,
         publication_id, reason),
    )


class Database:
    """进程级单例式连接封装。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = self._connect()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            f"file:{self.db_path}?uri=true",
            uri=True,
            check_same_thread=False,
            isolation_level=None,  # 事务完全由代码显式管理
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            # _SCHEMA 全部使用 IF NOT EXISTS：新库全量建表，旧库只补新表/触发器
            self._conn.executescript(_SCHEMA)
            row = self._conn.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
                return
            version = int(row["value"])
            if version > SCHEMA_VERSION:  # pragma: no cover - 防御性
                raise RuntimeError(
                    f"数据库 schema 版本 {version} 高于程序 {SCHEMA_VERSION}，请升级程序"
                )
            if version < SCHEMA_VERSION:
                self._migrate(version)
                self._conn.execute(
                    "UPDATE schema_meta SET value = ? WHERE key = 'schema_version'",
                    (str(SCHEMA_VERSION),),
                )

    def _migrate(self, from_version: int) -> None:
        """逐版本迁移。v1 → v2：receipts 增加 publication_id 并回填已发布行。

        变更事件流自启用时起记录：迁移不给历史已发布接收补发 PUBLISHED 事件，
        只补 publication_id，使其后的撤回事件仍能指向确定的一次发布。
        """
        if from_version < 2:
            cols = {
                r["name"]
                for r in self._conn.execute("PRAGMA table_info(receipts)").fetchall()
            }
            if "publication_id" not in cols:
                self._conn.execute(
                    "ALTER TABLE receipts ADD COLUMN publication_id TEXT"
                )
            self._conn.execute(
                "UPDATE receipts SET publication_id = lower(hex(randomblob(16))) "
                "WHERE state = 'PUBLISHED' AND publication_id IS NULL"
            )

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        """串行化写事务上下文。

        immediate=True 使用 BEGIN IMMEDIATE，进入即取写锁，
        配合 busy_timeout 实现跨进程写串行化。
        """
        with self._lock:
            if immediate:
                self._conn.execute("BEGIN IMMEDIATE")
            else:
                self._conn.execute("BEGIN")
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params or ()).fetchone()

    def query_all(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, params or ()).fetchall())

    def close(self) -> None:
        with self._lock:
            self._conn.close()
