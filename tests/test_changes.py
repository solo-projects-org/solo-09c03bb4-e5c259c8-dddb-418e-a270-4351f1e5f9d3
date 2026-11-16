"""变更事件流（GET /changes）测试。

覆盖需求要点：
- 发布/撤回事件保留当时的接收号、站号、UTC 日期、SHA-256、行数；
- 共享原件缺失时对账撤回所有关联接收；
- 同号删除后重新入湖（即使摘要相同）先撤回旧件再发布新件，
  消费者按序应用不误撤新件（publication_id 区分两次发布）；
- 未成功的变更不产生事件；已生效的变更不漏记、不重复（含重试与并发）；
- 事件一经返回不变，后来的变更只追加在其后（游标分页稳定）；
- 双连接（模拟双实例）共享同一事件流；v1 旧库平滑迁移。
"""

from __future__ import annotations

import pathlib
import sqlite3
import threading

import pytest

from conftest import declare_payload, make_parquet
from app import main as main_mod
from app.config import Settings
from app.db import Database
from app.storage import blob_path, reconcile


def _decl(client, receipt_id, data, **kw):
    return client.put(
        f"/receipts/{receipt_id}/declaration",
        json=declare_payload(receipt_id, data, **kw),
    )


def _publish(client, receipt_id, data, **kw):
    assert _decl(client, receipt_id, data, **kw).status_code == 200
    r = client.post(f"/receipts/{receipt_id}/content", content=data)
    assert r.status_code == 200, r.text
    assert r.json()["state"] == "PUBLISHED"
    return r


def _all_events(client, page_size=1000):
    """walk 全部页，返回事件列表（同时校验翻页本身不多不漏）。"""
    events, cursor = [], 0
    while True:
        r = client.get("/changes", params={"cursor": cursor, "limit": page_size})
        assert r.status_code == 200, r.text
        body = r.json()
        events.extend(body["events"])
        cursor = body["next_cursor"]
        if not body["has_more"]:
            return events


def test_publish_emits_event_with_snapshot(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet", rows=24)
    _publish(client, "RCV-CH1", data, station_id="BUOY-001", obs_date="2026-11-15")

    r = client.get("/changes")
    assert r.status_code == 200
    body = r.json()
    assert body["has_more"] is False
    assert len(body["events"]) == 1
    ev = body["events"][0]
    assert ev["seq"] == 1
    assert ev["event"] == "PUBLISHED"
    assert ev["receipt_id"] == "RCV-CH1"
    assert ev["station_id"] == "BUOY-001"
    assert ev["obs_date"] == "2026-11-15"
    assert ev["sha256"] == helpers.sha256_of(data)
    assert ev["row_count"] == 24
    assert ev["reason"] == "published"
    assert ev["publication_id"]
    assert ev["recorded_at"]
    assert body["next_cursor"] == ev["seq"]


def test_unsuccessful_changes_emit_nothing(client, tmp_path, helpers):
    # 仅声明不发布
    data = make_parquet(tmp_path / "a.parquet")
    assert _decl(client, "RCV-N1", data).status_code == 200
    # 摘要不符发布失败
    bad = declare_payload("RCV-N2", data)
    bad["sha256"] = "a" * 64
    assert client.put("/receipts/RCV-N2/declaration", json=bad).status_code == 200
    assert client.post("/receipts/RCV-N2/content", content=data).status_code == 422
    # 非法 Parquet 发布失败
    garbage = b"NOT PARQUET" * 8
    assert _decl(client, "RCV-N3", garbage).status_code == 200
    assert client.post("/receipts/RCV-N3/content", content=garbage).status_code == 422
    # 中止在途接收
    assert client.post("/receipts/RCV-N1/abort").status_code == 200
    # 删除从未发布的接收
    assert client.delete("/receipts/RCV-N3").status_code == 204

    assert _all_events(client) == []


def test_quota_rejected_publish_emits_nothing(client, tmp_path, monkeypatch):
    d1 = make_parquet(tmp_path / "a.parquet", salt=0.0)
    d2 = make_parquet(tmp_path / "b.parquet", salt=5.0)
    base = main_mod.settings
    tight = Settings(
        data_dir=base.data_dir, db_path=base.db_path, staging_dir=base.staging_dir,
        blob_dir=base.blob_dir, lock_dir=base.lock_dir,
        quota_bytes=len(d1) + len(d2) - 1,
        instance_id="test", reconcile_interval=3600, chunk_size=1024,
    )
    monkeypatch.setattr(main_mod, "settings", tight)
    _publish(client, "RCV-QP1", d1)
    assert _decl(client, "RCV-QP2", d2).status_code == 200
    r = client.post("/receipts/RCV-QP2/content", content=d2)
    assert r.status_code == 507  # 发布被额度拒绝

    events = _all_events(client)
    assert [e["event"] for e in events] == ["PUBLISHED"]
    assert events[0]["receipt_id"] == "RCV-QP1"


def test_delete_published_emits_withdraw(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-CH2", data)
    assert client.delete("/receipts/RCV-CH2").status_code == 204
    # 删除重试：记录已不存在，404，不再产生事件
    assert client.delete("/receipts/RCV-CH2").status_code == 404

    events = _all_events(client)
    assert [e["event"] for e in events] == ["PUBLISHED", "WITHDRAWN"]
    pub, wd = events
    assert wd["reason"] == "receipt_deleted"
    assert wd["publication_id"] == pub["publication_id"]
    assert wd["sha256"] == pub["sha256"] == helpers.sha256_of(data)
    assert wd["row_count"] == pub["row_count"] == 24
    assert wd["station_id"] == pub["station_id"]
    assert wd["obs_date"] == pub["obs_date"]
    assert wd["seq"] > pub["seq"]


def test_reingest_same_digest_after_delete_withdraws_then_publishes(
    client, tmp_path, helpers
):
    """同号删除后重新入湖：即使摘要相同，也先撤回旧件再发布新件。"""
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-RE1"
    _publish(client, rid, data)
    assert client.delete(f"/receipts/{rid}").status_code == 204
    # 完全相同的声明与内容重新入湖
    _publish(client, rid, data)

    events = _all_events(client)
    assert [e["event"] for e in events] == ["PUBLISHED", "WITHDRAWN", "PUBLISHED"]
    p1, w, p2 = events
    # 两次发布摘要相同，但发布标识不同；撤回指向旧发布
    assert p1["sha256"] == p2["sha256"] == w["sha256"]
    assert p1["publication_id"] != p2["publication_id"]
    assert w["publication_id"] == p1["publication_id"]
    assert p1["seq"] < w["seq"] < p2["seq"]

    # 模拟下游消费者按序应用：撤回只命中对应 publication_id，不误撤新件
    directory: dict[str, str] = {}
    for e in events:
        if e["event"] == "PUBLISHED":
            directory[e["receipt_id"]] = e["publication_id"]
        elif directory.get(e["receipt_id"]) == e["publication_id"]:
            del directory[e["receipt_id"]]
    assert directory == {rid: p2["publication_id"]}


def test_retry_and_redeclare_no_duplicate_events(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-CH3"
    _publish(client, rid, data)
    # 已发布后重传（幂等成功）与重复声明都不重复记事件
    assert client.post(f"/receipts/{rid}/content", content=data).status_code == 200
    assert _decl(client, rid, data).status_code == 200

    events = _all_events(client)
    assert [e["event"] for e in events] == ["PUBLISHED"]


def test_concurrent_same_receipt_publish_records_exactly_one_event(
    client, tmp_path, helpers
):
    """并发/重试上传同一接收：只生效一次发布，事件流只有一条 PUBLISHED。"""
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-CNC1"
    assert _decl(client, rid, data).status_code == 200

    def worker():
        client.post(f"/receipts/{rid}/content", content=data)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert client.get(f"/receipts/{rid}").json()["state"] == "PUBLISHED"
    events = _all_events(client)
    assert [e["event"] for e in events] == ["PUBLISHED"]


def test_concurrent_distinct_receipts_each_exactly_one_event(client, tmp_path, helpers):
    payloads = {}
    for i in range(5):
        d = make_parquet(tmp_path / f"p{i}.parquet", salt=i * 0.7)
        rid = f"RCV-CND{i}"
        assert _decl(client, rid, d).status_code == 200
        payloads[rid] = d

    def worker(rid, data):
        r = client.post(f"/receipts/{rid}/content", content=data)
        assert r.status_code == 200, r.text

    threads = [threading.Thread(target=worker, args=(rid, d))
               for rid, d in payloads.items()]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    events = _all_events(client)
    assert len(events) == 5
    assert all(e["event"] == "PUBLISHED" for e in events)
    assert {e["receipt_id"] for e in events} == set(payloads)
    seqs = [e["seq"] for e in events]
    assert seqs == sorted(seqs) and len(set(seqs)) == 5
    assert len({e["publication_id"] for e in events}) == 5


def test_reconcile_missing_shared_blob_withdraws_all_associated(
    client, settings, database, tmp_path, helpers
):
    """共享原件缺失：对账撤回所有关联接收，且只撤回一次。"""
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-SH1", data)
    _publish(client, "RCV-SH2", data)  # 同内容，共享同一 blob
    sha = helpers.sha256_of(data)

    pathlib.Path(blob_path(settings, sha)).unlink()  # 模拟共享原件丢失
    result = reconcile(settings, database)
    assert result["reset_receipts"] == 2

    events = _all_events(client)
    assert [e["event"] for e in events] == ["PUBLISHED", "PUBLISHED",
                                           "WITHDRAWN", "WITHDRAWN"]
    pubs = {e["receipt_id"]: e for e in events[:2]}
    wds = events[2:]
    assert {e["receipt_id"] for e in wds} == {"RCV-SH1", "RCV-SH2"}
    for e in wds:
        assert e["reason"] == "blob_missing"
        assert e["sha256"] == sha
        assert e["row_count"] == 24  # 快照保留当时的行数
        # 撤回指向各自当时的发布
        assert e["publication_id"] == pubs[e["receipt_id"]]["publication_id"]

    # 再次对账：已撤回的不会重复记事件
    reconcile(settings, database)
    assert len(_all_events(client)) == 4

    # 缺件撤回后可重新入湖：新发布追加新事件，使用新发布标识
    _publish(client, "RCV-SH1", data)
    events = _all_events(client)
    assert len(events) == 5
    assert events[-1]["event"] == "PUBLISHED"
    assert events[-1]["receipt_id"] == "RCV-SH1"
    assert events[-1]["publication_id"] != pubs["RCV-SH1"]["publication_id"]


def test_reconcile_reset_of_uploading_emits_nothing(
    client, settings, database, tmp_path, helpers
):
    """写盘/登记中断（UPLOADING 缺暂存）被对账回退：从未发布，不产生事件。"""
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-NP1"
    assert _decl(client, rid, data).status_code == 200
    with database.transaction() as conn:
        conn.execute(
            "UPDATE receipts SET bytes_received = ? WHERE receipt_id = ?",
            (len(data), rid),
        )
    result = reconcile(settings, database)
    assert result["reset_receipts"] == 1
    assert _all_events(client) == []


def test_cursor_pagination_and_immutability(client, tmp_path, helpers):
    for i in range(5):
        d = make_parquet(tmp_path / f"p{i}.parquet", salt=i * 1.3)
        _publish(client, f"RCV-PG{i}", d)

    # 逐页翻完：2/2/1
    page1 = client.get("/changes", params={"cursor": 0, "limit": 2}).json()
    assert [e["seq"] for e in page1["events"]] == [1, 2]
    assert page1["has_more"] is True and page1["next_cursor"] == 2
    page2 = client.get("/changes", params={"cursor": 2, "limit": 2}).json()
    assert [e["seq"] for e in page2["events"]] == [3, 4]
    page3 = client.get("/changes", params={"cursor": 4, "limit": 2}).json()
    assert [e["seq"] for e in page3["events"]] == [5]
    assert page3["has_more"] is False and page3["next_cursor"] == 5
    # 空页：游标不变
    page4 = client.get("/changes", params={"cursor": 5, "limit": 2}).json()
    assert page4["events"] == [] and page4["next_cursor"] == 5

    # 事件一经返回不变：后续变更只追加，不改已返回内容
    d = make_parquet(tmp_path / "late.parquet", salt=9.9)
    _publish(client, "RCV-PG-LATE", d)
    again = client.get("/changes", params={"cursor": 0, "limit": 2}).json()
    assert again["events"] == page1["events"]
    tail = client.get("/changes", params={"cursor": 5, "limit": 10}).json()
    assert [e["receipt_id"] for e in tail["events"]] == ["RCV-PG-LATE"]
    assert tail["events"][0]["seq"] == 6


def test_events_are_append_only_at_db_level(client, database, tmp_path):
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-IM1", data)
    with pytest.raises(sqlite3.IntegrityError):
        with database.transaction() as conn:
            conn.execute("UPDATE changes SET station_id = 'X' WHERE seq = 1")
    with pytest.raises(sqlite3.IntegrityError):
        with database.transaction() as conn:
            conn.execute("DELETE FROM changes WHERE seq = 1")
    # 触发器拦截后事件原样还在
    assert len(_all_events(client)) == 1


def test_events_shared_across_database_connections(settings, client, database, tmp_path):
    """模拟双实例：一个连接写入的事件，另一个连接立即可读。"""
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-XINST", data)
    other = Database(settings.db_path)
    try:
        rows = other.query_all("SELECT * FROM changes ORDER BY seq")
        assert len(rows) == 1
        assert rows[0]["receipt_id"] == "RCV-XINST"
        assert rows[0]["event"] == "PUBLISHED"
    finally:
        other.close()


_V1_SCHEMA = """
CREATE TABLE receipts (
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
    last_error     TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE TABLE blobs (
    sha256     TEXT PRIMARY KEY,
    size       INTEGER NOT NULL CHECK (size >= 0),
    ref_count  INTEGER NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO schema_meta(key, value) VALUES ('schema_version', '1');
"""


def test_migration_from_v1_database(settings, tmp_path):
    """v1 旧库启用新版：平滑迁移；事件自启用时起记录，历史件可撤回。"""
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_V1_SCHEMA)
    conn.execute(
        "INSERT INTO receipts(receipt_id, station_id, obs_date, declared_size, "
        "declared_sha256, state, bytes_received, blob_sha256, row_count) "
        "VALUES ('RCV-OLD1', 'BUOY-001', '2026-11-10', 100, ?, 'PUBLISHED', 100, ?, 7)",
        ("b" * 64, "b" * 64),
    )
    conn.execute(
        "INSERT INTO blobs(sha256, size, ref_count) VALUES (?, 100, 1)", ("b" * 64,)
    )
    conn.commit()
    conn.close()

    db = Database(str(db_path))
    try:
        version = db.query_one(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        )["value"]
        assert version == "2"
        # 迁移回填了发布标识，但不补发历史 PUBLISHED 事件（自启用时起记录）
        row = db.query_one("SELECT * FROM receipts WHERE receipt_id = 'RCV-OLD1'")
        assert row["publication_id"]
        assert db.query_all("SELECT * FROM changes") == []

        # 启用后删除历史已发布接收：撤回事件照常登记，指向回填的发布标识
        from app.service import delete_receipt

        delete_receipt(settings, db, "RCV-OLD1")
        events = db.query_all("SELECT * FROM changes ORDER BY seq")
        assert len(events) == 1
        assert events[0]["event"] == "WITHDRAWN"
        assert events[0]["reason"] == "receipt_deleted"
        assert events[0]["publication_id"] == row["publication_id"]
        assert events[0]["row_count"] == 7
        assert events[0]["obs_date"] == "2026-11-10"
    finally:
        db.close()
