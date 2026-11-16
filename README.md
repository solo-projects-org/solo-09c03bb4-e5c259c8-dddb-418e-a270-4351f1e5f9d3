# 海洋数仓 · 浮标观测包接收服务

浮标站每日 Parquet 观测包入湖的接收 API。支持**双实例**共享同一存储、
**内容寻址去重**、**字节配额**、**崩溃自动回收**、完整的**接收进度查询/原件下载**，
以及供下游数仓同步的**变更事件流**（发布/撤回，游标分页，自启用时起记录）。

技术栈：FastAPI + PyArrow + SQLite（WAL）+ uvicorn，全部阻塞 I/O 在线程池执行，
慢上传不阻塞其他接收与查询。

---

## 1. 核心能力与需求对应

| 需求 | 实现 |
| --- | --- |
| 按接收号声明站号、UTC 日期、长度、SHA-256，再上传原包 | `PUT /receipts/{id}/declaration` → `POST /receipts/{id}/content` |
| 包内含站号、UTC 时刻、盐度三列，逐行站号/站日符合声明 | `app/validation.py` 用 PyArrow 逐行校验，保留全部原始行（只读不改写） |
| 每次接收独立留档，可查进度、下载原件 | `receipts` 表每接收一行；`GET /receipts`、`GET /receipts/{id}`、`.../download` |
| 同号同声明重试沿用原记录，异声明拒绝 | 声明幂等；四要素任一不同返回 `409 declaration_conflict` |
| 配置原包字节额度，暂存与成品共用 | `OBS_QUOTA_BYTES`；口径 = 成品 blob + 在途暂存 |
| 相同内容跨接收只存一份、不重复占额 | 内容寻址 `blobs/<sha 前2位>/<sha>` + `ref_count`，硬链接发布 |
| 空间不足明确拒收 | `507 quota_exceeded`，落盘/发布前按实际内容核算 |
| 长度、摘要、内容均通过才发布，失败无可下载成品 | 三关依次校验；失败保持 `UPLOADING`，下载返回 `409` |
| 中断后无用文件及占额自动回收，未完成可重传 | 启动 + 周期 + 手动 `reconcile`，清理孤儿暂存/成品、回退缺件记录 |
| 恢复不误删另一实例在途/已发布原包，不留缺件成功记录 | 全局 flock + 写事务对账；在途暂存按 DB 归属保留，缺件成品回退而非掩盖 |
| 慢上传期间其他包仍可接收、查询 | 网络读取异步、阻塞重活进 I/O 线程池；接收级 flock 只锁同一接收号 |
| 下游同步：发布/撤回事件流，游标分页，自启用时起记录 | `GET /changes`；事件与状态变更同事务写入，只追加、不可变 |
| 接收删除须撤回；共享原件缺失须撤回所有关联接收 | 删除与对账在同一事务内登记 `WITHDRAWN` 事件 |
| 同号删除后重新入湖（即使摘要相同）先撤回再发布，不误撤新件 | 每次发布生成唯一 `publication_id`，撤回事件指向旧发布 |
| 可启动的双实例 Docker Compose 与 Swagger 入口 | `docker-compose.yml`（app1/app2 + nginx），`/docs` Swagger |

---

## 2. 目录结构

```
app/
  config.py      环境变量配置（数据目录、额度、对账周期等）
  db.py          SQLite 连接、WAL、BEGIN IMMEDIATE 写事务、建表与版本迁移、事件登记
  errors.py      领域异常与 HTTP 状态码映射
  schemas.py     Pydantic 请求/响应模型
  storage.py     flock 文件锁、内容寻址 blob、暂存、配额、崩溃对账（缺件撤回登记）
  validation.py  PyArrow Parquet 三列与站号/站日逐行校验
  service.py     声明/上传/发布/中止/删除编排、变更事件流读取，I/O 线程池
  main.py        FastAPI 路由、Swagger、后台对账、异常处理
tests/           pytest 单元/并发/回收/事件流测试 + 自研同步 ASGI 客户端
scripts/
  run_local.sh       无 Docker 时本机双实例启动
  integration_test.py 真实双进程端到端自测
Dockerfile  docker-compose.yml  nginx.conf  requirements.txt
```

数据卷布局（双实例共享）：

```
$OBS_DATA_DIR/
  obs.db                 SQLite（WAL：obs.db-wal / obs.db-shm）
  staging/<receipt>.part 在途暂存，按接收号命名
  blobs/<xx>/<sha256>    内容寻址成品
  locks/rcpt-<id>.lock   接收级互斥锁
  locks/global.lock      发布/对账全局锁
```

---

## 3. 快速开始

### 方式 A：Docker Compose（推荐，双实例）

```bash
docker compose up -d --build
# 统一入口（nginx 轮询到 app1/app2）
# Swagger:  http://localhost:8080/docs
# OpenAPI:  http://localhost:8080/openapi.json
# 健康检查:  http://localhost:8080/health
```

调整额度：`OBS_QUOTA_BYTES=10485760 docker compose up -d`（默认 1 GiB）。

### 方式 B：本机双进程（无 Docker 环境，自测用）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
./scripts/run_local.sh
# 实例1 http://127.0.0.1:8001/docs ；实例2 http://127.0.0.1:8002/docs
```

---

## 4. API 流程

### 第一步：声明

```bash
curl -X PUT http://localhost:8080/receipts/RCV-0001/declaration \
  -H 'Content-Type: application/json' \
  -d '{"receipt_id":"RCV-0001","station_id":"BUOY-001",
       "obs_date":"2026-11-15","size":1488,
       "sha256":"<64位十六进制>"}'
```

- 同号同声明重复调用 → 幂等返回同一记录；
- 同号异声明（站号/日期/长度/摘要任一不同）→ `409`；
- 声明长度超过总额度 → `507`。

### 第二步：上传原包（body 即 Parquet 原始字节）

```bash
curl -X POST http://localhost:8080/receipts/RCV-0001/content \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @BUOY-001_2026-11-15.parquet
```

依次通过 **长度 → SHA-256 → Parquet 三列与逐行站号/站日** 三关后发布为
`PUBLISHED`；任一失败返回 `422`，接收保持 `UPLOADING` 可重传，且**不产生成品**。

### 查询与下载

```bash
curl http://localhost:8080/receipts                         # 列表，可 ?state=&station_id=
curl http://localhost:8080/receipts/RCV-0001                # 单接收进度
curl -O -J http://localhost:8080/receipts/RCV-0001/download # 下载原件（未发布 409）
curl http://localhost:8080/quota                            # 额度占用
curl -X POST http://localhost:8080/admin/reconcile          # 手动触发回收对账
```

辅助接口：`POST /receipts/{id}/abort`（中止在途并释放暂存）、
`DELETE /receipts/{id}`（删除接收，唯一引用的成品一并回收）。

### 下游同步：变更事件流 `GET /changes`

数仓下游同步程序用本接口维护浮标站日原包目录：发布后可供分析，
接收被删除或对账发现共享原件缺失后须撤回。事件**自启用时起记录**，
按 `seq` 游标分页，只追加、不可变。

```bash
curl 'http://localhost:8080/changes?cursor=0&limit=100'
```

```json
{
  "events": [
    {"seq": 1, "event": "PUBLISHED", "receipt_id": "RCV-0001",
     "station_id": "BUOY-001", "obs_date": "2026-11-15",
     "sha256": "<64位十六进制>", "row_count": 24,
     "publication_id": "9f0b…", "reason": "published",
     "recorded_at": "2026-11-16T08:30:00.123Z"},
    {"seq": 2, "event": "WITHDRAWN", "receipt_id": "RCV-0001", …,
     "publication_id": "9f0b…", "reason": "receipt_deleted",
     "recorded_at": "…"}
  ],
  "next_cursor": 2,
  "has_more": false
}
```

- **游标语义**：`cursor` 为已消费的最大 `seq`（0 表示从头）；响应 `next_cursor`
  供下一页使用，`has_more=false` 表示已追到尾部。
- **事件五要素**：接收号、站号、UTC 日期、SHA-256、行数均为变更当时的快照，
  之后接收记录如何变化都不影响已登记事件。
- **`publication_id`**：每次发布生成的唯一标识。同号删除后重新入湖（即使摘要
  相同）会先产生 `WITHDRAWN`（指向旧 `publication_id`）再产生新的 `PUBLISHED`；
  消费者按 `seq` 顺序应用，撤回只命中对应发布，不会误撤新件。
- **`reason`**：`published`（正常发布）、`receipt_deleted`（接收被删除）、
  `blob_missing`（对账发现共享原件缺失；引用该原件的**所有**接收逐一撤回）。
- **可靠性**：事件与状态变更在同一 SQLite 写事务内登记——未成功的变更
  （校验失败、额度不足、中止、崩溃回滚）不产生事件；已生效的变更不漏记、
  不重复（重试/幂等路径不再登记）；`changes` 表由触发器强制只追加，
  事件一经返回不变，后来的变更只追加在其后。

### Parquet 约束

- 必需三列：`station_id`（字符串）、`timestamp_utc`（timestamp/date/ISO8601 字符串）、
  `salinity`（数值）；允许携带额外列，原样保留。
- 每行 `station_id` 必须等于声明站号；每行时间戳的 **UTC 日期**必须等于声明日期。
- 空表拒绝；时区按 UTC 归一化处理。

---

## 5. 并发与崩溃安全设计

1. **SQLite 多进程共享**：WAL + `busy_timeout` + `BEGIN IMMEDIATE`，写事务跨实例串行化。
2. **接收级 flock**：同一接收号同时只允许一个上传会话，冲突返回 `409 receipt_busy`；
   不同接收号互不阻塞。进程崩溃时内核自动释放锁。
3. **全局 flock**：发布与对账互斥，保证回收不会与在途发布竞争。
4. **原子发布**：成品先写 `.tmp` 再 `rename`；内容已存在则只增 `ref_count`，不重复占额。
5. **对账（reconcile）**：启动即执行，之后每 `OBS_RECONCILE_INTERVAL` 秒一次——
   - 无 `UPLOADING` 归属的暂存（含他实例崩溃残留）→ 删除；
   - `UPLOADING` 缺暂存 → 进度清零、标记可重传；
   - `PUBLISHED` 缺成品 → 同事务登记 `WITHDRAWN` 事件后回退 `UPLOADING`
     并解除引用，共享原件缺失时所有关联接收逐一撤回，杜绝缺件成功记录；
   - 无引用 blob → 删除并注销；账本与磁盘不符 → 以磁盘为准修正。
6. **变更事件流（changes）**：`seq INTEGER PRIMARY KEY AUTOINCREMENT` 全局单调；
   发布/删除/缺件回退在各自写事务内插入事件，崩溃只可能同事务一起回滚；
   触发器禁止 UPDATE/DELETE，事件一经返回不可变，只追加。

---

## 6. 配额语义

- `GET /quota` 返回 `quota_bytes / used_bytes / blob_bytes / staging_bytes / available_bytes`。
- 相同内容（同 SHA-256）无论被多少接收引用，物理只存一份，`blob_bytes` 只计一次。
- 落盘暂存前按「成品已用 + 本次暂存」核算；发布新内容前按「成品已用 + 本包」核算，
  超额均返回 `507` 且不留半成品。同内容重发因发布去重，不会因暂存而被误拒。

---

## 7. 测试

```bash
pip install pytest httpx pyarrow
python -m pytest tests/                  # 44 个用例（含并发、配额、崩溃回收、事件流）
python -W error -m pytest tests/         # 严格模式（资源告警也视为失败）

# 真实双进程端到端：先按方式 B 启动两实例，再执行
OBS_DATA_DIR=/tmp/obs-e2e python -I scripts/integration_test.py
```

测试覆盖：完整发布/下载往返、幂等与冲突、长度/摘要/Parquet/站号/站日五类拒绝、
去重占额、配额 507、慢上传不阻塞、同接收并发互斥、孤儿暂存/缺件成品回收、
双 Database 连接共享状态，以及变更事件流的快照五要素、共享缺件全部撤回、
同号重入湖先撤后发、重试/并发不重复、失败无事件、游标分页与不可变性、
v1 旧库迁移等。

---

## 8. 配置项（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OBS_DATA_DIR` | `/data` | 数据根目录（DB、暂存、成品、锁） |
| `OBS_QUOTA_BYTES` | `1073741824` | 原包字节总额度（暂存+成品共用） |
| `OBS_INSTANCE_ID` | `instance-0` | 实例标识，仅用于健康检查/排查 |
| `OBS_RECONCILE_INTERVAL` | `30` | 周期对账间隔秒 |
| `OBS_CHUNK_SIZE` | `1048576` | 流式分块大小 |
