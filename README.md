# grantgate

射电阵列接收机访问闸门：多个无状态 API 进程共享一个 PostgreSQL 后端，
对接收机的 SHARED / EXCLUSIVE 授权做跨进程一致的并发裁决。

## 模型

- 授权记录持久化：`grant_id`、`receiver`、`mode`（`SHARED`/`EXCLUSIVE`）、
  `status`（`ACTIVE`/`RELEASED`/`UPGRADE_PENDING`）、所有者令牌的 SHA-256 摘要。
- 申请 `SHARED`：仅当该接收机没有活动 `EXCLUSIVE` 授权、也没有待升级记录时创建 `ACTIVE` 授权。
- 申请 `EXCLUSIVE`：仅当该接收机没有任何活动授权、也没有待升级记录时成功。
- 冲突一律返回 `409 {"error":"BUSY"}`，且事务回滚、不留任何记录。
- **共享授权升级（原地独占接管）**：持有者凭原令牌调用升级接口，不释放原授权、
  不更换令牌：
  - 没有其他活动 `SHARED` 时，记录在同一事务内原地变为 `ACTIVE EXCLUSIVE`。
  - 否则变为 `UPGRADE_PENDING`；此后该接收机拒绝一切新的 `SHARED` 与
    `EXCLUSIVE` 申请（屏障）。每个接收机至多一条待升级记录。
  - 其他共享授权逐个释放时，释放事务在同一接收机咨询锁下检查剩余授权；最后一个
    竞争者离开的同一提交内把待升级记录晋升为 `ACTIVE EXCLUSIVE`，查询观察不到
    中间状态。
  - 待升级授权自身被释放即取消升级（记录变 `RELEASED`），准入恢复正常。
  - 升级对同一条记录 + 正确令牌幂等（等待中重复返回等待态，晋升后重复返回独占态）。
  - 错误令牌 `403 FORBIDDEN`；已释放授权、原生 `EXCLUSIVE` 授权返回
    `409 NOT_UPGRADABLE`；接收机已有另一条待升级记录时返回
    `409 ALREADY_UPGRADING`；这些错误均不改变授权集合。
- 冲突检查与写入在同一事务内完成；事务首先获取
  `pg_advisory_xact_lock(hashtext(receiver))`，按接收机串行化裁决，
  因此两个 API 进程并发申请/升级同一接收机不会产生双重独占或双重待升级。
- 所有者令牌只在创建响应中出现一次；释放与升级都凭令牌鉴权，错误令牌返回
  `403 {"error":"FORBIDDEN"}`，目标记录与活动集合保持不变。
- 查询接口按插入顺序稳定返回该接收机的授权标识、模式、状态，不含令牌。
- 状态存于 PostgreSQL，全部进程重启后授权状态（含 `UPGRADE_PENDING`）与原令牌效力不变。

## API

| 方法 | 路径 | 请求体 | 响应 |
| --- | --- | --- | --- |
| POST | `/receivers/{receiver}/grants` | `{"mode":"SHARED"\|"EXCLUSIVE"}` | `201` → `{grant_id, receiver, mode, status, owner_token}`；冲突 `409 BUSY`（有待升级记录时同样 `BUSY`） |
| GET | `/receivers/{receiver}/grants` | — | `200` → `{"grants":[{grant_id, receiver, mode, status}, ...]}`（按序稳定，无令牌） |
| POST | `/grants/{grant_id}/release` | `{"owner_token":"..."}` | `200` → 更新后的授权；可能在同一提交内晋升待升级记录；令牌错误 `403 FORBIDDEN`；不存在 `404 NOT_FOUND` |
| POST | `/grants/{grant_id}/upgrade` | `{"owner_token":"..."}` | `200` → 原地转换后的授权（`ACTIVE EXCLUSIVE` 或 `UPGRADE_PENDING`，无新令牌）；`403 FORBIDDEN`；`404 NOT_FOUND`；`409 NOT_UPGRADABLE`/`ALREADY_UPGRADING` |
| GET | `/healthz` | — | `200` |

## 运行（Docker Compose）

```sh
docker compose up --build
```

发布端口可用环境变量覆盖（默认值见下）：

```sh
API1_PORT=9080 API2_PORT=9081 DB_PORT=55432 docker compose up --build
```

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `API1_PORT` | 8080 | api1 宿主机端口 |
| `API2_PORT` | 8081 | api2 宿主机端口 |
| `DB_PORT` | 5432 | PostgreSQL 宿主机端口 |
| `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | grants | 数据库身份 |

## 一次性验收

`verify` 服务对两个真实 API 进程执行完整验收序列（共享共存、409 冲突
不留记录、403 越权释放状态不变、跨进程释放、并发独占竞赛唯一胜者、
共享授权等待升级与屏障、最后竞争者离开自动晋升、取消升级恢复准入、
错误/原生独占/重复升级稳定错误、并发升级竞赛唯一胜者、查询不泄露令牌），
成功退出码 0：

```sh
docker compose up --build --exit-code-from verify --abort-on-container-exit
echo $?   # 0 = 验收通过
```

## 测试

集成测试用两个真实 API 实例（独立 `http.Server` 与连接池）覆盖并发独占
竞争、共享共存、越权释放、升级等待/自动晋升/取消/幂等/稳定错误、并发升级
唯一胜者、并发交织压力与重启后状态/令牌效力。需要一个 PostgreSQL：

```sh
TEST_DATABASE_URL=postgres://grants:grants@localhost:5432/grants?sslmode=disable \
  go test ./... -count=1
```

未设置 `TEST_DATABASE_URL` 时测试自动跳过。另有一个针对旧版 schema 的迁移
兼容测试，需要预先用原始 v1 表结构填充一个库后设置 `MIGRATION_DATABASE_URL`。

## 数据库迁移

升级特性对旧库做在线、幂等的迁移（在迁移咨询锁内）：`ALTER TABLE ...
ADD COLUMN IF NOT EXISTS upgraded_at`，并用同名约束
`grants_status_check` 把 `status` 域从 `(ACTIVE, RELEASED)` 扩展到
`(ACTIVE, RELEASED, UPGRADE_PENDING)`。旧记录的 `upgraded_at` 为 NULL，
故旧的 `ACTIVE EXCLUSIVE` 记录仍被识别为原生独占、拒绝升级；新旧二进制可在
同一库上先后启动。

## 布局

```
cmd/server    API 进程入口（DATABASE_URL、LISTEN_ADDR）
cmd/verify    一次性验收程序（API1_URL、API2_URL）
internal/grants  授权存储（事务+按接收机咨询锁）与 HTTP 层
```
