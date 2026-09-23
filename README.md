# grantgate

射电阵列接收机访问闸门：多个无状态 API 进程共享一个 PostgreSQL 后端，
对接收机的 SHARED / EXCLUSIVE 授权做跨进程一致的并发裁决。

## 模型

- 授权记录持久化：`grant_id`、`receiver`、`mode`（`SHARED`/`EXCLUSIVE`）、
  `status`（`ACTIVE`/`RELEASED`）、所有者令牌的 SHA-256 摘要。
- 申请 `SHARED`：仅当该接收机没有活动 `EXCLUSIVE` 授权时创建 `ACTIVE` 授权。
- 申请 `EXCLUSIVE`：仅当该接收机没有任何活动授权时成功。
- 冲突一律返回 `409 {"error":"BUSY"}`，且事务回滚、不留任何记录。
- 冲突检查与写入在同一事务内完成；事务首先获取
  `pg_advisory_xact_lock(hashtext(receiver))`，按接收机串行化裁决，
  因此两个 API 进程并发申请同一接收机不会产生双重独占。
- 所有者令牌只在创建响应中出现一次；释放时凭令牌鉴权，错误令牌返回
  `403 {"error":"FORBIDDEN"}`，目标记录与活动集合保持不变。
- 查询接口按插入顺序稳定返回该接收机的授权标识、模式、状态，不含令牌。
- 状态存于 PostgreSQL，全部进程重启后授权状态与原令牌效力不变。

## API

| 方法 | 路径 | 请求体 | 响应 |
| --- | --- | --- | --- |
| POST | `/receivers/{receiver}/grants` | `{"mode":"SHARED"\|"EXCLUSIVE"}` | `201` → `{grant_id, receiver, mode, status, owner_token}`；冲突 `409 BUSY` |
| GET | `/receivers/{receiver}/grants` | — | `200` → `{"grants":[{grant_id, receiver, mode, status}, ...]}`（按序稳定，无令牌） |
| POST | `/grants/{grant_id}/release` | `{"owner_token":"..."}` | `200` → 更新后的授权；令牌错误 `403 FORBIDDEN`；不存在 `404 NOT_FOUND` |
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
查询不泄露令牌），成功退出码 0：

```sh
docker compose up --build --exit-code-from verify --abort-on-container-exit
echo $?   # 0 = 验收通过
```

## 测试

集成测试用两个真实 API 实例（独立 `http.Server` 与连接池）覆盖并发独占
竞争、共享共存、越权释放与重启后状态/令牌效力。需要一个 PostgreSQL：

```sh
TEST_DATABASE_URL=postgres://grants:grants@localhost:5432/grants?sslmode=disable \
  go test ./... -count=1
```

未设置 `TEST_DATABASE_URL` 时测试自动跳过。

## 布局

```
cmd/server    API 进程入口（DATABASE_URL、LISTEN_ADDR）
cmd/verify    一次性验收程序（API1_URL、API2_URL）
internal/grants  授权存储（事务+按接收机咨询锁）与 HTTP 层
```
