# quorum-log-replication-java

本机可复现的日志复制与多数派（quorum）提交能力。在单个 Spring Boot 进程内以三个副本角色
（`n1`/`n2`/`n3`）模拟复制、主副本更替、崩溃重启与日志回收，不依赖任何外部服务或中间件。

## 构建与测试

```bash
mvn test          # 运行全部测试（17 个用例）
mvn spring-boot:run   # 启动 REST 服务（默认 8080）
```

## 架构

```
api/      REST 入口（客户端读写 + 运维操作）
cluster/  ReplicaNode（副本状态机）、Cluster（崩溃/重启/隔离）、Transport（进程内通信）
log/      LogStore（CRC 校验的持久化日志）、MetaStore（任期/投票/提交点）
error/    ErrorCode + ReplicationException + 全局异常映射
config/   可配置项与集群装配
```

每个副本的数据目录下持久化：

| 文件 | 内容 |
|---|---|
| `log.dat` | 追加式日志记录：`[crc32][term][index][requestId][key][value]` |
| `log.base` | 回收后的日志基址 `(baseIndex, baseTerm)` |
| `meta.properties` | `currentTerm` / `votedFor` / `commitIndex`，原子替换写入 |
| `state.properties` | 日志回收时保存的已提交状态机快照 |

## 一致性与读取语义

- **写入**：仅主副本受理。条目先持久化入队，复制到多数派（含自己）后才推进 `commitIndex`
  并对外确认；未达多数派抛出 `NO_MAJORITY`，条目保持未提交、对读不可见。
- **读取**：committed-read——任何副本只返回已提交状态机内容，绝不暴露未提交条目。
  跟随者在补齐期间只读到其本地已提交前缀；响应携带 `commitIndex` 便于校验。
- **幂等**：同一 `requestId` 重复到达直接返回首次结果，不重复追加、不重复推进提交点。
- **并发**：副本内所有变更串行化，索引严格连续，提交点单调推进，无跳变/错位/半更新。

## 恢复规则

- **崩溃重启**：从 `log.dat` + `meta.properties` 恢复；尾部不完整记录视为撕裂写直接截断；
  CRC 不匹配的完整记录判定为日志损坏，抛出 `LOG_CORRUPTED`。已提交数据由持久化提交点
  与状态机快照保证不丢失、不被静默改写。
- **主副本更替**：新主以更高任期复制；旧主收到更高任期请求必须让位（`stepDown`）。
  跟随者日志前序（`prevLogIndex/prevLogTerm`，含基址 `baseTerm`）不匹配时拒绝，
  主副本逐格回退 `nextIndex` 补齐；冲突条目先截断再追加，同一位置不会被两条不同记录覆盖。
- **任期/日志不匹配的副本**：`term` 过低的复制请求被拒绝；非主副本写入返回 `NOT_LEADER`，
  拒绝不改变既有状态。

## 日志回收与资源控制

- `replication.max-log-entries`：日志条目上限。超过时回收**已提交**的历史前缀
  （推进 `baseIndex`），回收前把已提交状态机快照落盘，已提交数据仍可读取。
- 无可回收前缀仍超限时拒绝写入并返回 `RESOURCE_EXHAUSTED`（HTTP 507），不做无界占用。

## 可配置项（`application.properties`）

| 配置 | 默认 | 说明 |
|---|---|---|
| `replication.data-dir` | `data` | 副本数据根目录（每副本一个子目录） |
| `replication.max-log-entries` | `1000` | 日志保留上限，超过触发已提交前缀回收 |

## REST 接口

| 方法/路径 | 说明 |
|---|---|
| `POST /api/nodes/{id}/write` | 客户端写入 `{requestId,key,value}`，多数派确认后返回 `{term,index,commitIndex}` |
| `GET  /api/nodes/{id}/read?key=` | committed-read，返回 `{ok,value,commitIndex}` |
| `GET  /api/nodes/{id}/status` | 角色/任期/提交点/日志范围/状态机 |
| `POST /api/nodes/{id}/elect` | 指定节点成为主副本（任期自增） |
| `POST /api/nodes/{id}/crash` / `restart` | 模拟崩溃 / 从磁盘恢复 |
| `POST /api/nodes/{id}/reachable?value=` | 网络隔离/恢复 |

错误响应统一为 `{ok:false, errorCode, message}`，`errorCode` 可区分：
`NOT_LEADER`(409)、`NO_MAJORITY`(503)、`TERM_MISMATCH`/`LOG_MISMATCH`/`STALE_READ`(409)、
`INVALID_ARGUMENT`(400)、`LOG_CORRUPTED`(500)、`REPLICA_UNREACHABLE`(503)、
`RESOURCE_EXHAUSTED`(507)、`INTERNAL`(500)。底层 IO 异常不直接透出。

## 验证方法

```bash
# 手工验证（服务启动后）
curl -X POST localhost:8080/api/nodes/n1/elect
curl -X POST localhost:8080/api/nodes/n1/write -H 'Content-Type: application/json' \
     -d '{"requestId":"r1","key":"k1","value":"v1"}'
curl 'localhost:8080/api/nodes/n2/read?key=k1'      # 跟随者读到已提交值
curl -X POST localhost:8080/api/nodes/n2/crash
curl -X POST localhost:8080/api/nodes/n3/crash
curl -X POST localhost:8080/api/nodes/n1/write -H 'Content-Type: application/json' \
     -d '{"requestId":"r2","key":"k2","value":"v2"}'   # 503 NO_MAJORITY
curl -X POST localhost:8080/api/nodes/n2/restart && curl 'localhost:8080/api/nodes/n2/status'
```

自动化测试覆盖（`src/test/.../tests/`）：

- `ReplicationTest`：多数派提交与可见性、多数派不足拒绝、崩溃重启持久性、
  落后副本补齐、主更替与日志收敛、幂等、日志回收、边界校验、日志损坏识别；
- `ConcurrencyTest`：8 线程并发写入，索引连续无跳变、各副本状态机一致；
- `ApiTest`：REST 入口下写入/读取/幂等/错误码与核心层结论一致。

运行日志包含节点、任期、索引与判定依据（如 `[n1] 提交点推进 commitIndex=3`、
`[n2] 冲突截断 index=2 旧term=1 新term=3`），可用于追踪每次判定。
