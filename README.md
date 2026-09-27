# quorum-log-replication-java

本机可复现的日志复制与多数派（quorum）提交能力。在单个 Spring Boot 进程内以三个副本角色
（`n1`/`n2`/`n3`）模拟复制、主副本更替、崩溃重启、日志回收，以及历史完全不可用副本的快照追平，
不依赖任何外部服务或中间件。

## 构建与测试

```bash
mvn test          # 运行全部测试（34 个用例，纯本机、不联网）
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
- **读取（read-index + committed-read）**：只返回已提交状态机内容，绝不暴露未提交条目。
  主副本直接读本地；跟随者读取前先向主副本确认当前已提交点，只有本地提交点追平时才应答。
  出现以下“自身状态不足以给出可信结论”的情况时，读取被**明确拒绝**为 `STALE_READ`（HTTP 409），
  而不会被当成“这个键不存在”而返回空值：
  - 副本不知道当前有效的主副本（例如刚由空目录/重启重建、尚未被任何主副本接触）；
  - 主副本不可达，无法确认已提交点；
  - 本地提交点落后于主副本已对外确认的写入。

  副本重新与主副本同步后（上任心跳或下一次复制），读取自动恢复正常；主副本状态充分时，
  “键不存在”仍正常返回 `{ok:false}`，与 `STALE_READ` 明确区分。响应携带 `commitIndex` 便于校验。
- **幂等**：同一 `requestId` 重复到达直接返回首次结果，不重复追加、不重复推进提交点。
- **并发**：副本内所有变更串行化，索引严格连续，提交点单调推进，无跳变/错位/半更新。

## 内容与容量

- 键、值、`requestId` 均为任意 UTF-8 文本：中文、emoji（含组合 emoji）、换行/制表符、
  引号反斜杠、长文本等都逐字复制、逐字持久化。日志按 UTF-8 字节做长度前缀，
  回收、冲突截断、崩溃重启后内容不截断、不串位、不消失。
- **单条写入大小上限** `replication.max-entry-bytes`：按 `requestId+key+value` 的 UTF-8
  字节合计计算（默认 1 MiB）。超限写入在任何状态变更之前被拒绝，返回独立错误码
  `ENTRY_TOO_LARGE`（HTTP 413），可与其它拒绝原因区分；拒绝不追加日志、不推进提交点、
  不改动任何副本状态，也不留下去重痕迹（同一 `requestId` 随后以合规大小重提照常成功）。
  该上限只在主副本受理入口强制；跟随者通过正常复制收到的条目必然已通过主副本校验。

## 历史不可用副本的追平（快照安装）

长时间离线、本地历史已被回收干净、或直接由**空目录重建**的副本，无需人工重建数据目录即可追平：

- 主副本复制时若发现跟随者需要的日志起点已落在自己的回收基址之前（或跟随者在基址处的
  前序检查仍失败），改为发送 `InstallSnapshot`：携带基址 `(lastIncludedIndex, lastIncludedTerm)`
  与完整的已提交状态机快照，随后再补发基址之后的日志。
- 跟随者安装顺序为崩溃安全设计：先把快照写入临时文件并原子替换 `state.properties`，
  再推进持久化提交点，最后原子替换日志文件并重设基址；全部落盘后才切换内存状态机。
  安装期间该副本对外读取一律是 `STALE_READ`，不会暴露未提交或半旧数据。
- 追平与正常写入、日志回收可同时发生（见 `CatchUpConcurrencyTest`）；追平完成后该副本
  在所有入口下读到的内容、提交点与日志范围都与主副本一致，并可正常提供读取。
- 快照安装是幂等的：重发的旧快照不会让基址/提交点回退。

## 恢复规则

- **崩溃重启**：从 `log.dat` + `meta.properties` 恢复；尾部不完整记录视为撕裂写直接截断；
  CRC 不匹配的完整记录判定为日志损坏，抛出 `LOG_CORRUPTED`。已提交数据由持久化提交点
  与状态机快照保证不丢失、不被静默改写；任意文本内容重启后逐字保持，日志范围
  （`baseIndex/lastIndex/commitIndex`）前后自洽。
- **主副本更替**：新主以更高任期复制；旧主收到更高任期请求必须让位（`stepDown`）。
  跟随者日志前序（`prevLogIndex/prevLogTerm`，含基址 `baseTerm`）不匹配时拒绝，
  主副本逐格回退 `nextIndex` 补齐；回退到回收基址仍不匹配则改用快照追平；
  冲突条目先截断再追加，同一位置不会被两条不同记录覆盖。
- **任期/日志不匹配的副本**：`term` 过低的复制/快照/read-index 请求被拒绝；
  非主副本写入返回 `NOT_LEADER`，拒绝不改变既有状态。

## 日志回收与资源控制

- `replication.max-log-entries`：日志条目上限。超过时回收**已提交**的历史前缀
  （推进 `baseIndex`），回收前把已提交状态机快照落盘，已提交数据仍可读取。
- 无可回收前缀仍超限时拒绝写入并返回 `RESOURCE_EXHAUSTED`（HTTP 507），不做无界占用。
- 回收按 UTF-8 字节维护日志偏移，多字节文本回收后再发生冲突截断也不会切错位置。

## 可配置项（`application.properties`）

| 配置 | 默认 | 说明 |
|---|---|---|
| `replication.data-dir` | `data` | 副本数据根目录（每副本一个子目录） |
| `replication.max-log-entries` | `1000` | 日志保留上限，超过触发已提交前缀回收 |
| `replication.max-entry-bytes` | `1048576` | 单条写入 UTF-8 字节上限，超过返回 `ENTRY_TOO_LARGE`(413) |

## 兼容性

- **数据目录**：磁盘格式（`log.dat` 长度前缀记录、`log.base` 基址、`meta.properties`、
  `state.properties` 快照）保持不变，已有的数据目录可直接沿用、启动即恢复；
  快照追平会复用同一套文件，以临时文件原子替换写入。
- **接口**：`/api/nodes/{id}/...` 路径、请求/响应字段保持不变；`/status` 额外返回
  `baseTerm` 与 `stateMachineSize`（纯新增字段）。行为上唯一收紧的是读取语义：
  状态不足的跟随者现在返回 `STALE_READ`(409) 而不是空结果——这是本次有意强化的语义。

## REST 接口

| 方法/路径 | 说明 |
|---|---|
| `POST /api/nodes/{id}/write` | 客户端写入 `{requestId,key,value}`，多数派确认后返回 `{term,index,commitIndex}`；超限返回 413 |
| `GET  /api/nodes/{id}/read?key=` | read-index + committed-read，返回 `{ok,value,commitIndex}`；状态不足返回 409 `STALE_READ` |
| `GET  /api/nodes/{id}/status` | 角色/任期/主副本/提交点/日志范围（含 `baseTerm`）/状态机 |
| `POST /api/nodes/{id}/elect` | 指定节点成为主副本（任期自增，上任即发心跳） |
| `POST /api/nodes/{id}/crash` / `restart` | 模拟崩溃 / 从磁盘（含空目录）恢复 |
| `POST /api/nodes/{id}/reachable?value=` | 网络隔离/恢复 |

错误响应统一为 `{ok:false, errorCode, message}`，`errorCode` 可区分：
`NOT_LEADER`(409)、`NO_MAJORITY`(503)、`TERM_MISMATCH`/`LOG_MISMATCH`/`STALE_READ`(409)、
`INVALID_ARGUMENT`(400)、`LOG_CORRUPTED`(500)、`REPLICA_UNREACHABLE`(503)、
`RESOURCE_EXHAUSTED`(507)、`ENTRY_TOO_LARGE`(413)、`INTERNAL`(500)。底层 IO 异常不直接透出。

## 验证方法

```bash
# 手工验证（服务启动后）
curl -X POST localhost:8080/api/nodes/n1/elect
# 任意文本（中文/emoji）
curl -X POST localhost:8080/api/nodes/n1/write -H 'Content-Type: application/json' \
     -d '{"requestId":"r1","key":"表情键","value":"你好😀🚀"}'
curl 'localhost:8080/api/nodes/n2/read?key=%E8%A1%A8%E6%83%85%E9%94%AE'  # 逐字返回
# 历史不可用副本追平：清空 n3 数据目录后重启，再做一次写入即可追平
rm -rf data/n3 && curl -X POST localhost:8080/api/nodes/n3/restart
curl 'localhost:8080/api/nodes/n3/read?key=k1'   # 追平前 409 STALE_READ
curl -X POST localhost:8080/api/nodes/n1/write -H 'Content-Type: application/json' \
     -d '{"requestId":"r2","key":"k2","value":"v2"}'   # 触发快照追平
curl 'localhost:8080/api/nodes/n3/status'        # commitIndex/baseIndex 与主一致
# 超限写入（把 replication.max-entry-bytes 调小后）：413 ENTRY_TOO_LARGE，且不留任何状态
```

自动化测试覆盖（`src/test/.../tests/`，`mvn test` 直接运行，不依赖外部服务或联网）：

- `ReplicationTest`：多数派提交与可见性、多数派不足拒绝、崩溃重启持久性、
  落后副本补齐、主更替与日志收敛、幂等、日志回收、边界校验、日志损坏识别；
- `ConcurrencyTest`：8 线程并发写入，索引连续无跳变、各副本状态机一致；
- `ApiTest`：REST 入口下写入/读取/幂等/超限(413)/`STALE_READ`(409)/错误码与核心层结论一致；
- `ContentIntegrityTest`：中文/emoji/混合空白与长文本逐字复制、超限拒绝不留痕、
  回收与重启后的内容完整性与日志范围自洽、多字节内容回收后的冲突截断；
- `SnapshotCatchUpTest`：空目录重建追平、历史被回收干净的副本追平、
  追平与写入/回收交错、追平期间不暴露半旧数据、主更替后仍一致；
- `CatchUpConcurrencyTest`：4 线程持续写入（同时持续回收）与历史不可用副本追平真正并发，
  最终提交点与状态机逐键逐字收敛；
- `StaleReadTest`：不知主/主不可达/落后已确认写入的读取拒绝、与“键不存在”区分、
  同步后自动恢复、各类拒绝不改动提交结果与副本状态。

运行日志包含节点、任期、索引与判定依据（如 `[n1] 提交点推进 commitIndex=3`、
`[n2] 冲突截断 index=2 旧term=1 新term=3`、`[n3] 快照安装完成 snapshotIndex=9`），
可用于追踪每次判定。
