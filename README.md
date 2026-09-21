# quorum-log-replication-java

本机可复现的多数派日志复制服务：在单个 JVM 内以本地持久化数据模拟多副本（默认 3 个），
提供写入确认、读取可见性、主副本更替、崩溃恢复、幂等提交与日志回收能力。
不依赖任何外部服务、中间件或联网资源。

## 构建与运行

```bash
mvn compile          # 编译
mvn test             # 全部测试（22 个用例）
mvn spring-boot:run  # 本地启动，默认 8080 端口
```

## 架构

```
ClientController (/client/*)  ─┐
                               ├─► Cluster ─► RaftNode × N ─► LogStore（data/<nodeId>/）
NodeController  (/raft/*)    ─┘        └─ InMemoryTransport（模拟网络/故障注入）
```

- `LogStore`：每节点一个目录，`meta.properties`（任期/投票/提交点，临时文件+原子替换防半更新）
  与 `log.dat`（每行 `term|index|requestId|base64(payload)|crc32`）。加载时逐条校验 CRC
  与索引连续性，损坏即报 `CORRUPTED_LOG`。
- `RaftNode`：状态机。写入仅在多数派确认后推进 `commitIndex`；提交后向各副本广播提交点。
- `Cluster`：编排选主、崩溃/重启、网络隔离等故障注入；客户端请求路由到当前主副本。

## 一致性与读取语义

- **写入确认**：仅主副本受理写。条目先在主副本落盘，再复制到各副本；获得多数派确认
  （`N/2+1`）后推进提交点并确认客户端。多数派不足时返回 `UNAVAILABLE`，条目保留在
  主副本日志中但**不对外可见**，副本恢复后由后续写入触发补齐并一并提交。
- **读取语义**：已提交前缀读（committed-prefix read）。任何在线副本只返回
  `index ≤ commitIndex` 的条目；落后副本补齐期间不会读到未提交数据。
  客户端入口 `/client/read` 读主副本，保证读到最新已提交序列。
- **已确认写入的持久性**：已提交条目随 `commitIndex` 一并落盘，重启或副本恢复后不丢失、
  不被静默改写；旧主的未提交后缀在与新主日志冲突时被截断并由新主条目覆盖，
  同一位置不会同时存在两条不同记录（见 `CrashRecoveryTest`）。

## 恢复规则

- `crash()`：节点立即下线，拒绝一切读写（`NODE_DOWN`），持久化内容保留在磁盘。
- `recover()`：从磁盘重放 `meta.properties` 与 `log.dat`，以 FOLLOWER 身份上线；
  日志损坏时恢复失败并报 `CORRUPTED_LOG`，不会带伤提供服务。
- 主副本更替：候选者任期自增并请求多数派投票；任期过期的追加/投票请求被拒绝
  （`TERM_MISMATCH`），日志位置不匹配被拒绝（`LOG_MISMATCH`）并触发逐条回退重试；
  所有拒绝均不改变既有状态。

## 幂等与并发

- 同一 `requestId` 重复到达：返回首次追加的条目（同一 index），不重复追加、不重复计账；
  重启后去重仍然有效（去重依据为持久化日志本身）。
- 写入路径全程串行化（节点级锁），并发写入索引连续递增，无跳变、无错位；
  提交点推进与落盘同事务边界，不存在半更新状态。

## 错误模型（可区分）

| ErrorCode           | 含义                       | HTTP 状态 |
|---------------------|---------------------------|-----------|
| `INVALID_ARGUMENT`  | 参数非法/越权（空 requestId、超大 payload、未知节点） | 400 |
| `NOT_LEADER`        | 当前节点非主副本/集群无主    | 409 |
| `TERM_MISMATCH`     | 任期不匹配                 | 409 |
| `LOG_MISMATCH`      | 日志位置/任期不匹配          | 409 |
| `UNAVAILABLE`       | 副本不可达/多数派不足        | 503 |
| `NODE_DOWN`         | 节点已崩溃或未恢复           | 503 |
| `RESOURCE_EXHAUSTED`| 超出限流/容量策略            | 429 |
| `CORRUPTED_LOG`     | 日志损坏（校验和/连续性失败） | 500 |

底层 IO 异常一律包装为 `RaftException`，不直接透出；节点间协议（`/raft/*`）以结构化
结果返回 `error` 字段，拒绝不改变既有状态。

## 可配置项（`raft.*`，见 `application.properties`）

| 配置 | 默认 | 说明 |
|------|------|------|
| `raft.node-id` | `node1` | 本节点标识 |
| `raft.nodes` | `node1,node2,node3` | 集群全部节点（单 JVM 内模拟） |
| `raft.data-dir` | `data` | 持久化根目录（每节点一个子目录） |
| `raft.max-payload-bytes` | `4096` | 单条 payload 上限，超限拒绝（`INVALID_ARGUMENT`） |
| `raft.max-uncommitted-entries` | `1000` | 未提交积压上限，超限拒绝（`RESOURCE_EXHAUSTED`） |
| `raft.max-log-entries` | `10000` | 活跃日志条数上限，触发前缀回收；回收后仍超限则拒绝 |
| `raft.compaction-retain` | `100` | 前缀回收时保留的最近已提交条数 |

**回收语义**：仅回收已提交的历史前缀；被截断的前缀追加到 `archive.log` 归档（本机可读核对），
已提交数据不丢失；保留窗口内的已提交条目照常可读。

## 接口

```bash
# 写入（幂等：requestId 唯一）
curl -X POST localhost:8080/client/write -H 'Content-Type: application/json' \
     -d '{"requestId":"req-1","payload":"hello"}'

# 读取已提交前缀
curl localhost:8080/client/read

# 节点间协议（测试/调试用，node 参数指定目标副本）
curl -X POST 'localhost:8080/raft/append-entries?node=node2' -H 'Content-Type: application/json' \
     -d '{"term":1,"leaderId":"node1","prevLogIndex":0,"prevLogTerm":0,"entries":[],"leaderCommit":0}'
```

## 测试与验证方法

```bash
mvn test                                          # 全部
mvn test -Dtest=MajorityCommitTest                # 多数派不足：不提交、不可见、恢复后补齐
mvn test -Dtest=CrashRecoveryTest                 # 崩溃重启/主更替：已提交不丢失、同位置不双写
mvn test -Dtest=IdempotencyTest                   # 重复提交幂等 + 并发索引连续性
mvn test -Dtest=LogCompactionTest                 # 前缀回收/归档 + 限流拒绝
mvn test -Dtest=CorruptedLogTest                  # 日志损坏归一化为 CORRUPTED_LOG
mvn test -Dtest=ErrorMappingTest                  # HTTP 入口错误码与状态映射
```

运行日志包含任期、索引与判定依据（slf4j），例如：

```
leader elected id=node1 term=1 votes=3 majority=2
entry appended id=node1 term=1 index=3 requestId=req-3
commit advanced id=node1 term=1 commitIndex=3
append rejected id=node2 term=2 leaderTerm=1 reason=TERM_MISMATCH
duplicate write ignored id=node1 term=1 requestId=req-1 index=1
log compacted id=node1 term=1 cut=8 commitIndex=10 remaining=3
```

> 注：本机 `/tmp` 若受磁盘配额限制，测试 JVM 已通过 surefire `argLine`
> 将 `java.io.tmpdir` 指向 `target/`，无需额外配置。
