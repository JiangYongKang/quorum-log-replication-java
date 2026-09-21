package com.github.highcumontoa.quorumlogreplicationjava.cluster;

import com.github.highcumontoa.quorumlogreplicationjava.error.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ReplicationException;
import com.github.highcumontoa.quorumlogreplicationjava.log.LogEntry;
import com.github.highcumontoa.quorumlogreplicationjava.log.LogStore;
import com.github.highcumontoa.quorumlogreplicationjava.log.MetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 单个副本节点。
 *
 * <p>一致性语义：
 * <ul>
 *   <li>写入仅由主副本受理，获得多数派确认后才推进提交点并对外确认；</li>
 *   <li>读取只返回已提交状态机内容（committed-read），任何角色都不会暴露未提交条目；</li>
 *   <li>任期低于当前任期的复制请求被拒绝，日志前序不匹配时由主副本回退补齐；</li>
 *   <li>相同 requestId 的重复写入直接返回首次结果，不重复追加。</li>
 * </ul>
 */
public class ReplicaNode {

    private static final Logger log = LoggerFactory.getLogger(ReplicaNode.class);

    private final String id;
    private final List<String> peers;
    private final int clusterSize;
    private final Transport transport;
    private final long maxLogEntries;
    private final LogStore logStore;
    private final MetaStore metaStore;
    private final Path stateFile;

    private volatile Role role = Role.FOLLOWER;
    private volatile boolean running = true;
    private volatile String leaderId;

    /** 已提交状态机。 */
    private final Map<String, String> stateMachine = new LinkedHashMap<>();
    private long appliedIndex;
    /** 幂等去重：requestId -> 已受理条目。 */
    private final Map<String, LogEntry> dedup = new HashMap<>();
    /** 主副本视角：每个跟随者下一条待复制索引。 */
    private final Map<String, Long> nextIndex = new HashMap<>();

    public ReplicaNode(String id, List<String> peers, Path dataDir, Transport transport,
                       long maxLogEntries) {
        this.id = id;
        this.peers = List.copyOf(peers);
        this.clusterSize = peers.size() + 1;
        this.transport = transport;
        this.maxLogEntries = maxLogEntries;
        this.logStore = new LogStore(dataDir);
        this.metaStore = new MetaStore(dataDir);
        this.stateFile = dataDir.resolve("state.properties");
        // 重启恢复：先加载回收时保存的已提交快照，再重放日志中剩余的已提交条目
        loadSnapshot();
        this.appliedIndex = logStore.baseIndex();
        for (LogEntry e : logStore.entriesFrom(logStore.baseIndex() + 1)) {
            dedup.put(e.requestId(), e);
            if (e.index() <= metaStore.commitIndex()) {
                stateMachine.put(e.key(), e.value());
                appliedIndex = e.index();
            }
        }
        log.info("[{}] 恢复完成 term={} commitIndex={} lastIndex={} baseIndex={}",
                id, currentTerm(), commitIndex(), logStore.lastIndex(), logStore.baseIndex());
    }

    private void checkRunning() {
        if (!running) {
            throw new ReplicationException(ErrorCode.REPLICA_UNREACHABLE, "节点 " + id + " 已崩溃");
        }
    }

    public String id() {
        return id;
    }

    public Role role() {
        return role;
    }

    public long currentTerm() {
        return metaStore.currentTerm();
    }

    public long commitIndex() {
        return metaStore.commitIndex();
    }

    public String leaderId() {
        return leaderId;
    }

    public LogStore logStore() {
        return logStore;
    }

    public MetaStore metaStore() {
        return metaStore;
    }

    /** 模拟崩溃：停止一切服务，数据保留在磁盘。 */
    public void simulateCrash() {
        this.running = false;
        this.role = Role.FOLLOWER;
        log.info("[{}] 崩溃 term={} commitIndex={}", id, currentTerm(), commitIndex());
    }

    /** 成为主副本（测试驱动的选主入口）：任期自增并投票给自己。 */
    public synchronized void becomeLeader() {
        checkRunning();
        metaStore.currentTerm(currentTerm() + 1);
        metaStore.votedFor(id);
        this.role = Role.LEADER;
        this.leaderId = id;
        long next = logStore.lastIndex() + 1;
        for (String p : peers) {
            nextIndex.put(p, next);
        }
        log.info("[{}] 成为主副本 term={} lastIndex={}", id, currentTerm(), logStore.lastIndex());
    }

    /** 降级为跟随者。 */
    public synchronized void stepDown(long term) {
        if (term > currentTerm()) {
            metaStore.currentTerm(term);
            metaStore.votedFor(null);
        }
        this.role = Role.FOLLOWER;
        log.info("[{}] 降级为跟随者 term={}", id, currentTerm());
    }

    /** 客户端写入：仅主副本受理，多数派确认后提交。 */
    public synchronized LogEntry clientWrite(String requestId, String key, String value) {
        checkRunning();
        if (requestId == null || requestId.isBlank() || key == null || key.isBlank() || value == null) {
            throw new ReplicationException(ErrorCode.INVALID_ARGUMENT, "requestId/key/value 不能为空");
        }
        LogEntry seen = dedup.get(requestId);
        if (seen != null) {
            log.info("[{}] 幂等命中 requestId={} index={}", id, requestId, seen.index());
            return seen;
        }
        if (role != Role.LEADER) {
            throw new ReplicationException(ErrorCode.NOT_LEADER,
                    "节点 " + id + " 不是主副本, leader=" + leaderId);
        }
        enforceRetention();
        LogEntry entry = new LogEntry(currentTerm(), logStore.lastIndex() + 1, requestId, key, value);
        logStore.append(entry);
        dedup.put(requestId, entry);
        log.info("[{}] 追加 term={} index={} key={}", id, entry.term(), entry.index(), key);

        int acks = 1; // 自己
        for (String peer : peers) {
            if (replicateTo(peer)) {
                acks++;
            }
        }
        if (acks <= clusterSize / 2) {
            log.warn("[{}] 多数派不足 acks={}/{} index={} 不提交", id, acks, clusterSize, entry.index());
            throw new ReplicationException(ErrorCode.NO_MAJORITY,
                    "多数派不足: acks=" + acks + "/" + clusterSize);
        }
        advanceCommit(entry.index());
        broadcastCommit();
        return entry;
    }

    /** 提交点推进后通知所有跟随者（空批次心跳，携带最新 leaderCommit）。 */
    private void broadcastCommit() {
        for (String peer : peers) {
            replicateTo(peer);
        }
    }

    /** 主副本向单个跟随者复制到其 nextIndex 追平为止，返回是否成功。 */
    private boolean replicateTo(String peer) {
        while (true) {
            long ni = nextIndex.getOrDefault(peer, logStore.lastIndex() + 1);
            long prevIndex = ni - 1;
            long prevTerm;
            if (prevIndex == logStore.baseIndex()) {
                prevTerm = logStore.baseTerm();
            } else if (prevIndex < logStore.baseIndex()) {
                prevTerm = 0;
            } else {
                prevTerm = logStore.get(prevIndex).map(LogEntry::term).orElse(0L);
            }
            List<LogEntry> batch = logStore.entriesFrom(ni);
            Transport.AppendRequest req = new Transport.AppendRequest(
                    currentTerm(), id, prevIndex, prevTerm, batch, commitIndex());
            Transport.AppendResponse resp;
            try {
                resp = transport.append(peer, req);
            } catch (ReplicationException e) {
                log.warn("[{}] 复制到 {} 失败: {}", id, peer, e.getMessage());
                return false;
            }
            if (resp.term() > currentTerm()) {
                stepDown(resp.term());
                return false;
            }
            if (resp.success()) {
                nextIndex.put(peer, resp.matchIndex() + 1);
                if (resp.matchIndex() >= logStore.lastIndex()) {
                    return true;
                }
                continue; // 还有剩余条目
            }
            // 日志前序不匹配：回退一格重试；已触底仍被拒绝则放弃（需要快照恢复，超出本机模拟范围）
            long floor = logStore.baseIndex() + 1;
            if (ni <= floor) {
                log.warn("[{}] 跟随者 {} 前序检查在基址处仍失败, 放弃本轮复制", id, peer);
                return false;
            }
            nextIndex.put(peer, Math.max(floor, ni - 1));
            log.info("[{}] 跟随者 {} 日志不匹配, 回退 nextIndex={}", id, peer, ni - 1);
        }
    }

    private void advanceCommit(long index) {
        metaStore.commitIndex(index);
        applyCommitted();
        log.info("[{}] 提交点推进 commitIndex={}", id, index);
        enforceRetention();
    }

    private void applyCommitted() {
        long commit = commitIndex();
        while (appliedIndex < commit) {
            long next = appliedIndex + 1;
            Optional<LogEntry> e = logStore.get(next);
            if (e.isEmpty()) {
                break; // 条目尚未到达（跟随者等待复制）
            }
            stateMachine.put(e.get().key(), e.get().value());
            appliedIndex = next;
        }
    }

    /** 日志超过上限时回收已提交前缀；仍超限则拒绝写入。 */
    private void enforceRetention() {
        if (logStore.size() > maxLogEntries && commitIndex() > logStore.baseIndex()) {
            long collectedTo = commitIndex();
            saveSnapshot();
            logStore.truncatePrefix(collectedTo + 1);
            log.info("[{}] 回收已提交前缀至 index={}", id, collectedTo);
        }
        if (logStore.size() > maxLogEntries) {
            throw new ReplicationException(ErrorCode.RESOURCE_EXHAUSTED,
                    "日志积压超限: size=" + logStore.size() + " max=" + maxLogEntries);
        }
    }

    /** 客户端读取：只返回已提交数据。 */
    public synchronized Optional<String> clientRead(String key) {
        checkRunning();
        if (key == null || key.isBlank()) {
            throw new ReplicationException(ErrorCode.INVALID_ARGUMENT, "key 不能为空");
        }
        return Optional.ofNullable(stateMachine.get(key));
    }

    /** 处理来自主副本的复制请求。 */
    public synchronized Transport.AppendResponse onAppend(Transport.AppendRequest req) {
        checkRunning();
        if (req.term() < currentTerm()) {
            log.warn("[{}] 拒绝过期任期 term={} 当前 term={}", id, req.term(), currentTerm());
            return new Transport.AppendResponse(currentTerm(), false, logStore.lastIndex());
        }
        if (req.term() > currentTerm()) {
            stepDown(req.term());
        }
        this.leaderId = req.leaderId();
        this.role = Role.FOLLOWER;

        // 前序一致性检查
        if (req.prevLogIndex() > logStore.lastIndex()) {
            return new Transport.AppendResponse(currentTerm(), false, logStore.lastIndex());
        }
        if (req.prevLogIndex() == logStore.baseIndex()) {
            if (req.prevLogTerm() != logStore.baseTerm()) {
                return new Transport.AppendResponse(currentTerm(), false, logStore.lastIndex());
            }
        } else if (req.prevLogIndex() > logStore.baseIndex()) {
            Optional<LogEntry> prev = logStore.get(req.prevLogIndex());
            if (prev.isEmpty() || prev.get().term() != req.prevLogTerm()) {
                return new Transport.AppendResponse(currentTerm(), false, logStore.lastIndex());
            }
        }
        // 追加：冲突处截断，保证同一位置不会被两条不同记录覆盖
        List<LogEntry> newEntries = req.entries() == null ? List.of() : req.entries();
        for (LogEntry e : newEntries) {
            if (e.index() <= logStore.baseIndex()) {
                continue; // 已回收的已提交前缀，跳过
            }
            Optional<LogEntry> existing = logStore.get(e.index());
            if (existing.isPresent()) {
                if (existing.get().term() != e.term()) {
                    log.info("[{}] 冲突截断 index={} 旧term={} 新term={}",
                            id, e.index(), existing.get().term(), e.term());
                    logStore.truncateSuffix(e.index());
                    logStore.append(e);
                    dedup.put(e.requestId(), e);
                }
            } else {
                logStore.append(e);
                dedup.put(e.requestId(), e);
            }
        }
        long lastNew = newEntries.isEmpty() ? req.prevLogIndex()
                : newEntries.get(newEntries.size() - 1).index();
        if (req.leaderCommit() > commitIndex()) {
            metaStore.commitIndex(Math.min(req.leaderCommit(), lastNew));
            applyCommitted();
        }
        return new Transport.AppendResponse(currentTerm(), true, Math.max(lastNew, logStore.lastIndex()));
    }

    /** 回收前把已提交状态机落盘，保证重启后已提交数据不丢。 */
    private void saveSnapshot() {
        Properties p = new Properties();
        stateMachine.forEach((k, v) -> p.setProperty(k, v));
        Path tmp = stateFile.resolveSibling("state.properties.tmp");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            p.store(out, null);
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new ReplicationException(ErrorCode.INTERNAL, "状态机快照持久化失败", e);
        }
    }

    private void loadSnapshot() {
        if (!Files.exists(stateFile)) {
            return;
        }
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(stateFile)) {
            p.load(in);
        } catch (IOException e) {
            throw new ReplicationException(ErrorCode.LOG_CORRUPTED, "状态机快照损坏: " + stateFile, e);
        }
        p.forEach((k, v) -> stateMachine.put(String.valueOf(k), String.valueOf(v)));
    }

    /** 已提交状态机内容（只读视图）。 */
    public synchronized Map<String, String> stateMachine() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(stateMachine));
    }
}
