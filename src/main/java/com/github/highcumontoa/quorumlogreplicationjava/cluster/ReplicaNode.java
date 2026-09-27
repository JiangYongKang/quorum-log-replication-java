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
    private final long maxWriteBytes;
    private final LogStore logStore;
    private final MetaStore metaStore;
    private final Path stateFile;

    private volatile Role role = Role.FOLLOWER;
    private volatile boolean running = true;

    /** 已提交状态机。 */
    private final Map<String, String> stateMachine = new LinkedHashMap<>();
    private long appliedIndex;
    /** 幂等去重：requestId -> 已受理条目。 */
    private final Map<String, LogEntry> dedup = new HashMap<>();
    /** 主副本视角：每个跟随者下一条待复制索引。 */
    private final Map<String, Long> nextIndex = new HashMap<>();

    public ReplicaNode(String id, List<String> peers, Path dataDir, Transport transport,
                       long maxLogEntries) {
        this(id, peers, dataDir, transport, maxLogEntries, 1_000_000);
    }

    public ReplicaNode(String id, List<String> peers, Path dataDir, Transport transport,
                       long maxLogEntries, long maxWriteBytes) {
        this.id = id;
        this.peers = List.copyOf(peers);
        this.clusterSize = peers.size() + 1;
        this.transport = transport;
        this.maxLogEntries = maxLogEntries;
        this.maxWriteBytes = maxWriteBytes;
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
        return metaStore.leaderId();
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
        metaStore.leaderId(id);
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
        // 单条写入大小上限：在任何状态变更之前拒绝，不留下半成品
        long writeBytes = utf8Len(key) + utf8Len(value);
        if (writeBytes > maxWriteBytes) {
            throw new ReplicationException(ErrorCode.PAYLOAD_TOO_LARGE,
                    "单条写入超限: " + writeBytes + " 字节 > 上限 " + maxWriteBytes);
        }
        LogEntry seen = dedup.get(requestId);
        if (seen != null) {
            log.info("[{}] 幂等命中 requestId={} index={}", id, requestId, seen.index());
            return seen;
        }
        if (role != Role.LEADER) {
            throw new ReplicationException(ErrorCode.NOT_LEADER,
                    "节点 " + id + " 不是主副本, leader=" + leaderId());
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

    private static long utf8Len(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
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
            // 所需前序已被回收：日志补齐不可行，改为传输已提交快照
            if (ni <= logStore.baseIndex()) {
                if (!sendSnapshot(peer)) {
                    return false;
                }
                continue;
            }
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
            // 日志前序不匹配：回退一格重试；触底后下一轮循环会走快照传输
            nextIndex.put(peer, ni - 1);
            log.info("[{}] 跟随者 {} 日志不匹配, 回退 nextIndex={}", id, peer, ni - 1);
        }
    }

    /**
     * 向历史不可用的跟随者传输已提交快照（InstallSnapshot）。
     * 快照内容仅为已提交状态机，跟随者安装后与主副本提交结论一致。
     */
    private boolean sendSnapshot(String peer) {
        long idx = commitIndex();
        long term;
        if (idx == logStore.baseIndex()) {
            term = logStore.baseTerm();
        } else {
            term = logStore.get(idx).map(LogEntry::term).orElse(0L);
        }
        Transport.SnapshotRequest req = new Transport.SnapshotRequest(
                currentTerm(), id, idx, term, stateMachine());
        Transport.SnapshotResponse resp;
        try {
            resp = transport.snapshot(peer, req);
        } catch (ReplicationException e) {
            log.warn("[{}] 快照传输到 {} 失败: {}", id, peer, e.getMessage());
            return false;
        }
        if (resp.term() > currentTerm()) {
            stepDown(resp.term());
            return false;
        }
        if (!resp.success()) {
            return false;
        }
        nextIndex.put(peer, idx + 1);
        log.info("[{}] 快照已安装到 {} lastIncludedIndex={}", id, peer, idx);
        return true;
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
            saveSnapshot(stateMachine);
            logStore.truncatePrefix(collectedTo + 1);
            log.info("[{}] 回收已提交前缀至 index={}", id, collectedTo);
        }
        if (logStore.size() > maxLogEntries) {
            throw new ReplicationException(ErrorCode.RESOURCE_EXHAUSTED,
                    "日志积压超限: size=" + logStore.size() + " max=" + maxLogEntries);
        }
    }

    /**
     * 客户端读取：只返回已提交数据，且仅在副本状态可信时返回。
     *
     * <p>跟随者每次读取都向已知主副本做一次实时读校验（read-check），只有
     * “主副本仍在任，且其提交点不超过本地提交点”时才给出结论——此时本地状态机
     * 必然包含全部已确认写入，无论键是否存在，结论都与主副本一致。
     *
     * <p>拒绝情形（统一为 {@link ErrorCode#STALE_READ}，读路径不做任何状态变更）：
     * <ul>
     *   <li>不知道当前有效主副本（leaderId 未知，如全新/重建节点）；</li>
     *   <li>主副本暂时不可达（网络隔离、崩溃），无法确认本地是否已追平；</li>
     *   <li>已知主副本已卸任（任期变更/主副本更替）；</li>
     *   <li>主副本可达但其提交点高于本地——本地是半旧数据（含漏掉已确认写入的情况）。</li>
     * </ul>
     * 因此落后副本在主副本不可达时既不会把缺失键误报为“不存在”，也不会对旧键
     * 返回可能已被覆盖的旧值。主副本恢复并完成一次重新同步后，读校验自然通过，
     * 读取自动恢复，“暂时无法确认”不会被缓存为空值或落后标记。
     */
    public synchronized Optional<String> clientRead(String key) {
        checkRunning();
        if (key == null || key.isBlank()) {
            throw new ReplicationException(ErrorCode.INVALID_ARGUMENT, "key 不能为空");
        }
        checkReadTrusted();
        return Optional.ofNullable(stateMachine.get(key));
    }

    private void checkReadTrusted() {
        if (role == Role.LEADER) {
            return; // 主副本自身即提交结论的来源
        }
        String leader = leaderId();
        if (leader == null) {
            throw new ReplicationException(ErrorCode.STALE_READ,
                    "节点 " + id + " 尚不知道当前主副本，无法确认读取结论");
        }
        if (leader.equals(id)) {
            // 重启后的旧主：角色已是跟随者，不能再凭“自己曾是主”给出结论，
            // 必须重新当选或联系上当前主副本
            throw new ReplicationException(ErrorCode.STALE_READ,
                    "节点 " + id + " 已不是当前主副本，无法确认读取结论");
        }
        long localCommit = commitIndex();
        Transport.ReadCheckResponse resp;
        try {
            resp = transport.readCheck(leader, new Transport.ReadCheckRequest(currentTerm(), id));
        } catch (ReplicationException e) {
            // 主副本不可达：本地可能漏掉已确认写入（存在与否都无法确认），必须拒绝
            throw new ReplicationException(ErrorCode.STALE_READ,
                    "节点 " + id + " 暂时联系不上主副本 " + leader
                            + "，无法确认本地是否已追平，拒绝读取", e);
        }
        if (resp.term() > currentTerm()) {
            // 集群已进入更高任期：不在读路径改状态，直接拒绝，待复制/心跳自然收敛
            throw new ReplicationException(ErrorCode.STALE_READ,
                    "节点 " + id + " 任期落后于主副本 term=" + resp.term()
                            + " > " + currentTerm() + "，拒绝读取");
        }
        if (!resp.isLeader()) {
            throw new ReplicationException(ErrorCode.STALE_READ,
                    "已知主副本 " + leader + " 已卸任，当前主副本未知，拒绝读取");
        }
        if (resp.commitIndex() > localCommit) {
            throw new ReplicationException(ErrorCode.STALE_READ,
                    "节点 " + id + " 落后于主副本已确认写入: commitIndex=" + localCommit
                            + " < " + resp.commitIndex() + "，追平前拒绝读取");
        }
        // 主副本仍在任且本地提交点 >= 主副本提交点：本地已包含全部已确认写入，
        // 命中即权威值，未命中即权威的“键不存在”。
    }

    /** 读校验应答：纯只读方法，刻意不加节点锁，避免与复制路径形成锁序死锁。 */
    public Transport.ReadCheckResponse onReadCheck(Transport.ReadCheckRequest req) {
        checkRunning();
        // role/currentTerm/commitIndex 均为易失读或 MetaStore 同步读，调用方只做水位比较
        return new Transport.ReadCheckResponse(currentTerm(), role == Role.LEADER, commitIndex());
    }

    /**
     * 安装来自主副本的已提交快照。整个方法在副本锁内原子完成：
     * 对外要么看到安装前、要么看到安装后的完整已提交状态，绝不暴露半成品。
     *
     * <p>持久化顺序为 状态机快照 → 提交点 → 日志基址，任意时刻崩溃重启后状态自洽。
     */
    public synchronized Transport.SnapshotResponse onSnapshot(Transport.SnapshotRequest req) {
        checkRunning();
        if (req.term() < currentTerm()) {
            log.warn("[{}] 拒绝过期任期的快照 term={} 当前 term={}", id, req.term(), currentTerm());
            return new Transport.SnapshotResponse(currentTerm(), false, logStore.lastIndex());
        }
        if (req.term() > currentTerm()) {
            stepDown(req.term());
        }
        metaStore.leaderId(req.leaderId());
        this.role = Role.FOLLOWER;
        long idx = req.lastIncludedIndex();
        log.info("[{}] 安装快照 lastIncludedIndex={} keys={} 来自 {}",
                id, idx, req.state().size(), req.leaderId());
        saveSnapshot(req.state());
        metaStore.commitIndex(idx);
        // 本地后续条目与快照一致的予以保留，否则整体重置到快照基址
        Optional<LogEntry> atIdx = logStore.get(idx);
        if (atIdx.isPresent() && atIdx.get().term() == req.lastIncludedTerm()) {
            logStore.truncatePrefix(idx + 1);
        } else {
            logStore.resetToBase(idx, req.lastIncludedTerm());
        }
        stateMachine.clear();
        stateMachine.putAll(req.state());
        appliedIndex = idx;
        dedup.entrySet().removeIf(e -> e.getValue().index() <= idx);
        return new Transport.SnapshotResponse(currentTerm(), true, idx);
    }

    /** 运维入口：主副本立即对所有跟随者发起一轮复制/快照追平。 */
    public synchronized void syncFollowers() {
        if (role != Role.LEADER) {
            return;
        }
        broadcastCommit();
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
        metaStore.leaderId(req.leaderId());
        this.role = Role.FOLLOWER;

        // 前序一致性检查：失败说明本地历史不足，主副本按响应回退补齐即可；
        // 是否落后由读取时的实时读校验判定，这里不保留任何“落后”状态
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

    /** 把已提交状态机落盘，保证重启后已提交数据不丢。 */
    private void saveSnapshot(Map<String, String> state) {
        Properties p = new Properties();
        state.forEach(p::setProperty);
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
