package com.github.highcumontoa.quorumlogreplicationjava.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 单个副本节点。
 *
 * <p>一致性语义：
 * <ul>
 *   <li>写入仅在多数派确认后推进 commitIndex，未提交条目对客户端不可见；</li>
 *   <li>读取只返回本地已提交前缀（index &le; commitIndex），落后副本补齐期间不会读到未提交数据；</li>
 *   <li>任期或日志位置不匹配的节点间请求被拒绝且不改变既有状态；</li>
 *   <li>崩溃重启后从磁盘恢复，已提交数据不丢失、不被改写。</li>
 * </ul>
 */
public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final String id;
    private final List<String> peers;
    private final RaftConfig config;
    private final LogStore store;

    private NodeRole role = NodeRole.FOLLOWER;
    private long currentTerm;
    private String votedFor;
    private long commitIndex;
    private final List<LogEntry> entries = new ArrayList<>();
    private final Map<String, Long> nextIndex = new HashMap<>();
    private Transport transport;
    private volatile boolean up;

    public RaftNode(String id, List<String> peers, RaftConfig config, LogStore store) {
        this.id = id;
        this.peers = peers;
        this.config = config;
        this.store = store;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public String id() {
        return id;
    }

    public NodeRole role() {
        return role;
    }

    public synchronized long currentTerm() {
        return currentTerm;
    }

    public synchronized long commitIndex() {
        return commitIndex;
    }

    public boolean isUp() {
        return up;
    }

    public synchronized long lastIndex() {
        return entries.isEmpty() ? baseIndex() : entries.get(entries.size() - 1).index();
    }

    private long baseIndex() {
        return entries.isEmpty() ? commitIndex : entries.get(0).index() - 1;
    }

    private LogEntry entryAt(long index) {
        long base = baseIndex();
        if (index <= base || index > lastIndex()) {
            return null;
        }
        return entries.get((int) (index - base - 1));
    }

    /** 从磁盘加载并上线（启动与崩溃恢复共用）。 */
    public synchronized void recover() {
        store.load();
        this.currentTerm = store.currentTerm();
        this.votedFor = store.votedFor();
        this.commitIndex = store.commitIndex();
        this.entries.clear();
        this.entries.addAll(store.entries());
        this.role = NodeRole.FOLLOWER;
        this.nextIndex.clear();
        this.up = true;
        log.info("node recovered id={} term={} commitIndex={} lastIndex={}",
                id, currentTerm, commitIndex, lastIndex());
    }

    /** 崩溃：立即下线，丢弃易失状态，持久化内容保留在磁盘。 */
    public synchronized void crash() {
        this.up = false;
        this.role = NodeRole.FOLLOWER;
        this.nextIndex.clear();
        log.info("node crashed id={} term={} commitIndex={}", id, currentTerm, commitIndex);
    }

    /** 发起选主：任期自增并请求多数派投票。 */
    public synchronized void startElection() {
        ensureUp();
        currentTerm++;
        votedFor = id;
        role = NodeRole.FOLLOWER;
        store.saveMeta(currentTerm, votedFor, commitIndex);
        long lastIdx = lastIndex();
        long lastTerm = lastIdx == baseIndex() ? 0 : entryAt(lastIdx).term();
        int votes = 1;
        int majority = peers.size() / 2 + 1;
        for (String peer : peers) {
            if (peer.equals(id)) {
                continue;
            }
            try {
                Transport.VoteResult result = transport.sendRequestVote(peer,
                        new Transport.VoteRequest(currentTerm, id, lastIdx, lastTerm));
                if (result.term() > currentTerm) {
                    stepDown(result.term());
                    return;
                }
                if (result.granted()) {
                    votes++;
                }
            } catch (RaftException e) {
                log.info("vote request failed id={} peer={} term={} reason={}",
                        id, peer, currentTerm, e.code());
            }
        }
        if (votes >= majority) {
            role = NodeRole.LEADER;
            nextIndex.clear();
            for (String peer : peers) {
                nextIndex.put(peer, lastIndex() + 1);
            }
            log.info("leader elected id={} term={} votes={} majority={}", id, currentTerm, votes, majority);
        } else {
            log.info("election failed id={} term={} votes={} majority={}", id, currentTerm, votes, majority);
        }
    }

    private void stepDown(long newTerm) {
        role = NodeRole.FOLLOWER;
        currentTerm = newTerm;
        votedFor = null;
        store.saveMeta(currentTerm, votedFor, commitIndex);
    }

    /**
     * 客户端写入：仅主副本受理；幂等去重；多数派确认后推进提交点。
     *
     * @return 已落盘的日志条目（重复请求返回原有条目，不重复追加）
     */
    public synchronized LogEntry clientWrite(String requestId, String payload) {
        ensureUp();
        if (role != NodeRole.LEADER) {
            throw new RaftException(ErrorCode.NOT_LEADER, "节点 " + id + " 不是主副本");
        }
        validate(requestId, payload);
        for (LogEntry e : entries) {
            if (e.requestId().equals(requestId)) {
                log.info("duplicate write ignored id={} term={} requestId={} index={}",
                        id, currentTerm, requestId, e.index());
                return e;
            }
        }
        checkResourceLimits();
        LogEntry entry = new LogEntry(currentTerm, lastIndex() + 1, requestId, payload);
        store.append(List.of(entry));
        entries.add(entry);
        log.info("entry appended id={} term={} index={} requestId={}", id, currentTerm, entry.index(), requestId);

        int acks = 1;
        int majority = peers.size() / 2 + 1;
        for (String peer : peers) {
            if (peer.equals(id)) {
                continue;
            }
            if (replicateTo(peer) >= entry.index()) {
                acks++;
            }
        }
        if (acks >= majority) {
            advanceCommit(entry.index());
            broadcastCommit();
            maybeCompact();
        } else {
            log.info("commit deferred id={} term={} index={} acks={} majority={}",
                    id, currentTerm, entry.index(), acks, majority);
            throw new RaftException(ErrorCode.UNAVAILABLE,
                    "多数派不足 acks=" + acks + " majority=" + majority + "，条目未提交");
        }
        return entry;
    }

    /** 复制到单个副本，返回其已确认的最大索引；失败时返回 -1。 */
    private long replicateTo(String peer) {
        long next = nextIndex.getOrDefault(peer, 1L);
        for (int attempt = 0; attempt <= lastIndex() - baseIndex() + 1; attempt++) {
            long prevIndex = next - 1;
            LogEntry prev = prevIndex > baseIndex() ? entryAt(prevIndex) : null;
            long prevTerm = prev != null ? prev.term() : 0;
            List<LogEntry> batch = new ArrayList<>();
            for (long i = next; i <= lastIndex(); i++) {
                LogEntry e = entryAt(i);
                if (e != null) {
                    batch.add(e);
                }
            }
            Transport.AppendEntriesRequest req = new Transport.AppendEntriesRequest(
                    currentTerm, id, prevIndex, prevTerm, batch, commitIndex);
            Transport.AppendEntriesResult result;
            try {
                result = transport.sendAppendEntries(peer, req);
            } catch (RaftException e) {
                log.info("replication unreachable id={} peer={} term={} reason={}", id, peer, currentTerm, e.code());
                return -1;
            }
            if (result.term() > currentTerm) {
                stepDown(result.term());
                return -1;
            }
            if (result.success()) {
                nextIndex.put(peer, result.matchIndex() + 1);
                return result.matchIndex();
            }
            if (result.error() == ErrorCode.LOG_MISMATCH) {
                next = Math.max(baseIndex() + 1, next - 1);
                nextIndex.put(peer, next);
                continue;
            }
            return -1;
        }
        return -1;
    }

    private void advanceCommit(long index) {
        if (index > commitIndex) {
            commitIndex = index;
            store.saveMeta(currentTerm, votedFor, commitIndex);
            log.info("commit advanced id={} term={} commitIndex={}", id, currentTerm, commitIndex);
        }
    }

    /** 提交点推进后通知各副本（空追加，仅携带 leaderCommit）。 */
    private void broadcastCommit() {
        long lastIdx = lastIndex();
        long lastTerm = lastIdx == baseIndex() ? 0 : entryAt(lastIdx).term();
        for (String peer : peers) {
            if (peer.equals(id)) {
                continue;
            }
            try {
                transport.sendAppendEntries(peer, new Transport.AppendEntriesRequest(
                        currentTerm, id, lastIdx, lastTerm, List.of(), commitIndex));
            } catch (RaftException e) {
                log.info("commit broadcast skipped id={} peer={} term={} reason={}",
                        id, peer, currentTerm, e.code());
            }
        }
    }

    /** 回收已提交的历史前缀，仅保留最近 compactionRetain 条已提交条目。 */
    private void maybeCompact() {
        if (entries.size() < config.getMaxLogEntries()) {
            return;
        }
        long cut = commitIndex - config.getCompactionRetain() + 1;
        if (cut <= baseIndex() + 1) {
            return;
        }
        store.truncatePrefix(cut);
        entries.removeIf(e -> e.index() < cut);
        log.info("log compacted id={} term={} cut={} commitIndex={} remaining={}",
                id, currentTerm, cut, commitIndex, entries.size());
    }

    /** 客户端读取：只返回已提交前缀，任何在线副本均可提供。 */
    public synchronized List<LogEntry> committedEntries() {
        ensureUp();
        List<LogEntry> result = new ArrayList<>();
        for (LogEntry e : entries) {
            if (e.index() <= commitIndex) {
                result.add(e);
            }
        }
        return result;
    }

    /** 处理主副本的追加请求；任期或日志不匹配时拒绝且不改变既有状态。 */
    public synchronized Transport.AppendEntriesResult handleAppendEntries(Transport.AppendEntriesRequest req) {
        if (!up) {
            return new Transport.AppendEntriesResult(false, currentTerm, 0, ErrorCode.NODE_DOWN);
        }
        if (req.term() < 0 || req.prevLogIndex() < 0 || req.entries() == null) {
            return new Transport.AppendEntriesResult(false, currentTerm, 0, ErrorCode.INVALID_ARGUMENT);
        }
        if (req.term() < currentTerm) {
            log.info("append rejected id={} term={} leaderTerm={} reason=TERM_MISMATCH", id, currentTerm, req.term());
            return new Transport.AppendEntriesResult(false, currentTerm, 0, ErrorCode.TERM_MISMATCH);
        }
        if (req.term() > currentTerm) {
            stepDown(req.term());
        }
        role = NodeRole.FOLLOWER;

        long base = baseIndex();
        if (req.prevLogIndex() > lastIndex()) {
            log.info("append rejected id={} term={} prevLogIndex={} lastIndex={} reason=LOG_MISMATCH",
                    id, currentTerm, req.prevLogIndex(), lastIndex());
            return new Transport.AppendEntriesResult(false, currentTerm, 0, ErrorCode.LOG_MISMATCH);
        }
        LogEntry prev = req.prevLogIndex() > base ? entryAt(req.prevLogIndex()) : null;
        if (prev != null && prev.term() != req.prevLogTerm()) {
            store.truncateSuffix(req.prevLogIndex());
            entries.removeIf(e -> e.index() >= req.prevLogIndex());
            log.info("append rejected id={} term={} prevLogIndex={} reason=LOG_MISMATCH(truncated)",
                    id, currentTerm, req.prevLogIndex());
            return new Transport.AppendEntriesResult(false, currentTerm, 0, ErrorCode.LOG_MISMATCH);
        }
        List<LogEntry> toAppend = new ArrayList<>();
        for (LogEntry e : req.entries()) {
            LogEntry existing = entryAt(e.index());
            if (existing != null) {
                if (existing.term() != e.term()) {
                    store.truncateSuffix(e.index());
                    entries.removeIf(x -> x.index() >= e.index());
                    toAppend.add(e);
                }
            } else if (e.index() > lastIndex()) {
                toAppend.add(e);
            }
        }
        if (!toAppend.isEmpty()) {
            store.append(toAppend);
            entries.addAll(toAppend);
        }
        if (req.leaderCommit() > commitIndex) {
            commitIndex = Math.min(req.leaderCommit(), lastIndex());
            store.saveMeta(currentTerm, votedFor, commitIndex);
        }
        log.info("append accepted id={} term={} leader={} prevLogIndex={} appended={} commitIndex={}",
                id, currentTerm, req.leaderId(), req.prevLogIndex(), toAppend.size(), commitIndex);
        return new Transport.AppendEntriesResult(true, currentTerm, lastIndex(), null);
    }

    /** 处理投票请求；任期过期或日志落后时拒绝。 */
    public synchronized Transport.VoteResult handleRequestVote(Transport.VoteRequest req) {
        if (!up) {
            return new Transport.VoteResult(false, currentTerm);
        }
        if (req.term() < currentTerm) {
            return new Transport.VoteResult(false, currentTerm);
        }
        if (req.term() > currentTerm) {
            stepDown(req.term());
        }
        long lastIdx = lastIndex();
        long lastTerm = lastIdx == baseIndex() ? 0 : entryAt(lastIdx).term();
        boolean upToDate = req.lastLogTerm() > lastTerm
                || (req.lastLogTerm() == lastTerm && req.lastLogIndex() >= lastIdx);
        if ((votedFor == null || votedFor.equals(req.candidateId())) && upToDate) {
            votedFor = req.candidateId();
            store.saveMeta(currentTerm, votedFor, commitIndex);
            log.info("vote granted id={} term={} candidate={}", id, currentTerm, req.candidateId());
            return new Transport.VoteResult(true, currentTerm);
        }
        log.info("vote denied id={} term={} candidate={} votedFor={} upToDate={}",
                id, currentTerm, req.candidateId(), votedFor, upToDate);
        return new Transport.VoteResult(false, currentTerm);
    }

    private void validate(String requestId, String payload) {
        if (requestId == null || requestId.isBlank()) {
            throw new RaftException(ErrorCode.INVALID_ARGUMENT, "requestId 不能为空");
        }
        if (payload == null) {
            throw new RaftException(ErrorCode.INVALID_ARGUMENT, "payload 不能为空");
        }
        if (payload.getBytes(StandardCharsets.UTF_8).length > config.getMaxPayloadBytes()) {
            throw new RaftException(ErrorCode.INVALID_ARGUMENT, "payload 超过最大字节数 " + config.getMaxPayloadBytes());
        }
    }

    private void checkResourceLimits() {
        long uncommitted = lastIndex() - commitIndex;
        if (uncommitted >= config.getMaxUncommittedEntries()) {
            throw new RaftException(ErrorCode.RESOURCE_EXHAUSTED,
                    "未提交积压超限 uncommitted=" + uncommitted);
        }
        if (entries.size() >= config.getMaxLogEntries()) {
            maybeCompact();
            if (entries.size() >= config.getMaxLogEntries()) {
                throw new RaftException(ErrorCode.RESOURCE_EXHAUSTED,
                        "日志条数超限 size=" + entries.size());
            }
        }
    }

    private void ensureUp() {
        if (!up) {
            throw new RaftException(ErrorCode.NODE_DOWN, "节点 " + id + " 已崩溃或未启动");
        }
    }
}
