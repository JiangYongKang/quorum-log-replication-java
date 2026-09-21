package com.github.highcumontoa.quorumlogreplicationjava.raft;

import java.util.List;

/** 节点间通信抽象，本机以内存实现模拟网络与故障。 */
public interface Transport {

    record AppendEntriesRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                List<LogEntry> entries, long leaderCommit) {
    }

    record AppendEntriesResult(boolean success, long term, long matchIndex, ErrorCode error) {
    }

    record VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
    }

    record VoteResult(boolean granted, long term) {
    }

    AppendEntriesResult sendAppendEntries(String targetId, AppendEntriesRequest request);

    VoteResult sendRequestVote(String targetId, VoteRequest request);

    void setReachable(String nodeId, boolean reachable);
}
