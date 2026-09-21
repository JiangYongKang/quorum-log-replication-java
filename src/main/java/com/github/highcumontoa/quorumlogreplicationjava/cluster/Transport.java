package com.github.highcumontoa.quorumlogreplicationjava.cluster;

import com.github.highcumontoa.quorumlogreplicationjava.log.LogEntry;

import java.util.List;

/** 节点间通信抽象（本机进程内实现）。 */
public interface Transport {

    record AppendRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit) {
    }

    record AppendResponse(long term, boolean success, long matchIndex) {
    }

    AppendResponse append(String targetId, AppendRequest request);

    /** 标记某节点不可达（模拟崩溃/网络隔离）。 */
    void setReachable(String nodeId, boolean reachable);
}
