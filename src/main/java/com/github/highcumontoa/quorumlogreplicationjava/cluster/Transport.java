package com.github.highcumontoa.quorumlogreplicationjava.cluster;

import com.github.highcumontoa.quorumlogreplicationjava.log.LogEntry;

import java.util.List;
import java.util.Map;

/** 节点间通信抽象（本机进程内实现）。 */
public interface Transport {

    record AppendRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit) {
    }

    record AppendResponse(long term, boolean success, long matchIndex) {
    }

    /** 读校验：跟随者向已知主副本确认当前已提交水位。 */
    record ReadCheckRequest(long term, String fromId) {
    }

    record ReadCheckResponse(long term, boolean isLeader, long commitIndex) {
    }

    /** 快照传输：把已提交状态机整体发给历史不可用的跟随者。 */
    record SnapshotRequest(long term, String leaderId, long lastIncludedIndex,
                           long lastIncludedTerm, Map<String, String> state) {
    }

    record SnapshotResponse(long term, boolean success, long matchIndex) {
    }

    AppendResponse append(String targetId, AppendRequest request);

    ReadCheckResponse readCheck(String targetId, ReadCheckRequest request);

    SnapshotResponse snapshot(String targetId, SnapshotRequest request);

    /** 标记某节点不可达（模拟崩溃/网络隔离）。 */
    void setReachable(String nodeId, boolean reachable);
}
