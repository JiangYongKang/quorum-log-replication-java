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

    /**
     * 快照安装请求：跟随者本地历史已不可用时，主副本直接发送已提交状态机快照。
     *
     * @param lastIncludedIndex 快照覆盖的最后一条已提交日志索引
     * @param lastIncludedTerm  该索引对应条目的任期
     */
    record SnapshotRequest(long term, String leaderId, long lastIncludedIndex,
                           long lastIncludedTerm, Map<String, String> state) {
    }

    record SnapshotResponse(long term, boolean success, long matchIndex) {
    }

    /** read-index 查询：跟随者向主副本确认“当前已提交点”。 */
    record ReadIndexRequest(long term, String requesterId) {
    }

    record ReadIndexResponse(long term, String leaderId, long commitIndex) {
    }

    AppendResponse append(String targetId, AppendRequest request);

    SnapshotResponse installSnapshot(String targetId, SnapshotRequest request);

    ReadIndexResponse queryReadIndex(String targetId, ReadIndexRequest request);

    /** 标记某节点不可达（模拟崩溃/网络隔离）。 */
    void setReachable(String nodeId, boolean reachable);
}
