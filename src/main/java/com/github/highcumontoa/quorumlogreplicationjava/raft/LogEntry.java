package com.github.highcumontoa.quorumlogreplicationjava.raft;

/**
 * 日志条目。
 *
 * @param term      写入时的主副本任期
 * @param index     日志位置（从 1 开始连续递增）
 * @param requestId 客户端请求标识，用于幂等去重
 * @param payload   变更内容
 */
public record LogEntry(long term, long index, String requestId, String payload) {
}
