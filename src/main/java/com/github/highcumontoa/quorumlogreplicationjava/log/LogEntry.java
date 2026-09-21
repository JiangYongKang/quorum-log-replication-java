package com.github.highcumontoa.quorumlogreplicationjava.log;

/**
 * 一条复制日志记录。
 *
 * @param term      写入该条目时的任期
 * @param index     日志位置（单调递增，从 1 开始）
 * @param requestId 客户端请求标识，用于幂等去重
 * @param key       变更键
 * @param value     变更值
 */
public record LogEntry(long term, long index, String requestId, String key, String value) {
}
