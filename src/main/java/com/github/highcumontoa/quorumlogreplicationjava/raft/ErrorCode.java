package com.github.highcumontoa.quorumlogreplicationjava.raft;

/** 可区分的错误类型，用于接口行为与测试断言。 */
public enum ErrorCode {
    /** 当前节点不是主副本，或主副本未知。 */
    NOT_LEADER,
    /** 任期不匹配（请求任期过期或节点任期落后）。 */
    TERM_MISMATCH,
    /** 日志位置/任期不匹配，追加被拒绝。 */
    LOG_MISMATCH,
    /** 本地日志损坏（校验和失败等）。 */
    CORRUPTED_LOG,
    /** 副本不可达或多数派不足。 */
    UNAVAILABLE,
    /** 参数非法或越权操作。 */
    INVALID_ARGUMENT,
    /** 超出限流/容量策略。 */
    RESOURCE_EXHAUSTED,
    /** 节点已崩溃或未恢复，不能提供读写。 */
    NODE_DOWN
}
