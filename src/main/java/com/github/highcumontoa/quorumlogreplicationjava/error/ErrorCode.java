package com.github.highcumontoa.quorumlogreplicationjava.error;

/** 可区分的错误类型。 */
public enum ErrorCode {
    NOT_LEADER,
    NO_MAJORITY,
    TERM_MISMATCH,
    LOG_MISMATCH,
    STALE_READ,
    INVALID_ARGUMENT,
    LOG_CORRUPTED,
    REPLICA_UNREACHABLE,
    RESOURCE_EXHAUSTED,
    ENTRY_TOO_LARGE,
    INTERNAL
}
