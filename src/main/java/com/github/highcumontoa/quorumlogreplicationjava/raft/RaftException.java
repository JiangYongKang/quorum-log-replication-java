package com.github.highcumontoa.quorumlogreplicationjava.raft;

/** 归一化异常：底层 IO 等异常一律包装为本类型，不直接透出。 */
public class RaftException extends RuntimeException {

    private final ErrorCode code;

    public RaftException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public RaftException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
