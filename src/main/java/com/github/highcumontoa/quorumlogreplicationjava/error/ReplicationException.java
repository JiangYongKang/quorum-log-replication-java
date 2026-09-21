package com.github.highcumontoa.quorumlogreplicationjava.error;

/** 复制层统一异常，携带可区分错误码。 */
public class ReplicationException extends RuntimeException {
    private final ErrorCode code;

    public ReplicationException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ReplicationException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
