package com.github.highcumontoa.quorumlogreplicationjava.api;

import com.github.highcumontoa.quorumlogreplicationjava.raft.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.raft.RaftException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.EnumMap;
import java.util.Map;

/** 异常归一化：RaftException 映射为可区分的 HTTP 状态与错误码，底层异常不直接透出。 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    private static final Map<ErrorCode, HttpStatus> STATUS = new EnumMap<>(Map.of(
            ErrorCode.NOT_LEADER, HttpStatus.CONFLICT,
            ErrorCode.TERM_MISMATCH, HttpStatus.CONFLICT,
            ErrorCode.LOG_MISMATCH, HttpStatus.CONFLICT,
            ErrorCode.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST,
            ErrorCode.UNAVAILABLE, HttpStatus.SERVICE_UNAVAILABLE,
            ErrorCode.NODE_DOWN, HttpStatus.SERVICE_UNAVAILABLE,
            ErrorCode.RESOURCE_EXHAUSTED, HttpStatus.TOO_MANY_REQUESTS,
            ErrorCode.CORRUPTED_LOG, HttpStatus.INTERNAL_SERVER_ERROR
    ));

    @ExceptionHandler(RaftException.class)
    public ResponseEntity<Dtos.ErrorResponse> handleRaft(RaftException ex) {
        HttpStatus status = STATUS.getOrDefault(ex.code(), HttpStatus.INTERNAL_SERVER_ERROR);
        log.info("request rejected code={} status={} message={}", ex.code(), status.value(), ex.getMessage());
        return ResponseEntity.status(status).body(new Dtos.ErrorResponse(ex.code().name(), ex.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Dtos.ErrorResponse> handleOther(Exception ex) {
        log.error("unexpected error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new Dtos.ErrorResponse("INTERNAL", "内部错误"));
    }
}
