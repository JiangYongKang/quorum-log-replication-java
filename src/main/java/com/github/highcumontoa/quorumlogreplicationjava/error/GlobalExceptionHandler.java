package com.github.highcumontoa.quorumlogreplicationjava.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/** 异常到响应状态的归一化映射：错误类型可区分，底层异常不直接透出。 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Map<ErrorCode, HttpStatus> STATUS = Map.ofEntries(
            Map.entry(ErrorCode.NOT_LEADER, HttpStatus.CONFLICT),
            Map.entry(ErrorCode.NO_MAJORITY, HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry(ErrorCode.TERM_MISMATCH, HttpStatus.CONFLICT),
            Map.entry(ErrorCode.LOG_MISMATCH, HttpStatus.CONFLICT),
            Map.entry(ErrorCode.STALE_READ, HttpStatus.CONFLICT),
            Map.entry(ErrorCode.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST),
            Map.entry(ErrorCode.LOG_CORRUPTED, HttpStatus.INTERNAL_SERVER_ERROR),
            Map.entry(ErrorCode.REPLICA_UNREACHABLE, HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry(ErrorCode.RESOURCE_EXHAUSTED, HttpStatus.INSUFFICIENT_STORAGE),
            Map.entry(ErrorCode.ENTRY_TOO_LARGE, HttpStatus.PAYLOAD_TOO_LARGE),
            Map.entry(ErrorCode.INTERNAL, HttpStatus.INTERNAL_SERVER_ERROR));

    @ExceptionHandler(ReplicationException.class)
    public ResponseEntity<Map<String, Object>> handle(ReplicationException ex) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", false);
        body.put("errorCode", ex.code().name());
        body.put("message", String.valueOf(ex.getMessage()));
        return ResponseEntity.status(STATUS.getOrDefault(ex.code(), HttpStatus.INTERNAL_SERVER_ERROR))
                .body(body);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleOther(Exception ex) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", false);
        body.put("errorCode", ErrorCode.INTERNAL.name());
        body.put("message", "内部错误");
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
    }
}
