package com.github.highcumontoa.quorumlogreplicationjava.api;

import java.util.List;

/** 接口出入参。 */
public final class Dtos {

    private Dtos() {
    }

    public record WriteRequest(String requestId, String payload) {
    }

    public record WriteResponse(long term, long index, String requestId, boolean committed) {
    }

    public record ReadResponse(long commitIndex, List<EntryView> entries) {
    }

    public record EntryView(long term, long index, String requestId, String payload) {
    }

    public record ErrorResponse(String code, String message) {
    }
}
