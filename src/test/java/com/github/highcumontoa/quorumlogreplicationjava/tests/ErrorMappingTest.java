package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.raft.Cluster;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 入口行为：错误类型与 HTTP 状态可区分，拒绝不改变既有状态。 */
@SpringBootTest(properties = {
        "raft.data-dir=target/it-data-error-mapping-${random.uuid}",
        "raft.nodes=node1,node2,node3"
})
@AutoConfigureMockMvc
class ErrorMappingTest {

    @Autowired
    MockMvc mvc;

    @Autowired
    Cluster cluster;

    @Test
    void blankRequestIdIsRejectedAsInvalidArgument() throws Exception {
        mvc.perform(post("/client/write")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"\",\"payload\":\"v\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_ARGUMENT"));
    }

    @Test
    void writeAndReadRoundTrip() throws Exception {
        mvc.perform(post("/client/write")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"req-http-1\",\"payload\":\"hello\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.committed").value(true))
                .andExpect(jsonPath("$.index").value(1));

        mvc.perform(get("/client/read"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.entries[0].payload").value("hello"));

        // 重复提交：幂等返回同一位置，不重复计账
        mvc.perform(post("/client/write")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"req-http-1\",\"payload\":\"hello\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.index").value(1));
    }

    @Test
    void writeWithoutMajorityReturnsServiceUnavailable() throws Exception {
        cluster.crashNode("node2");
        cluster.crashNode("node3");
        try {
            mvc.perform(post("/client/write")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"requestId\":\"req-http-2\",\"payload\":\"v\"}"))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(jsonPath("$.code").value("UNAVAILABLE"));
        } finally {
            cluster.restartNode("node2");
            cluster.restartNode("node3");
        }
    }

    @Test
    void nodeRequestWithBlankLeaderIdIsRejected() throws Exception {
        mvc.perform(post("/raft/append-entries").param("node", "node2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"term\":1,\"leaderId\":\"\",\"prevLogIndex\":0,\"prevLogTerm\":0,"
                                + "\"entries\":[],\"leaderCommit\":0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_ARGUMENT"));
    }

    @Test
    void nodeRequestToUnknownNodeIsRejected() throws Exception {
        mvc.perform(post("/raft/append-entries").param("node", "ghost")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"term\":1,\"leaderId\":\"node1\",\"prevLogIndex\":0,\"prevLogTerm\":0,"
                                + "\"entries\":[],\"leaderCommit\":0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_ARGUMENT"));
    }

    @Test
    void staleTermAppendIsReportedAsTermMismatch() throws Exception {
        mvc.perform(post("/raft/append-entries").param("node", "node2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"term\":0,\"leaderId\":\"node1\",\"prevLogIndex\":0,\"prevLogTerm\":0,"
                                + "\"entries\":[],\"leaderCommit\":0}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error").value("TERM_MISMATCH"));
    }
}
