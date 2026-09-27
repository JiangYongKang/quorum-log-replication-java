package com.github.highcumontoa.quorumlogreplicationjava.tests;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * REST 入口行为测试：写入确认、读取可见性与错误类型在各入口下保持一致。
 */
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ApiTest {

    @TempDir
    static Path dataDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("replication.data-dir", () -> dataDir.toString());
        registry.add("replication.max-log-entries", () -> "1000");
        registry.add("replication.max-write-bytes", () -> "16");
    }

    @Autowired
    MockMvc mvc;

    @Test
    @Order(1)
    void writeOnFollowerIsRejectedWithDistinctError() throws Exception {
        mvc.perform(post("/api/nodes/n1/elect")).andExpect(status().isOk());
        mvc.perform(post("/api/nodes/n2/write").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"r-f\",\"key\":\"k\",\"value\":\"v\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("NOT_LEADER"));
    }

    @Test
    @Order(2)
    void writeThenReadIsConsistentAcrossEntries() throws Exception {
        mvc.perform(post("/api/nodes/n1/write").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"r-1\",\"key\":\"k1\",\"value\":\"v1\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.index").value(1));
        // 主副本与跟随者入口读取结果一致
        for (String id : new String[]{"n1", "n2", "n3"}) {
            mvc.perform(get("/api/nodes/" + id + "/read").param("key", "k1"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.ok").value(true))
                    .andExpect(jsonPath("$.value").value("v1"));
        }
    }

    @Test
    @Order(3)
    void duplicateWriteIsIdempotentViaApi() throws Exception {
        mvc.perform(post("/api/nodes/n1/write").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"r-1\",\"key\":\"k1\",\"value\":\"v1\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.index").value(1)); // 与首次相同，不重复追加
    }

    @Test
    @Order(4)
    void invalidArgumentIsRejected() throws Exception {
        mvc.perform(post("/api/nodes/n1/write").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"\",\"key\":\"k\",\"value\":\"v\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_ARGUMENT"));
    }

    @Test
    @Order(5)
    void writeFailsWithNoMajorityWhenReplicasDown() throws Exception {
        mvc.perform(post("/api/nodes/n2/crash")).andExpect(status().isOk());
        mvc.perform(post("/api/nodes/n3/crash")).andExpect(status().isOk());
        mvc.perform(post("/api/nodes/n1/write").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"r-2\",\"key\":\"k2\",\"value\":\"v2\"}"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.errorCode").value("NO_MAJORITY"));
        // 未提交数据不可见
        mvc.perform(get("/api/nodes/n1/read").param("key", "k2"))
                .andExpect(jsonPath("$.ok").value(false));
    }

    @Test
    @Order(6)
    void committedDataSurvivesRestartViaApi() throws Exception {
        mvc.perform(post("/api/nodes/n1/crash")).andExpect(status().isOk());
        mvc.perform(post("/api/nodes/n1/restart")).andExpect(status().isOk());
        // 重启后是跟随者角色，在可信主副本恢复前读取必须明确拒绝，不能凭本地数据下结论
        mvc.perform(get("/api/nodes/n1/read").param("key", "k1"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("STALE_READ"));
        // 已提交数据未丢失
        mvc.perform(get("/api/nodes/n1/status"))
                .andExpect(jsonPath("$.commitIndex").value(1));
        // 重新当选（成为当前主副本）后读取自动恢复
        mvc.perform(post("/api/nodes/n1/elect")).andExpect(status().isOk());
        mvc.perform(get("/api/nodes/n1/read").param("key", "k1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.value").value("v1"));
    }

    @Test
    @Order(7)
    void oversizedWriteIsRejectedWithDistinctErrorViaApi() throws Exception {
        mvc.perform(post("/api/nodes/n2/restart")).andExpect(status().isOk());
        mvc.perform(post("/api/nodes/n3/restart")).andExpect(status().isOk());
        mvc.perform(post("/api/nodes/n1/elect")).andExpect(status().isOk());
        mvc.perform(post("/api/nodes/n1/write").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"r-big\",\"key\":\"kb\",\"value\":\""
                                + "x".repeat(100) + "\"}"))
                .andExpect(status().isPayloadTooLarge())
                .andExpect(jsonPath("$.errorCode").value("PAYLOAD_TOO_LARGE"));
        // 拒绝不留半成品：提交点与既有数据不变
        mvc.perform(get("/api/nodes/n1/status"))
                .andExpect(jsonPath("$.commitIndex").value(1));
        mvc.perform(get("/api/nodes/n1/read").param("key", "kb"))
                .andExpect(jsonPath("$.ok").value(false));
    }

    @Test
    @Order(8)
    void rebuiltNodeRejectsReadUntilSyncedViaApi() throws Exception {
        // 空目录重建：不知道主副本，读取被明确拒绝而不是返回空值
        mvc.perform(post("/api/nodes/n3/rebuild")).andExpect(status().isOk());
        mvc.perform(get("/api/nodes/n3/read").param("key", "k1"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("STALE_READ"));
        // 触发追平后读取自动恢复，且与主副本一致
        mvc.perform(post("/api/cluster/sync")).andExpect(status().isOk());
        mvc.perform(get("/api/nodes/n3/read").param("key", "k1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.value").value("v1"));
        mvc.perform(get("/api/nodes/n3/status"))
                .andExpect(jsonPath("$.commitIndex").value(1));
    }
}
