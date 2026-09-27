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
        registry.add("replication.max-entry-bytes", () -> "64");
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
        // 重启后尚未确认有效主副本：读取被明确拒绝，而不是伪装成“键不存在”
        mvc.perform(get("/api/nodes/n1/read").param("key", "k1"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("STALE_READ"));
        // 重新选主（恢复可信状态）后读取自动恢复
        mvc.perform(post("/api/nodes/n1/elect")).andExpect(status().isOk());
        mvc.perform(get("/api/nodes/n1/read").param("key", "k1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.value").value("v1"));
        mvc.perform(get("/api/nodes/n1/status"))
                .andExpect(jsonPath("$.commitIndex").value(1));
    }

    @Test
    @Order(7)
    void oversizedWriteIsRejectedWithDistinctError() throws Exception {
        // 恢复 order 5 中崩溃的跟随者，恢复多数派
        mvc.perform(post("/api/nodes/n2/restart")).andExpect(status().isOk());
        mvc.perform(post("/api/nodes/n3/restart")).andExpect(status().isOk());
        // n1 在 order 6 重新成为主副本；UTF-8 中文每字 3 字节，30 字约 90 字节 > 64
        String big = "中".repeat(30);
        mvc.perform(post("/api/nodes/n1/write").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"r-big\",\"key\":\"kbig\",\"value\":\"" + big + "\"}"))
                .andExpect(status().isPayloadTooLarge())
                .andExpect(jsonPath("$.errorCode").value("ENTRY_TOO_LARGE"));
        // 拒绝不留痕：查不到该键，提交点不变
        mvc.perform(get("/api/nodes/n1/read").param("key", "kbig"))
                .andExpect(jsonPath("$.ok").value(false));
        mvc.perform(get("/api/nodes/n1/status"))
                .andExpect(jsonPath("$.commitIndex").value(1));
        // 同 requestId 以合规大小重提成功
        mvc.perform(post("/api/nodes/n1/write").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"r-big\",\"key\":\"kbig\",\"value\":\"小\"}"))
                .andExpect(status().isOk());
    }

    @Test
    @Order(8)
    void followerUnknownLeaderReadIsRejectedAsStale() throws Exception {
        // 重启 n3 后其尚未接触主副本，读取被明确拒绝而非返回空值
        mvc.perform(post("/api/nodes/n3/crash")).andExpect(status().isOk());
        mvc.perform(post("/api/nodes/n3/restart")).andExpect(status().isOk());
        mvc.perform(get("/api/nodes/n3/read").param("key", "k1"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("STALE_READ"));
    }
}
