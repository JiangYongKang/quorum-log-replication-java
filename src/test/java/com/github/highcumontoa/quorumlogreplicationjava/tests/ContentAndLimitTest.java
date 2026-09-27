package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.cluster.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.cluster.ReplicaNode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ReplicationException;
import com.github.highcumontoa.quorumlogreplicationjava.log.LogEntry;
import com.github.highcumontoa.quorumlogreplicationjava.log.LogStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 任意文本内容与单条写入大小上限：
 * 中文/emoji/长文本逐字保留，超限写入以可区分原因拒绝且不留半成品。
 */
class ContentAndLimitTest {

    private static final Logger log = LoggerFactory.getLogger(ContentAndLimitTest.class);
    private static final List<String> IDS = List.of("n1", "n2", "n3");

    @TempDir
    Path dir;

    private Cluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private static Map<String, String> trickyContents() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("中文键", "中文值：日志复制，多数派提交。");
        m.put("emoji", "🎉🚀 混合 emoji 与文本 👍🏽");
        m.put("混合", "中文 mixed English 123 と日本語");
        m.put("special=:=", "包含=等号:冒号\n换行\t制表 末尾空格 ");
        m.put(" 前导空格", "  值的前导与尾部空格  ");
        m.put("长文本", "长".repeat(50_000) + "🎉".repeat(1_000));
        return m;
    }

    @Test
    void arbitraryTextRoundTripsVerbatimAcrossReplicas() {
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();

        Map<String, String> contents = trickyContents();
        int i = 0;
        for (Map.Entry<String, String> e : contents.entrySet()) {
            leader.clientWrite("req-" + (++i), e.getKey(), e.getValue());
        }
        log.info("写入 {} 条任意文本, commitIndex={}", contents.size(), leader.commitIndex());

        for (String id : IDS) {
            ReplicaNode n = cluster.node(id);
            for (Map.Entry<String, String> e : contents.entrySet()) {
                assertEquals(e.getValue(), n.clientRead(e.getKey()).orElseThrow(),
                        id + " 读到键 [" + e.getKey() + "] 的内容必须逐字一致");
            }
        }
    }

    @Test
    void oversizedWriteIsRejectedWithDistinctReasonAndNoSideEffects() {
        cluster = new Cluster(IDS, dir, 1000, 100); // 单条上限 100 字节（key+value）
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-ok", "k1", "v1");

        long commitBefore = leader.commitIndex();
        long lastBefore = leader.logStore().lastIndex();
        Map<String, String> stateBefore = leader.stateMachine();

        // 超限 value：拒绝原因必须可与其它拒绝区分
        ReplicationException ex = assertThrows(ReplicationException.class,
                () -> leader.clientWrite("req-big", "k2", "x".repeat(200)));
        assertEquals(ErrorCode.PAYLOAD_TOO_LARGE, ex.code());
        assertNotEquals(ErrorCode.NO_MAJORITY, ex.code());
        assertNotEquals(ErrorCode.RESOURCE_EXHAUSTED, ex.code());
        log.info("超限写入被拒绝 code={} msg={}", ex.code(), ex.getMessage());

        // 超限 key 同样拒绝
        ReplicationException exKey = assertThrows(ReplicationException.class,
                () -> leader.clientWrite("req-big-key", "k".repeat(200), "v"));
        assertEquals(ErrorCode.PAYLOAD_TOO_LARGE, exKey.code());

        // 拒绝不得改变任何提交结果或副本状态，不留半成品
        assertEquals(commitBefore, leader.commitIndex());
        assertEquals(lastBefore, leader.logStore().lastIndex());
        assertEquals(stateBefore, leader.stateMachine());
        assertTrue(leader.clientRead("k2").isEmpty(), "被拒绝的写入不得可见");
        for (String id : List.of("n2", "n3")) {
            assertEquals(lastBefore, cluster.node(id).logStore().lastIndex());
        }

        // 边界：恰好等于上限（key+value 的 UTF-8 字节数）必须受理
        String key = "中".repeat(10);   // 30 字节
        String value = "文".repeat(35); // 105 字节 → 共 65？不够，凑到恰好 100
        value = "文".repeat(23) + "a";  // 69+1=70 字节，合计 100
        LogEntry e = leader.clientWrite("req-edge", key, value);
        assertEquals(100, (key + value).getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        assertEquals(value, leader.clientRead(key).orElseThrow());
        log.info("边界写入被受理 index={}", e.index());
    }

    @Test
    void contentSurvivesRetentionAndRestartVerbatim() {
        cluster = new Cluster(IDS, dir, 5); // 小日志上限，强制回收
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();

        Map<String, String> contents = trickyContents();
        int i = 0;
        for (Map.Entry<String, String> e : contents.entrySet()) {
            leader.clientWrite("req-" + (++i), e.getKey(), e.getValue());
        }
        assertTrue(leader.logStore().baseIndex() > 0, "应已发生历史回收");
        log.info("回收后 baseIndex={} commitIndex={}", leader.logStore().baseIndex(), leader.commitIndex());

        // 回收后、重启前：所有副本逐字一致
        for (String id : IDS) {
            for (Map.Entry<String, String> e : contents.entrySet()) {
                assertEquals(e.getValue(), cluster.node(id).clientRead(e.getKey()).orElseThrow(),
                        id + " 回收后读取键 [" + e.getKey() + "] 必须逐字一致");
            }
        }

        // 全部崩溃重启：内容不得丢失或截断，日志范围自洽
        for (String id : IDS) {
            cluster.crash(id);
        }
        for (String id : IDS) {
            ReplicaNode n = cluster.restart(id);
            assertEquals(contents.size(), n.commitIndex(), id + " 重启后提交点自洽");
            assertTrue(n.logStore().lastIndex() >= n.logStore().baseIndex(),
                    id + " 日志范围自洽");
            for (Map.Entry<String, String> e : contents.entrySet()) {
                assertEquals(e.getValue(), n.clientRead(e.getKey()).orElseThrow(),
                        id + " 重启后读取键 [" + e.getKey() + "] 必须逐字一致");
            }
        }
        log.info("回收+重启后全部内容逐字完整");
    }

    @Test
    void logStoreOffsetsStayCorrectWithMultibyteContent() {
        // 直接针对 LogStore：多字节内容下前缀回收后的偏移量必须仍然正确
        LogStore store = new LogStore(dir.resolve("ls"));
        for (int i = 1; i <= 6; i++) {
            store.append(new LogEntry(1, i, "r" + i, "键🎉" + i, "值".repeat(10) + i));
        }
        store.truncatePrefix(4); // 回收 1..3，保留 4..6
        store.truncateSuffix(5); // 再截断 5..6 —— 偏移量若按字符数算会切错位置
        store.append(new LogEntry(1, 5, "r5b", "新键", "新值🎉"));
        store.close();

        // 重新打开做全量恢复校验：任何偏移错误都会表现为撕裂写或 CRC 失败
        LogStore reopened = new LogStore(dir.resolve("ls"));
        assertEquals(3, reopened.baseIndex());
        assertEquals(5, reopened.lastIndex());
        assertEquals("值".repeat(10) + "4", reopened.get(4).orElseThrow().value());
        assertEquals("新值🎉", reopened.get(5).orElseThrow().value());
        reopened.close();
        log.info("多字节内容下回收/截断/重开恢复一致");
    }
}
