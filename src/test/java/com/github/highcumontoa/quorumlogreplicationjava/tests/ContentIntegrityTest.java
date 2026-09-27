package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.cluster.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.cluster.ReplicaNode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ReplicationException;
import com.github.highcumontoa.quorumlogreplicationjava.log.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 内容与容量：任意文本逐字完整（含中文、emoji、长文本）、单条写入大小上限、
 * 回收与崩溃重启后的内容完整性。
 */
class ContentIntegrityTest {

    private static final Logger log = LoggerFactory.getLogger(ContentIntegrityTest.class);
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

    @Test
    void arbitraryUnicodeTextReplicatesAndReadsVerbatimOnAllReplicas() {
        cluster = new Cluster(IDS, dir, 1000, 1024 * 1024);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();

        String emoji = "😀🚀👨‍👩‍👧";
        String mixed = "中文键值 mixed-ASCII \t\n换行 \"引号\" \\反斜杠 テスト 🎉";
        leader.clientWrite("req-emoji", "键-emoji", emoji);
        leader.clientWrite("req-mixed", "键-混合", mixed);

        for (String id : IDS) {
            assertEquals(emoji, cluster.node(id).clientRead("键-emoji").orElseThrow(),
                    id + " emoji 必须逐字一致");
            assertEquals(mixed, cluster.node(id).clientRead("键-混合").orElseThrow(),
                    id + " 混合文本必须逐字一致");
        }
    }

    @Test
    void longTextIsAcceptedUpToLimitAndReadsVerbatim() {
        cluster = new Cluster(IDS, dir, 1000, 4096);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();

        StringBuilder sb = new StringBuilder();
        // 3000 个中文，UTF-8 编码后 9000 字节——按字符数算会误判，按字节才算对
        sb.append("长".repeat(3000));
        String value = sb.toString();
        // 超 4096 字节：拒绝
        ReplicationException tooLarge = assertThrows(ReplicationException.class,
                () -> leader.clientWrite("req-big", "big", value));
        assertEquals(ErrorCode.ENTRY_TOO_LARGE, tooLarge.code());
        log.info("超限写入被拒绝 code={} bytes≈{}", tooLarge.code(), value.getBytes().length);

        // 恰好在上限内的长中文文本应成功且逐字可读
        String ok = "长".repeat(1300); // 3900 UTF-8 字节 + 开销 < 4096
        leader.clientWrite("req-long", "long", ok);
        assertEquals(ok, leader.clientRead("long").orElseThrow());
        assertEquals(ok, cluster.node("n3").clientRead("long").orElseThrow(),
                "跟随者上的长文本必须逐字一致");
    }

    @Test
    void oversizedWriteIsRejectedWithoutAnySideEffect() {
        cluster = new Cluster(IDS, dir, 1000, 64);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");
        long commitBefore = leader.commitIndex();
        long lastBefore = leader.logStore().lastIndex();

        // requestId+key+value 的 UTF-8 字节合计超 64
        String huge = "字".repeat(100);
        ReplicationException ex = assertThrows(ReplicationException.class,
                () -> leader.clientWrite("req-huge", "k-huge", huge));
        assertEquals(ErrorCode.ENTRY_TOO_LARGE, ex.code());

        // 拒绝后：提交点、日志范围、状态机全部不变，且无半成品
        assertEquals(commitBefore, leader.commitIndex(), "拒绝不得改变提交结果");
        assertEquals(lastBefore, leader.logStore().lastIndex(), "拒绝不得追加日志");
        assertTrue(leader.clientRead("k-huge").isEmpty(), "超限内容不得残留");
        for (String id : IDS) {
            assertEquals(commitBefore, cluster.node(id).commitIndex(),
                    id + " 副本状态不得被超限写入改动");
        }
        // 同 requestId 之后以合规大小重提不受影响（前次没有留下去重痕迹）
        leader.clientWrite("req-huge", "k-huge", "小");
        assertEquals("小", leader.clientRead("k-huge").orElseThrow());
    }

    @Test
    void unicodeContentStaysVerbatimAfterCompactionAndRestart() {
        cluster = new Cluster(IDS, dir, 5, 1024 * 1024);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        String emoji = "🎉🎉🎉";
        for (int i = 1; i <= 12; i++) {
            String v = (i % 2 == 0) ? "值-" + i + "-中文😀" : "v" + i;
            leader.clientWrite("req-" + i, "键-" + i, v);
        }
        leader.clientWrite("req-emoji", "表情键", emoji);
        assertTrue(leader.logStore().baseIndex() > 0, "应已发生回收");
        log.info("回收后 baseIndex={} lastIndex={}", leader.logStore().baseIndex(),
                leader.logStore().lastIndex());

        // 全部副本：回收后的已提交内容仍逐字可读
        for (String id : IDS) {
            ReplicaNode n = cluster.node(id);
            for (int i = 1; i <= 12; i++) {
                String expected = (i % 2 == 0) ? "值-" + i + "-中文😀" : "v" + i;
                assertEquals(expected, n.clientRead("键-" + i).orElseThrow(),
                        id + " 回收后内容必须逐字完整: 键-" + i);
            }
            assertEquals(emoji, n.clientRead("表情键").orElseThrow());
        }

        // 全部副本崩溃重启：磁盘恢复后内容、日志范围自洽
        for (String id : IDS) {
            cluster.crash(id);
        }
        ReplicaNode r1 = cluster.restart("n1");
        r1.becomeLeader();
        ReplicaNode r2 = cluster.restart("n2");
        ReplicaNode r3 = cluster.restart("n3");
        // 重新同步前读取不可信（不知道有效主副本）
        assertEquals(ErrorCode.STALE_READ,
                assertThrows(ReplicationException.class, () -> r2.clientRead("键-1")).code());
        // 一次新写入触发对重启副本的接触与同步，随后读取自动恢复
        r1.clientWrite("req-post-restart", "重启后键", "重启后值😀");
        for (String id : IDS) {
            ReplicaNode n = cluster.node(id);
            for (int i = 1; i <= 12; i++) {
                String expected = (i % 2 == 0) ? "值-" + i + "-中文😀" : "v" + i;
                assertEquals(expected, n.clientRead("键-" + i).orElseThrow(),
                        id + " 重启后内容必须逐字完整: 键-" + i);
            }
            assertEquals(emoji, n.clientRead("表情键").orElseThrow(),
                    id + " 重启后 emoji 不得消失或截断");
            assertEquals("重启后值😀", n.clientRead("重启后键").orElseThrow());
            assertEquals(14, n.commitIndex(), id + " 提交点重启前后自洽");
        }
        assertTrue(r1.logStore().baseIndex() > 0, "主副本回收基址重启后必须保持");
        assertTrue(r1.logStore().get(1).isEmpty(), "主副本已回收条目不应复活");
    }

    @Test
    void multibyteEntriesSurviveConflictTruncationAfterCompaction() {
        // 多字节内容回收后，offset 必须按 UTF-8 字节数计算，否则冲突截断会切错位置
        cluster = new Cluster(List.of("n1", "n2", "n3"), dir, 3, 1024 * 1024);
        ReplicaNode n1 = cluster.node("n1");
        n1.becomeLeader();
        for (int i = 1; i <= 8; i++) {
            n1.clientWrite("req-a-" + i, "中文键" + i, "emoji值😀😀" + i);
        }
        // n3 隔离后，n1 继续写并回收；随后由（已回收的）磁盘目录重建 n3，仍保持隔离
        cluster.setReachable("n3", false);
        n1.clientWrite("req-a-9", "中文键9", "值9😀");
        cluster.restart("n3"); // 从已回收磁盘目录重建（历史完全不可用）
        cluster.setReachable("n3", false);
        n1.clientWrite("req-a-10a", "离线键", "离线值😀"); // n1+n2 多数派，继续回收
        cluster.setReachable("n3", true);
        // 新主写入触发对 n3 的快照追平 + 后续复制，n3 不得出现 CRC 损坏
        n1.clientWrite("req-a-10", "中文键10", "值10😀");
        ReplicaNode n3 = cluster.node("n3");
        assertEquals(n1.commitIndex(), n3.commitIndex());
        assertEquals("离线值😀", n3.clientRead("离线键").orElseThrow(), "隔离期间的多数派写入也要追平");
        for (int i = 1; i <= 10; i++) {
            assertEquals(n1.clientRead("中文键" + i).orElseThrow(),
                    n3.clientRead("中文键" + i).orElseThrow(), "中文键" + i);
        }
        // 直接核对两副本仍保留在日志中的多字节记录逐字一致
        long start = Math.max(n3.logStore().baseIndex(), n1.logStore().baseIndex()) + 1;
        for (long idx = start; idx <= n3.logStore().lastIndex(); idx++) {
            LogEntry e3 = n3.logStore().get(idx).orElseThrow();
            LogEntry e1 = n1.logStore().get(idx).orElseThrow();
            assertEquals(e1, e3, "index=" + idx + " 多字节记录必须一致");
        }
    }
}
