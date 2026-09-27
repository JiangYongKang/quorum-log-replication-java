package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.cluster.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.cluster.ReplicaNode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ReplicationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * “历史已完全不可用”副本的追平：空目录重建、历史被回收干净、
 * 追平与正常写入/回收同时进行，追平完成后结论与主副本一致。
 */
class SnapshotCatchUpTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotCatchUpTest.class);
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

    private void wipeDataDir(String id) throws IOException {
        Path d = dir.resolve(id);
        if (!Files.exists(d)) {
            return;
        }
        try (var paths = Files.walk(d)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // 单测目录清理尽力而为
                }
            });
        }
    }

    @Test
    void replicaRebuiltFromEmptyDirectoryCatchesUpViaSnapshot() throws Exception {
        cluster = new Cluster(IDS, dir, 3, 1024 * 1024);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        for (int i = 1; i <= 12; i++) {
            leader.clientWrite("req-" + i, "k" + i, "中文值😀" + i);
        }
        assertTrue(leader.logStore().baseIndex() >= 9, "主副本历史应已回收");

        // n3 崩溃并清空数据目录，模拟由空目录重建的全新副本
        cluster.crash("n3");
        wipeDataDir("n3");
        ReplicaNode rebuilt = cluster.restart("n3");
        assertEquals(0, rebuilt.logStore().lastIndex(), "空目录重建后没有任何日志");
        // 追平完成前：不知道有效主副本，读取被明确拒绝
        assertEquals(ErrorCode.STALE_READ, assertThrows(ReplicationException.class,
                () -> rebuilt.clientRead("k1")).code());

        // 触发追平（与正常写入同时发生）
        leader.clientWrite("req-13", "k13", "v13");
        log.info("追平后 n3 commitIndex={} baseIndex={} lastIndex={}",
                rebuilt.commitIndex(), rebuilt.logStore().baseIndex(), rebuilt.logStore().lastIndex());

        assertEquals(leader.commitIndex(), rebuilt.commitIndex(), "提交点必须追平");
        assertTrue(rebuilt.logStore().baseIndex() > 0, "应通过快照获得基址");
        // 最早的已回收数据与最新数据都逐字一致
        for (int i = 1; i <= 13; i++) {
            String key = "k" + i;
            assertEquals(leader.clientRead(key).orElseThrow(),
                    rebuilt.clientRead(key).orElseThrow(), key + " 追平后必须一致");
        }
    }

    @Test
    void replicaWhoseHistoryWasCollectedCleanCatchesUp() throws Exception {
        cluster = new Cluster(IDS, dir, 3, 1024 * 1024);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        for (int i = 1; i <= 6; i++) {
            leader.clientWrite("req-" + i, "k" + i, "v" + i);
        }
        // 把 n3 隔离，主继续写入并回收，n3 保留的旧前缀与主历史完全分叉
        cluster.setReachable("n3", false);
        for (int i = 7; i <= 15; i++) {
            leader.clientWrite("req-" + i, "k" + i, "v" + i); // n1+n2 多数派
        }
        long base = leader.logStore().baseIndex();
        assertTrue(base > 6, "主副本已回收至 n3 旧日志之后: base=" + base);
        cluster.setReachable("n3", true);
        // 新一轮写入时主发现 n3 的前序全部不可用，走快照追平 + 剩余日志复制
        leader.clientWrite("req-16", "k16", "v16");
        ReplicaNode n3 = cluster.node("n3");
        assertEquals(leader.commitIndex(), n3.commitIndex());
        assertEquals(leader.stateMachine(), n3.stateMachine(), "状态机必须与主副本逐键一致");
        for (int i = 1; i <= 16; i++) {
            assertEquals(leader.clientRead("k" + i).orElseThrow(),
                    n3.clientRead("k" + i).orElseThrow());
        }
    }

    @Test
    void catchUpConcurrentWithWritesAndCompactionConverges() throws Exception {
        cluster = new Cluster(IDS, dir, 4, 1024 * 1024);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        for (int i = 1; i <= 20; i++) {
            leader.clientWrite("req-" + i, "k" + i, "v" + i + "😀");
        }
        // n3 长时间离线，主在此期间持续写入与回收
        cluster.setReachable("n3", false);
        for (int i = 21; i <= 60; i++) {
            leader.clientWrite("req-" + i, "k" + i, "v" + i + "😀");
        }
        cluster.setReachable("n3", true);
        // 恢复后继续大量写入：追平与写入/回收交错，最终必须收敛
        for (int i = 61; i <= 100; i++) {
            leader.clientWrite("req-" + i, "k" + i, "v" + i + "😀");
        }
        ReplicaNode n3 = cluster.node("n3");
        assertEquals(100, n3.commitIndex(), "长时间离线副本最终追平");
        assertEquals(leader.stateMachine(), n3.stateMachine());
        for (int i = 1; i <= 100; i++) {
            assertEquals("v" + i + "😀", n3.clientRead("k" + i).orElseThrow());
        }
        log.info("并发追平收敛 n3 baseIndex={} lastIndex={}",
                n3.logStore().baseIndex(), n3.logStore().lastIndex());
    }

    @Test
    void catchingUpReplicaNeverExposesHalfStateAndConvergesAfterRestartOfLeader() throws Exception {
        cluster = new Cluster(IDS, dir, 3, 1024 * 1024);
        ReplicaNode n1 = cluster.node("n1");
        n1.becomeLeader();
        for (int i = 1; i <= 10; i++) {
            n1.clientWrite("req-" + i, "k" + i, "v" + i);
        }
        // n3 由空目录重建
        cluster.crash("n3");
        wipeDataDir("n3");
        ReplicaNode n3 = cluster.restart("n3");

        // 追平过程中读取：要么 STALE_READ（尚未可信），要么与主副本完全一致，绝不半旧
        for (int round = 0; round < 6; round++) {
            n1.clientWrite("req-burst-" + round, "burst" + round, "值" + round);
            Map<String, String> leaderState = n1.stateMachine();
            if (n3.commitIndex() == n1.commitIndex() && n3.leaderId() != null) {
                assertEquals(leaderState, n3.stateMachine(),
                        "一旦追平，状态机必须与主副本完全一致，不能停在半旧状态");
            } else {
                // 未追平时不得对外给出可能误导的已提交结论
                assertThrows(ReplicationException.class, () -> n3.clientRead("k1"));
            }
        }
        assertEquals(n1.commitIndex(), n3.commitIndex());
        assertEquals(n1.stateMachine(), n3.stateMachine());

        // 主副本更替后，追平副本仍与新主一致
        cluster.crash("n1");
        ReplicaNode n2 = cluster.node("n2");
        n2.becomeLeader();
        n2.clientWrite("req-after-failover", "kf", "vf😀");
        assertEquals(n2.commitIndex(), n3.commitIndex());
        assertEquals("vf😀", n3.clientRead("kf").orElseThrow());
        assertEquals("v1", n3.clientRead("k1").orElseThrow(), "最早的快照数据仍然可读");
    }
}
