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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 历史完全不可用副本的快照追平（InstallSnapshot）：
 * 本地历史被回收干净的落后副本、由空目录重建的副本，都能追平到当前已提交状态。
 */
class SnapshotSyncTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotSyncTest.class);
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
    void followerWhoseHistoryWasCollectedCatchesUpViaSnapshot() {
        cluster = new Cluster(IDS, dir, 5);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        for (int i = 1; i <= 5; i++) {
            leader.clientWrite("req-" + i, "k" + i, "v" + i);
        }

        // 隔离 n3，主副本继续写入并触发回收：n3 需要的前序条目被回收干净
        cluster.setReachable("n3", false);
        for (int i = 6; i <= 15; i++) {
            leader.clientWrite("req-" + i, "k" + i, "v" + i);
        }
        assertTrue(leader.logStore().baseIndex() >= 5,
                "主副本历史应已回收, baseIndex=" + leader.logStore().baseIndex());
        assertEquals(5, cluster.node("n3").logStore().lastIndex(), "n3 停留在旧历史");

        // 网络恢复但主副本暂时不可达：历史被回收干净的落后副本不得给出任何读取结论，
        // 无论新旧键（无法确认本地是否漏掉已确认写入）
        cluster.setReachable("n3", true);
        cluster.setReachable("n1", false);
        ReplicaNode n3Stale = cluster.node("n3");
        long commitBefore = n3Stale.commitIndex();
        Map<String, String> stateBefore = n3Stale.stateMachine();
        for (String key : List.of("k1", "k6", "k15", "missing")) {
            ReplicationException ex = assertThrows(ReplicationException.class,
                    () -> n3Stale.clientRead(key), "键 " + key + " 读取应被拒绝");
            assertEquals(ErrorCode.STALE_READ, ex.code());
        }
        assertEquals(commitBefore, n3Stale.commitIndex(), "被拒绝的读取不得推进提交点");
        assertEquals(stateBefore, n3Stale.stateMachine(), "被拒绝的读取不得改动状态机");

        // 主副本恢复并触发追平：日志补齐已不可行，必须走快照
        cluster.setReachable("n1", true);
        cluster.triggerCatchUp();

        ReplicaNode n3 = cluster.node("n3");
        log.info("n3 快照追平后 commitIndex={} baseIndex={}", n3.commitIndex(), n3.logStore().baseIndex());
        assertEquals(leader.commitIndex(), n3.commitIndex(), "追平后提交结论与主副本一致");
        assertEquals(leader.stateMachine(), n3.stateMachine(), "追平后状态机与主副本一致");
        for (int i = 1; i <= 15; i++) {
            assertEquals("v" + i, n3.clientRead("k" + i).orElseThrow(), "n3 应读到 k" + i);
        }

        // 追平后新写入照常复制到 n3
        leader.clientWrite("req-16", "k16", "v16");
        assertEquals("v16", n3.clientRead("k16").orElseThrow());
    }

    @Test
    void rebuiltFromEmptyDirCatchesUpAndServesConsistentReads() {
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "中文键", "中文值🎉");
        leader.clientWrite("req-2", "k2", "v2");

        // n3 数据目录完全丢失，由空目录重建
        ReplicaNode n3 = cluster.rebuild("n3");
        assertEquals(0, n3.commitIndex());
        assertEquals(0, n3.logStore().lastIndex());

        // 追平前：不知道主副本，读取必须拒绝而不是返回空值
        ReplicationException stale = assertThrows(ReplicationException.class,
                () -> n3.clientRead("中文键"));
        assertEquals(ErrorCode.STALE_READ, stale.code());

        cluster.triggerCatchUp();
        log.info("n3 重建追平后 commitIndex={} leaderId={}", n3.commitIndex(), n3.leaderId());
        assertEquals(leader.commitIndex(), n3.commitIndex());
        assertEquals("中文值🎉", n3.clientRead("中文键").orElseThrow());
        assertEquals("v2", n3.clientRead("k2").orElseThrow());
        assertEquals(leader.stateMachine(), n3.stateMachine());

        // 追平后参与正常复制
        leader.clientWrite("req-3", "k3", "v3");
        assertEquals("v3", n3.clientRead("k3").orElseThrow());
        assertEquals(3, n3.commitIndex());
    }

    @Test
    void catchUpWorksAlongsideOngoingWritesAndRetention() {
        cluster = new Cluster(IDS, dir, 5);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-0", "k0", "v0");

        cluster.setReachable("n3", false);
        for (int i = 1; i <= 10; i++) {
            leader.clientWrite("req-" + i, "k" + i, "v" + i);
        }
        cluster.setReachable("n3", true);

        // 追平与正常写入、历史回收交替发生
        ReplicaNode n3 = cluster.node("n3");
        // 刚恢复、尚未追平：读取必须拒绝且不改动任何状态
        long commitBefore = n3.commitIndex();
        Map<String, String> stateBefore = n3.stateMachine();
        assertEquals(ErrorCode.STALE_READ,
                assertThrows(ReplicationException.class, () -> n3.clientRead("k10")).code());
        assertEquals(commitBefore, n3.commitIndex());
        assertEquals(stateBefore, n3.stateMachine());

        cluster.triggerCatchUp();
        long afterFirst = n3.commitIndex();
        assertTrue(afterFirst > 5, "第一轮快照应推进 n3 提交点");
        for (int i = 11; i <= 20; i++) {
            leader.clientWrite("req-" + i, "k" + i, "v" + i); // 写入继续触发回收
        }
        cluster.triggerCatchUp();

        log.info("最终 leader commitIndex={} n3 commitIndex={}", leader.commitIndex(), n3.commitIndex());
        assertEquals(leader.commitIndex(), n3.commitIndex(), "追平完成后提交结论一致");
        assertEquals(leader.stateMachine(), n3.stateMachine(), "各入口读到内容一致");
        for (int i = 0; i <= 20; i++) {
            assertEquals("v" + i, n3.clientRead("k" + i).orElseThrow());
        }
    }

}
