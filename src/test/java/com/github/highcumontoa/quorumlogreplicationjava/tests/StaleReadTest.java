package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.cluster.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.cluster.ReplicaNode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ReplicationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 读取可信性：状态不足（不知主、主不可达、落后于已确认写入）时读取被明确拒绝，
 * 重新同步后自动恢复；各类被拒绝的操作都不改动既有提交结果与副本状态。
 */
class StaleReadTest {

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
    void followerWithNoKnownLeaderRejectsReadWithStaleRead() {
        cluster = new Cluster(IDS, dir, 1000, 1024 * 1024);
        ReplicaNode n3 = cluster.node("n3"); // 从未接触过主副本
        assertNull(n3.leaderId());
        ReplicationException ex = assertThrows(ReplicationException.class,
                () -> n3.clientRead("anything"));
        assertEquals(ErrorCode.STALE_READ, ex.code());
        assertEquals(0, n3.commitIndex(), "被拒绝的读取不得改变状态");
    }

    @Test
    void staleReadIsDistinctFromKeyAbsenceOnLeader() {
        cluster = new Cluster(IDS, dir, 1000, 1024 * 1024);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "exists", "v");
        // 主副本状态充分：键不存在就是不存在（空 Optional），不能报 STALE_READ
        assertTrue(leader.clientRead("missing").isEmpty());
        assertEquals("v", leader.clientRead("exists").orElseThrow());
    }

    @Test
    void followerBehindLeaderCommitIsRejectedThenRecovers() {
        cluster = new Cluster(IDS, dir, 1000, 1024 * 1024);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");
        ReplicaNode n3 = cluster.node("n3");
        assertEquals("v1", n3.clientRead("k1").orElseThrow());

        // 隔离 n3，主继续确认写入：n3 明显落后
        cluster.setReachable("n3", false);
        leader.clientWrite("req-2", "k2", "v2");
        leader.clientWrite("req-3", "k3", "v3");
        // n3 连不上主：read-index 无法确认 -> STALE_READ，而不是假装 k2/k3 不存在
        ReplicationException ex = assertThrows(ReplicationException.class,
                () -> n3.clientRead("k2"));
        assertEquals(ErrorCode.STALE_READ, ex.code());
        // 已同步过的旧键也不能在不可信状态下返回
        assertEquals(ErrorCode.STALE_READ, assertThrows(ReplicationException.class,
                () -> n3.clientRead("k1")).code());

        // 恢复后一次写入触发追平，读取自动恢复
        cluster.setReachable("n3", true);
        leader.clientWrite("req-4", "k4", "v4");
        assertEquals(leader.commitIndex(), n3.commitIndex());
        for (int i = 1; i <= 4; i++) {
            assertEquals("v" + i, n3.clientRead("k" + i).orElseThrow());
        }
    }

    @Test
    void rejectsDoNotMutateCommittedState() {
        cluster = new Cluster(IDS, dir, 1000, 64);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        // 超限写入被拒绝：无任何状态变化
        assertThrows(ReplicationException.class,
                () -> leader.clientWrite("req-big", "k", "超".repeat(100)));
        // 非主写入被拒绝：无任何状态变化
        assertEquals(ErrorCode.NOT_LEADER, assertThrows(ReplicationException.class,
                () -> cluster.node("n2").clientWrite("req-x", "kx", "vx")).code());
        // 参数非法被拒绝
        assertEquals(ErrorCode.INVALID_ARGUMENT, assertThrows(ReplicationException.class,
                () -> leader.clientWrite("  ", "k", "v")).code());

        // 经历各类拒绝后，各副本结论与拒绝前完全一致
        for (String id : IDS) {
            ReplicaNode n = cluster.node(id);
            assertEquals(1, n.commitIndex(), id + " 提交点不应被拒绝操作改动");
            assertEquals("v1", n.clientRead("k1").orElseThrow());
            assertEquals(1, n.logStore().lastIndex(), id + " 拒绝操作不得追加日志");
        }
        // 后续正常写入照常提交
        leader.clientWrite("req-2", "k2", "v2");
        assertEquals(2, leader.commitIndex());
    }

    @Test
    void followerOfDeposedLeaderIsRefreshedAfterFailover() {
        cluster = new Cluster(IDS, dir, 1000, 1024 * 1024);
        ReplicaNode n1 = cluster.node("n1");
        n1.becomeLeader();
        n1.clientWrite("req-1", "k1", "v1");

        // n1 被隔离；n2 上任更高任期
        cluster.setReachable("n1", false);
        ReplicaNode n2 = cluster.node("n2");
        n2.becomeLeader();
        n2.becomeLeader(); // term=3
        n2.clientWrite("req-2", "k2", "v2");
        ReplicaNode n3 = cluster.node("n3");
        // n3 已跟随新主：读取到的是新主的已提交结论
        assertEquals("v2", n3.clientRead("k2").orElseThrow());
        assertEquals(3, n3.currentTerm());

        // n1 恢复，被更高任期拉回跟随者，随后读取恢复
        cluster.setReachable("n1", true);
        n2.clientWrite("req-3", "k3", "v3");
        assertEquals(3, n1.currentTerm());
        assertEquals("v3", n1.clientRead("k3").orElseThrow());
        assertEquals("v1", n1.clientRead("k1").orElseThrow());
    }
}
