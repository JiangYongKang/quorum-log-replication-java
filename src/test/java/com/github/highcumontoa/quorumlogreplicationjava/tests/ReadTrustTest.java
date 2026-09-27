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
 * 可信读取：状态不足的副本必须明确拒绝读取（STALE_READ），
 * 而不是把"不确定"当成"键不存在"返回空值；重新同步后读取自动恢复。
 */
class ReadTrustTest {

    private static final Logger log = LoggerFactory.getLogger(ReadTrustTest.class);
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
    void freshNodeWithoutLeaderRejectsRead() {
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        ReplicaNode n3 = cluster.rebuild("n3"); // 空目录重建：不知道任何主副本
        ReplicationException ex = assertThrows(ReplicationException.class,
                () -> n3.clientRead("k1"));
        assertEquals(ErrorCode.STALE_READ, ex.code());
        log.info("未知主副本读取被拒绝 code={} msg={}", ex.code(), ex.getMessage());

        // 被拒绝的读取不得改动任何状态
        assertEquals(0, n3.commitIndex());
        assertEquals(0, n3.logStore().lastIndex());

        // 同步后自动恢复
        cluster.triggerCatchUp();
        assertEquals("v1", n3.clientRead("k1").orElseThrow());
    }

    @Test
    void laggingFollowerRejectsReadThenRecoversAfterSync() {
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        // n3 隔离期间主副本提交了新的已确认写入
        cluster.setReachable("n3", false);
        leader.clientWrite("req-2", "k2", "v2");
        leader.clientWrite("req-3", "k3", "v3");
        cluster.setReachable("n3", true);

        ReplicaNode n3 = cluster.node("n3");
        long commitBefore = n3.commitIndex();
        Map<String, String> stateBefore = n3.stateMachine();

        // 主副本可达且明显领先：读取必须拒绝，而不是返回半旧数据
        ReplicationException ex = assertThrows(ReplicationException.class,
                () -> n3.clientRead("k1"));
        assertEquals(ErrorCode.STALE_READ, ex.code());
        log.info("落后副本读取被拒绝 code={} msg={}", ex.code(), ex.getMessage());

        // 拒绝不得顺带改动任何提交结果或副本状态
        assertEquals(commitBefore, n3.commitIndex());
        assertEquals(stateBefore, n3.stateMachine());

        // 重新同步后读取自动恢复，且与主副本一致
        cluster.triggerCatchUp();
        assertEquals(leader.commitIndex(), n3.commitIndex());
        assertEquals("v1", n3.clientRead("k1").orElseThrow());
        assertEquals("v3", n3.clientRead("k3").orElseThrow());
    }

    @Test
    void syncedFollowerReadIsConsistentWithLeader() {
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        // 已追平的跟随者：读取正常且结论与主副本一致
        for (String id : List.of("n2", "n3")) {
            ReplicaNode n = cluster.node(id);
            assertEquals(leader.commitIndex(), n.commitIndex());
            assertEquals("v1", n.clientRead("k1").orElseThrow());
        }
    }

    @Test
    void restartedNodeWithDurableCommittedDataStillReads() {
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        // 已提交数据是多数派持久化的安全结果：主副本不可用时本地读取仍然可信
        cluster.crash("n1");
        ReplicaNode n2 = cluster.node("n2");
        assertEquals("v1", n2.clientRead("k1").orElseThrow(),
                "主副本不可达时已提交数据仍可读取");

        // 重启后的节点记得主副本，本地已提交数据照常可读
        ReplicaNode n3 = cluster.restart("n3");
        assertEquals("v1", n3.clientRead("k1").orElseThrow());
        log.info("主副本不可达/节点重启后已提交数据读取正常");
    }
}
