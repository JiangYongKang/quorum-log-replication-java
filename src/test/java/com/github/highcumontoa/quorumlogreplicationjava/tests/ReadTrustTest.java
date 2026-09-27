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
    void leaderUnreachableAndLocalLaggingMustRejectBothNewAndOldKeys() {
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        // n3 离线/重连期间漏掉了之后已经对外确认的写入（k2/k3）
        cluster.setReachable("n3", false);
        leader.clientWrite("req-2", "k2", "v2");
        leader.clientWrite("req-3", "k3", "v3");
        // 网络恢复让副本带着旧日志重新上线，但主副本此刻又暂时联系不上
        cluster.setReachable("n3", true);
        cluster.setReachable("n1", false);

        ReplicaNode n3 = cluster.node("n3");
        long commitBefore = n3.commitIndex();
        Map<String, String> stateBefore = n3.stateMachine();

        // 新键绝不能被报成“不存在”，旧键也不能给出可能过时的值：一律明确拒绝
        for (String key : List.of("k2", "k3", "k1", "never-written")) {
            ReplicationException ex = assertThrows(ReplicationException.class,
                    () -> n3.clientRead(key), "读取键 " + key + " 应被拒绝");
            assertEquals(ErrorCode.STALE_READ, ex.code(), "键 " + key + " 的拒绝原因");
            assertNotEquals(ErrorCode.INVALID_ARGUMENT, ex.code());
        }
        log.info("主副本不可达且本地落后：新旧键读取全部以 STALE_READ 拒绝");

        // 被拒绝的读取不得改动任何已提交结果或副本状态
        assertEquals(commitBefore, n3.commitIndex());
        assertEquals(stateBefore, n3.stateMachine());
        assertEquals(1, n3.commitIndex(), "n3 仍停留在漏掉写入前的提交点");

        // 主副本恢复并完成一次重新同步后，读取自动恢复，结论与主副本一致
        cluster.setReachable("n1", true);
        cluster.triggerCatchUp();
        assertEquals(leader.commitIndex(), n3.commitIndex());
        assertEquals("v1", n3.clientRead("k1").orElseThrow());
        assertEquals("v2", n3.clientRead("k2").orElseThrow());
        assertEquals("v3", n3.clientRead("k3").orElseThrow());
        assertTrue(n3.clientRead("never-written").isEmpty(),
                "追平后“键不存在”才是可信结论");
    }

    @Test
    void crashedLeaderAndRestartedFollowerRejectReadUntilTrustRestored() {
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        // 崩溃重启链路：主副本崩溃（不可达），跟随者随后重启（仍记得旧主）
        cluster.crash("n1");
        ReplicaNode n3 = cluster.restart("n3");
        long commitBefore = n3.commitIndex();
        Map<String, String> stateBefore = n3.stateMachine();

        // 联系不上当前主副本，即使本地持有已提交数据也不能给出结论
        ReplicationException ex = assertThrows(ReplicationException.class,
                () -> n3.clientRead("k1"));
        assertEquals(ErrorCode.STALE_READ, ex.code());
        log.info("主副本崩溃期间读取被拒绝 code={} msg={}", ex.code(), ex.getMessage());
        assertEquals(commitBefore, n3.commitIndex(), "拒绝不得改动提交点");
        assertEquals(stateBefore, n3.stateMachine(), "拒绝不得改动状态机");

        // 旧主重启后是跟随者角色，同样不能凭“本地有数据”直接服务读取
        ReplicaNode n1 = cluster.restart("n1");
        ReplicationException staleSelf = assertThrows(ReplicationException.class,
                () -> n1.clientRead("k1"));
        assertEquals(ErrorCode.STALE_READ, staleSelf.code());

        // 重新当选并完成一轮复制后，各入口读取恢复且与主副本一致
        n1.becomeLeader();
        cluster.triggerCatchUp();
        assertEquals("v1", n1.clientRead("k1").orElseThrow());
        assertEquals("v1", n3.clientRead("k1").orElseThrow());
        assertTrue(n1.clientRead("k2").isEmpty(), "主副本自身可权威判定键不存在");
    }
}
