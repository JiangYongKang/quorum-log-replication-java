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
    void leaderUnreachableAndFollowerBehindRejectsBothNewAndOldKeys() {
        // 用户稳定复现的问题链路：副本离线期间漏掉已对外确认的写入，
        // 之后主副本暂时联系不上。此时无论读新键还是旧键，副本都无法证明
        // 自己已追平到最新已提交状态，必须明确报 STALE_READ：
        // 新键不能回答"不存在"，旧键也不能在状态不足时照常返回旧值。
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        // n3 离线期间主副本继续提交已确认写入
        cluster.setReachable("n3", false);
        leader.clientWrite("req-2", "k2", "v2");
        leader.clientWrite("req-3", "k3", "v3");
        // n3 恢复网络但尚未追平，随后主副本暂时联系不上（崩溃）
        cluster.setReachable("n3", true);
        cluster.crash("n1");

        ReplicaNode n3 = cluster.node("n3");
        long commitBefore = n3.commitIndex();
        Map<String, String> stateBefore = n3.stateMachine();
        log.info("主副本不可达且 n3 落后: n3.commitIndex={} stateKeys={}",
                commitBefore, stateBefore.keySet());

        // 漏掉的已提交新键：不得返回"键不存在"
        ReplicationException missing = assertThrows(ReplicationException.class,
                () -> n3.clientRead("k2"));
        assertEquals(ErrorCode.STALE_READ, missing.code());
        ReplicationException newest = assertThrows(ReplicationException.class,
                () -> n3.clientRead("k3"));
        assertEquals(ErrorCode.STALE_READ, newest.code());
        // 本地已有的旧键：状态不足同样必须拒绝，而不是照常返回旧值
        ReplicationException oldKey = assertThrows(ReplicationException.class,
                () -> n3.clientRead("k1"));
        assertEquals(ErrorCode.STALE_READ, oldKey.code());
        // 与"参数错误"可区分
        ReplicationException invalid = assertThrows(ReplicationException.class,
                () -> n3.clientRead(" "));
        assertEquals(ErrorCode.INVALID_ARGUMENT, invalid.code());
        assertNotEquals(invalid.code(), missing.code());
        log.info("主副本不可达且本地落后：新旧键读取均被明确拒绝 code={}", missing.code());

        // 被拒绝的读取不得改动任何已提交结果或副本状态
        assertEquals(commitBefore, n3.commitIndex());
        assertEquals(stateBefore, n3.stateMachine());
        // 多次拒绝同样无副作用，也不会把"暂时无法确认"缓存成空值
        assertThrows(ReplicationException.class, () -> n3.clientRead("k2"));
        assertThrows(ReplicationException.class, () -> n3.clientRead("k2"));
        assertEquals(commitBefore, n3.commitIndex());
        assertEquals(stateBefore, n3.stateMachine());
    }

    @Test
    void readsRecoverAutomaticallyAfterLeaderBackAndResync() {
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        cluster.setReachable("n3", false);
        leader.clientWrite("req-2", "k2", "v2");
        leader.clientWrite("req-3", "k3", "v3");
        cluster.setReachable("n3", true);
        cluster.crash("n1");

        ReplicaNode n3 = cluster.node("n3");
        ReplicationException whileDown = assertThrows(ReplicationException.class,
                () -> n3.clientRead("k2"));
        assertEquals(ErrorCode.STALE_READ, whileDown.code());

        // 主副本恢复（从磁盘重启），完成一次重新同步后读取必须自动恢复，
        // 不能一直拒绝，也不能把之前的"无法确认"当成空值
        ReplicaNode recoveredLeader = cluster.restart("n1");
        recoveredLeader.becomeLeader();
        cluster.triggerCatchUp();

        assertEquals(recoveredLeader.commitIndex(), n3.commitIndex());
        assertEquals("v1", n3.clientRead("k1").orElseThrow(), "旧键恢复后内容一致");
        assertEquals("v2", n3.clientRead("k2").orElseThrow(), "漏掉的新键恢复后可读");
        assertEquals("v3", n3.clientRead("k3").orElseThrow());
        // "键确实不存在" 与 STALE_READ 区分开：追平后不存在的键返回空（ok=false）而非报错
        assertTrue(n3.clientRead("never-existed").isEmpty());
        assertEquals(recoveredLeader.stateMachine(), n3.stateMachine());
        log.info("主副本恢复并重新同步后读取自动恢复，内容与提交结论与主副本一致");
    }

    @Test
    void caughtUpFollowerStillRejectsWhileLeaderTemporarilyUnreachable() {
        // 已追平到最新提交状态的跟随者：主副本可达时读取正常；
        // 一旦主副本暂时联系不上，它无法证明自己仍处于最新水位，同样拒绝，
        // 主副本恢复后立即自动恢复（无需重新拷贝任何数据）。
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        ReplicaNode n2 = cluster.node("n2");
        assertEquals("v1", n2.clientRead("k1").orElseThrow());

        cluster.crash("n1");
        ReplicationException ex = assertThrows(ReplicationException.class,
                () -> n2.clientRead("k1"));
        assertEquals(ErrorCode.STALE_READ, ex.code());
        log.info("已追平跟随者在主副本失联期间拒绝读取 code={}", ex.code());

        ReplicaNode recovered = cluster.restart("n1");
        recovered.becomeLeader();
        // 主副本恢复即可信（本地提交点本来就与之一致），无需追平也应立即恢复
        assertEquals("v1", n2.clientRead("k1").orElseThrow());
    }

    @Test
    void networkPartitionCycleKeepsReadSemanticsAndAutoRecovers() {
        // 网络隔离（非崩溃，内存状态保留）链路：隔离→拒绝读取→恢复隔离但未追平→
        // 主副本再次失联仍拒绝→网络恢复+追平后自动恢复，且无副作用
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        ReplicaNode n3 = cluster.node("n3");
        long syncedCommit = n3.commitIndex();
        assertEquals("v1", n3.clientRead("k1").orElseThrow());

        cluster.setReachable("n3", false);
        leader.clientWrite("req-2", "k2", "v2");
        // 隔离期间联系不上主副本，且本地落后：拒绝
        ReplicationException isolated = assertThrows(ReplicationException.class,
                () -> n3.clientRead("k2"));
        assertEquals(ErrorCode.STALE_READ, isolated.code());
        assertEquals(syncedCommit, n3.commitIndex(), "拒绝读取不改提交点");

        // 网络恢复但尚未复制（主副本立刻又被隔离）：读校验能联系上主副本时
        // 也必须因半旧数据拒绝
        cluster.setReachable("n3", true);
        ReplicationException behind = assertThrows(ReplicationException.class,
                () -> n3.clientRead("k2"));
        assertEquals(ErrorCode.STALE_READ, behind.code());
        assertEquals(syncedCommit, n3.commitIndex());

        // 追平后恢复，且期间任何拒绝都没有把"无法确认"缓存成空值
        cluster.triggerCatchUp();
        assertEquals(leader.commitIndex(), n3.commitIndex());
        assertEquals("v2", n3.clientRead("k2").orElseThrow());
        assertTrue(n3.clientRead("k2-no-such").isEmpty(), "追平后真正不存在的键才返回空");
        log.info("网络隔离往返后读取语义保持，追平后自动恢复");
    }

    @Test
    void rebuiltAfterHistoryCollectedRejectsUntilSnapshotCaughtUp() {
        // 历史被回收后重新加入（空目录重建 + 主副本侧已回收）：
        // 追平走快照，追平前拒绝，追平后新旧键与主副本完全一致
        cluster = new Cluster(IDS, dir, 5);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        for (int i = 1; i <= 12; i++) {
            leader.clientWrite("req-" + i, "k" + i, "v" + i);
        }
        assertTrue(leader.logStore().baseIndex() > 0);

        ReplicaNode n3 = cluster.rebuild("n3");
        // 重建后无主副本信息：新键、旧键一律拒绝，绝不返回"不存在"
        assertEquals(ErrorCode.STALE_READ, assertThrows(ReplicationException.class,
                () -> n3.clientRead("k12")).code());
        assertEquals(ErrorCode.STALE_READ, assertThrows(ReplicationException.class,
                () -> n3.clientRead("k1")).code());
        assertEquals(0, n3.commitIndex());
        assertEquals(0, n3.logStore().lastIndex());

        cluster.triggerCatchUp(); // 快照追平
        assertEquals(leader.commitIndex(), n3.commitIndex());
        assertEquals(leader.stateMachine(), n3.stateMachine());
        for (int i = 1; i <= 12; i++) {
            String expected = "v" + i;
            assertEquals(expected, n3.clientRead("k" + i).orElseThrow());
        }
        log.info("历史回收后重新加入的副本经快照追平，读取恢复且与主副本一致");
    }

    @Test
    void rejectedReadHasNoSideEffectsAcrossCrashRestart() {
        // 崩溃重启链路下拒绝无副作用：重启的落后节点在主副本失联时拒绝读取，
        // 拒绝不改持久化提交点/日志；恢复追平后内容一致
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        cluster.crash("n3");
        leader.clientWrite("req-2", "k2", "v2"); // n1+n2 多数派
        ReplicaNode n3 = cluster.restart("n3");
        long commitAfterRestart = n3.commitIndex();
        long lastAfterRestart = n3.logStore().lastIndex();
        cluster.crash("n1"); // 主副本暂时联系不上

        for (String key : List.of("k2", "k1", "k2")) {
            ReplicationException ex = assertThrows(ReplicationException.class,
                    () -> n3.clientRead(key));
            assertEquals(ErrorCode.STALE_READ, ex.code());
        }
        assertEquals(commitAfterRestart, n3.commitIndex(), "拒绝不得推进持久化提交点");
        assertEquals(lastAfterRestart, n3.logStore().lastIndex(), "拒绝不得改动日志");

        ReplicaNode recovered = cluster.restart("n1");
        recovered.becomeLeader();
        cluster.triggerCatchUp();
        assertEquals(recovered.commitIndex(), n3.commitIndex());
        assertEquals("v1", n3.clientRead("k1").orElseThrow());
        assertEquals("v2", n3.clientRead("k2").orElseThrow());
        log.info("崩溃重启链路下拒绝读取零副作用，恢复追平后结论一致");
    }
}
