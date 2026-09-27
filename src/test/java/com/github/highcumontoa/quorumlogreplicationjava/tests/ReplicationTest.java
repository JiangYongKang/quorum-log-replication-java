package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.cluster.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.cluster.ReplicaNode;
import com.github.highcumontoa.quorumlogreplicationjava.cluster.Role;
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
 * 日志复制与多数派提交的核心行为测试。
 */
class ReplicationTest {

    private static final Logger log = LoggerFactory.getLogger(ReplicationTest.class);
    private static final List<String> IDS = List.of("n1", "n2", "n3");

    @TempDir
    Path dir;

    private Cluster cluster;

    private Cluster newCluster(long maxLogEntries) {
        cluster = new Cluster(IDS, dir, maxLogEntries);
        return cluster;
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void majorityCommitMakesWriteVisibleEverywhere() {
        Cluster c = newCluster(1000);
        ReplicaNode leader = c.node("n1");
        leader.becomeLeader();

        LogEntry e = leader.clientWrite("req-1", "k1", "v1");
        log.info("写入确认 term={} index={} commitIndex={}", e.term(), e.index(), leader.commitIndex());
        assertEquals(1, e.index());
        assertEquals(1, leader.commitIndex());

        for (String id : IDS) {
            ReplicaNode n = c.node(id);
            assertEquals(1, n.commitIndex(), id + " 提交点应推进");
            assertEquals("v1", n.clientRead("k1").orElseThrow(), id + " 应读到已提交值");
        }
    }

    @Test
    void writeFailsWithoutMajorityAndLeavesNoVisibleState() {
        Cluster c = newCluster(1000);
        ReplicaNode leader = c.node("n1");
        leader.becomeLeader();
        c.crash("n2");
        c.crash("n3");

        ReplicationException ex = assertThrows(ReplicationException.class,
                () -> leader.clientWrite("req-2", "k2", "v2"));
        assertEquals(ErrorCode.NO_MAJORITY, ex.code());
        log.info("多数派不足被拒绝 code={} commitIndex={}", ex.code(), leader.commitIndex());
        assertEquals(0, leader.commitIndex(), "未达多数派不得提交");
        assertTrue(leader.clientRead("k2").isEmpty(), "未提交数据不得可见");

        // 恢复后旧主日志中未提交条目不得被静默提交
        c.restart("n2");
        c.restart("n3");
        assertEquals(0, c.node("n2").commitIndex());
        // n2 从未与主副本建立过联系（leaderId 未知），读取必须被明确拒绝而非返回空值
        ReplicationException stale = assertThrows(ReplicationException.class,
                () -> c.node("n2").clientRead("k2"));
        assertEquals(ErrorCode.STALE_READ, stale.code());
    }

    @Test
    void committedWriteSurvivesCrashAndRestart() {
        Cluster c = newCluster(1000);
        ReplicaNode leader = c.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");
        leader.clientWrite("req-2", "k2", "v2");

        c.crash("n1");
        c.crash("n2");
        ReplicaNode r1 = c.restart("n1");
        ReplicaNode r2 = c.restart("n2");
        log.info("重启后 n1 term={} commitIndex={} n2 commitIndex={}",
                r1.currentTerm(), r1.commitIndex(), r2.commitIndex());

        assertEquals(2, r1.commitIndex(), "已提交数据重启后不得丢失");
        assertEquals(2, r2.commitIndex());
        assertEquals("v1", r1.clientRead("k1").orElseThrow());
        assertEquals("v2", r2.clientRead("k2").orElseThrow());
    }

    @Test
    void laggingFollowerCatchesUpWithoutReadingUncommitted() {
        Cluster c = newCluster(1000);
        ReplicaNode leader = c.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        c.crash("n3");
        leader.clientWrite("req-2", "k2", "v2"); // n2+n1 多数派
        ReplicaNode n3 = c.restart("n3");
        log.info("n3 恢复时 commitIndex={} lastIndex={}", n3.commitIndex(), n3.logStore().lastIndex());
        // 补齐期间 n3 明显落后于已对外确认的写入，读取必须被拒绝而不是返回空值
        ReplicationException stale = assertThrows(ReplicationException.class, () -> n3.clientRead("k2"));
        assertEquals(ErrorCode.STALE_READ, stale.code());

        leader.clientWrite("req-3", "k3", "v3"); // 触发对 n3 的补齐
        assertEquals(3, n3.commitIndex());
        assertEquals("v2", n3.clientRead("k2").orElseThrow(), "补齐后应可读");
        assertEquals("v3", n3.clientRead("k3").orElseThrow());
    }

    @Test
    void leaderFailoverRejectsStaleLeaderAndConvergesLogs() {
        Cluster c = newCluster(1000);
        ReplicaNode oldLeader = c.node("n1");
        oldLeader.becomeLeader();
        oldLeader.clientWrite("req-1", "k1", "v1");

        // 隔离旧主，旧主任期内写入无法达到多数派
        c.setReachable("n1", true);
        c.crash("n2");
        c.crash("n3");
        assertThrows(ReplicationException.class, () -> oldLeader.clientWrite("req-x", "kx", "vx"));

        // 新主在 n2 上任，任期更高
        ReplicaNode n2 = c.restart("n2");
        ReplicaNode n3 = c.restart("n3");
        n2.becomeLeader();
        n2.becomeLeader(); // term=3，确保高于旧主
        n2.clientWrite("req-2", "k2", "v2");

        // 旧主恢复后收到更高任期的复制请求，必须让位并截断冲突条目
        c.setReachable("n1", true);
        n2.clientWrite("req-3", "k3", "v3");
        ReplicaNode n1 = c.node("n1");
        log.info("旧主恢复 role={} term={} commitIndex={}", n1.role(), n1.currentTerm(), n1.commitIndex());
        assertEquals(Role.FOLLOWER, n1.role(), "任期不匹配的副本不得继续作为主副本");
        assertEquals(n2.currentTerm(), n1.currentTerm());
        assertEquals(3, n1.commitIndex());

        // 同一位置不得被两条不同记录覆盖：各节点日志逐条一致
        for (long i = 1; i <= 3; i++) {
            LogEntry a = n1.logStore().get(i).orElseThrow();
            LogEntry b = n2.logStore().get(i).orElseThrow();
            assertEquals(a, b, "index=" + i + " 处记录必须一致");
        }
        assertEquals("v3", n1.clientRead("k3").orElseThrow());
    }

    @Test
    void duplicateRequestIdIsIdempotent() {
        Cluster c = newCluster(1000);
        ReplicaNode leader = c.node("n1");
        leader.becomeLeader();

        LogEntry first = leader.clientWrite("req-dup", "k", "v1");
        LogEntry second = leader.clientWrite("req-dup", "k", "v1");
        log.info("幂等命中 first.index={} second.index={}", first.index(), second.index());
        assertEquals(first.index(), second.index(), "重复请求不得重复追加");
        assertEquals(1, leader.logStore().size(), "日志中只能有一条记录");
        assertEquals(1, leader.commitIndex(), "重复请求不得重复计账");
    }

    @Test
    void retentionCollectsCommittedPrefixButKeepsDataReadable() {
        Cluster c = newCluster(5);
        ReplicaNode leader = c.node("n1");
        leader.becomeLeader();
        for (int i = 1; i <= 10; i++) {
            leader.clientWrite("req-" + i, "k" + i, "v" + i);
        }
        log.info("回收后 baseIndex={} lastIndex={} commitIndex={}",
                leader.logStore().baseIndex(), leader.logStore().lastIndex(), leader.commitIndex());
        assertTrue(leader.logStore().size() <= 5, "日志规模应受限");
        assertTrue(leader.logStore().baseIndex() > 0, "已提交前缀应被回收");
        assertEquals(10, leader.commitIndex());
        // 已回收前缀中的已提交数据仍可从状态机读取
        assertEquals("v1", leader.clientRead("k1").orElseThrow());
        assertEquals("v10", leader.clientRead("k10").orElseThrow());
        // 重启后回收状态与数据仍然一致
        c.crash("n1");
        ReplicaNode r = c.restart("n1");
        assertEquals(10, r.commitIndex());
        assertEquals("v1", r.clientRead("k1").orElseThrow());
    }

    @Test
    void boundaryValidationRejectsWithoutStateChange() {
        Cluster c = newCluster(1000);
        ReplicaNode leader = c.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");

        ReplicaNode follower = c.node("n2");
        ReplicationException notLeader = assertThrows(ReplicationException.class,
                () -> follower.clientWrite("req-f", "k", "v"));
        assertEquals(ErrorCode.NOT_LEADER, notLeader.code());
        log.info("非主副本写入被拒绝 code={} followerLastIndex={}",
                notLeader.code(), follower.logStore().lastIndex());
        assertEquals(1, follower.logStore().lastIndex(), "拒绝不得改变既有状态");

        ReplicationException invalid = assertThrows(ReplicationException.class,
                () -> leader.clientWrite("", "k", "v"));
        assertEquals(ErrorCode.INVALID_ARGUMENT, invalid.code());
        assertEquals(1, leader.logStore().lastIndex());

        ReplicationException badRead = assertThrows(ReplicationException.class,
                () -> leader.clientRead(" "));
        assertEquals(ErrorCode.INVALID_ARGUMENT, badRead.code());
    }

    @Test
    void corruptedLogIsReportedAsDistinctError() {
        Cluster c = newCluster(1000);
        ReplicaNode leader = c.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");
        c.close();

        // 破坏日志记录载荷
        Path logFile = dir.resolve("n2").resolve("log.dat");
        try (var raf = new java.io.RandomAccessFile(logFile.toFile(), "rw")) {
            raf.seek(raf.length() - 1);
            int b = raf.read();
            raf.seek(raf.length() - 1);
            raf.write(b ^ 0xFF);
        } catch (Exception e) {
            fail(e);
        }
        ReplicationException ex = assertThrows(ReplicationException.class, () -> c.restart("n2"));
        assertEquals(ErrorCode.LOG_CORRUPTED, ex.code());
        log.info("日志损坏被识别 code={} msg={}", ex.code(), ex.getMessage());
    }
}
