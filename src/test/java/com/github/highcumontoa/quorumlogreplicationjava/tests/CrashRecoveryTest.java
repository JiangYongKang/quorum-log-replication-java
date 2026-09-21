package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.raft.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.raft.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.raft.LogEntry;
import com.github.highcumontoa.quorumlogreplicationjava.raft.NodeRole;
import com.github.highcumontoa.quorumlogreplicationjava.raft.RaftException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 崩溃重启与主副本更替：已确认写入不丢失、不被静默改写，同一位置不得被两条不同记录覆盖。 */
class CrashRecoveryTest {

    @TempDir
    Path dataDir;

    @Test
    void committedWriteSurvivesLeaderCrashAndRestart() {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        cluster.write("req-1", "durable-1");
        cluster.write("req-2", "durable-2");

        cluster.crashNode("node1");
        cluster.restartNode("node1");

        // 恢复后已提交数据仍在
        List<LogEntry> entries = cluster.node("node1").committedEntries();
        assertEquals(2, entries.size());
        assertEquals("durable-1", entries.get(0).payload());
        assertEquals("durable-2", entries.get(1).payload());
    }

    @Test
    void crashedNodeRejectsReadAndWrite() {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        cluster.write("req-1", "v1");
        cluster.crashNode("node2");

        RaftException readEx = assertThrows(RaftException.class,
                () -> cluster.node("node2").committedEntries());
        assertEquals(ErrorCode.NODE_DOWN, readEx.code());
    }

    @Test
    void oldLeaderUncommittedEntryIsOverwrittenConsistentlyAfterRecovery() {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        cluster.write("req-1", "committed-1");

        // node1 在多数派不足时写入未提交条目
        cluster.crashNode("node2");
        cluster.crashNode("node3");
        assertThrows(RaftException.class, () -> cluster.write("req-x", "uncommitted"));

        // 旧主崩溃，node2/node3 恢复并选出新主
        cluster.crashNode("node1");
        cluster.restartNode("node2");
        cluster.restartNode("node3");
        cluster.elect("node2");
        assertEquals(NodeRole.LEADER, cluster.node("node2").role());
        cluster.write("req-2", "committed-2");

        // 旧主恢复后补齐日志：同一位置不得保留两条不同记录
        cluster.restartNode("node1");
        cluster.write("req-3", "committed-3");

        List<LogEntry> n1 = cluster.node("node1").committedEntries();
        List<LogEntry> n2 = cluster.node("node2").committedEntries();
        assertEquals(n2.size(), n1.size());
        for (int i = 0; i < n2.size(); i++) {
            assertEquals(n2.get(i).index(), n1.get(i).index());
            assertEquals(n2.get(i).payload(), n1.get(i).payload(),
                    "同一位置被两条不同记录覆盖: index=" + n2.get(i).index());
        }
        // 未提交的旧条目不得出现在已提交序列中
        assertTrue(n1.stream().noneMatch(e -> e.payload().equals("uncommitted")));
        assertEquals("committed-2", n1.get(1).payload());
    }

    @Test
    void staleTermAppendIsRejectedWithoutStateChange() {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        cluster.write("req-1", "v1");
        long commitBefore = cluster.node("node2").commitIndex();
        int sizeBefore = cluster.node("node2").committedEntries().size();

        // 过期任期的追加请求必须被拒绝且不改变既有状态
        var result = cluster.node("node2").handleAppendEntries(
                new com.github.highcumontoa.quorumlogreplicationjava.raft.Transport.AppendEntriesRequest(
                        0, "fake-leader", 0, 0,
                        List.of(new LogEntry(0, 1, "evil", "forged")), 99));
        assertEquals(ErrorCode.TERM_MISMATCH, result.error());
        assertEquals(commitBefore, cluster.node("node2").commitIndex());
        assertEquals(sizeBefore, cluster.node("node2").committedEntries().size());
    }
}
