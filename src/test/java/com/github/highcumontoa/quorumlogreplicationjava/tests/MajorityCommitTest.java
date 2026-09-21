package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.raft.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.raft.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.raft.LogEntry;
import com.github.highcumontoa.quorumlogreplicationjava.raft.RaftException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 多数派提交：不足多数派时条目不得对外可见，恢复后可继续提交。 */
class MajorityCommitTest {

    @TempDir
    Path dataDir;

    @Test
    void writeCommitsWithMajorityAndIsVisible() {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        LogEntry entry = cluster.write("req-1", "value-1");
        assertEquals(1, entry.index());

        List<LogEntry> visible = cluster.readCommitted();
        assertEquals(1, visible.size());
        assertEquals("value-1", visible.get(0).payload());
    }

    @Test
    void writeWithoutMajorityIsNotCommittedNorVisible() {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        cluster.crashNode("node2");
        cluster.crashNode("node3");

        RaftException ex = assertThrows(RaftException.class, () -> cluster.write("req-2", "value-2"));
        assertEquals(ErrorCode.UNAVAILABLE, ex.code());

        // 未达多数派的条目不得对外可见
        assertTrue(cluster.readCommitted().isEmpty());
        assertEquals(0, cluster.leader().commitIndex());
    }

    @Test
    void pendingEntryCommitsAfterReplicasRecover() {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        cluster.crashNode("node2");
        cluster.crashNode("node3");
        assertThrows(RaftException.class, () -> cluster.write("req-3", "deferred"));

        cluster.restartNode("node2");
        cluster.restartNode("node3");
        // 后续写入触发复制补齐，此前的积压条目一并提交
        cluster.write("req-4", "after-recovery");

        List<LogEntry> visible = cluster.readCommitted();
        assertEquals(2, visible.size());
        assertEquals("deferred", visible.get(0).payload());
        assertEquals("after-recovery", visible.get(1).payload());
        // 落后副本补齐后同样只能读到已提交前缀
        assertEquals(2, cluster.node("node2").committedEntries().size());
    }
}
