package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.raft.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.raft.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.raft.LogEntry;
import com.github.highcumontoa.quorumlogreplicationjava.raft.RaftConfig;
import com.github.highcumontoa.quorumlogreplicationjava.raft.RaftException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 日志回收与资源限额：超限明确拒绝，回收不影响已提交数据的可读性。 */
class LogCompactionTest {

    @TempDir
    Path dataDir;

    private Cluster newCluster(RaftConfig config) {
        Cluster cluster = new Cluster(config);
        cluster.start();
        return cluster;
    }

    @Test
    void committedPrefixIsCompactedAndArchived() throws Exception {
        RaftConfig config = ClusterTestSupport.config(dataDir);
        config.setMaxLogEntries(10);
        config.setCompactionRetain(3);
        Cluster cluster = newCluster(config);

        for (int i = 1; i <= 20; i++) {
            cluster.write("req-" + i, "value-" + i);
        }

        // 活跃日志受限流策略约束，不超上限
        List<LogEntry> active = cluster.readCommitted();
        assertTrue(active.size() <= config.getMaxLogEntries(),
                "活跃日志超限: " + active.size());
        // 保留的已提交尾部仍可读
        assertEquals("value-20", active.get(active.size() - 1).payload());
        assertEquals(20, cluster.leader().commitIndex());

        // 被回收的已提交前缀进入归档，不丢失
        Path archive = dataDir.resolve("node1/archive.log");
        assertTrue(Files.exists(archive), "归档文件不存在");
        List<String> archivedLines = Files.readAllLines(archive);
        assertEquals(20 - active.size(), archivedLines.size());

        // 重启后状态一致：已提交尾部仍可读
        cluster.restartNode("node1");
        cluster.elect("node1");
        assertEquals(active.size(), cluster.readCommitted().size());
    }

    @Test
    void uncommittedBacklogExceedingLimitIsRejected() {
        RaftConfig config = ClusterTestSupport.config(dataDir);
        config.setMaxUncommittedEntries(3);
        Cluster cluster = newCluster(config);
        cluster.crashNode("node2");
        cluster.crashNode("node3");

        // 多数派不足，写入全部积压不提交
        for (int i = 1; i <= 3; i++) {
            int n = i;
            RaftException ex = assertThrows(RaftException.class, () -> cluster.write("req-" + n, "v" + n));
            assertEquals(ErrorCode.UNAVAILABLE, ex.code());
        }
        // 积压超限后明确拒绝，错误类型可区分
        RaftException ex = assertThrows(RaftException.class, () -> cluster.write("req-4", "v4"));
        assertEquals(ErrorCode.RESOURCE_EXHAUSTED, ex.code());
    }

    @Test
    void oversizedPayloadIsRejected() {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        String big = "x".repeat(5000);
        RaftException ex = assertThrows(RaftException.class, () -> cluster.write("req-big", big));
        assertEquals(ErrorCode.INVALID_ARGUMENT, ex.code());
        assertTrue(cluster.readCommitted().isEmpty(), "非法写入不得改变状态");
    }
}
