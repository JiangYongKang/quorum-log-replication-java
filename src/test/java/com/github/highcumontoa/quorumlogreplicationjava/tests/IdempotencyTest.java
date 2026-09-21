package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.raft.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.raft.LogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 幂等与并发：重复请求标识不得重复追加，并发写入不得出现索引跳变或错位。 */
class IdempotencyTest {

    @TempDir
    Path dataDir;

    @Test
    void duplicateRequestIdAppendsOnlyOnce() {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        LogEntry first = cluster.write("req-dup", "payload");
        LogEntry second = cluster.write("req-dup", "payload");
        LogEntry third = cluster.write("req-dup", "payload");

        assertEquals(first.index(), second.index());
        assertEquals(first.index(), third.index());
        assertEquals(1, cluster.readCommitted().size());
        // 各副本同样只追加一次
        assertEquals(1, cluster.node("node2").committedEntries().size());
        assertEquals(1, cluster.node("node3").committedEntries().size());
    }

    @Test
    void duplicateRequestIdSurvivesRestart() {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        LogEntry first = cluster.write("req-persist", "p1");
        cluster.crashNode("node1");
        cluster.restartNode("node1");
        cluster.elect("node1");

        LogEntry again = cluster.write("req-persist", "p1");
        assertEquals(first.index(), again.index());
        assertEquals(1, cluster.readCommitted().size());
    }

    @Test
    void concurrentWritesProduceContiguousIndexes() throws InterruptedException {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        int writers = 16;
        int perWriter = 5;
        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch go = new CountDownLatch(1);
        ConcurrentLinkedQueue<LogEntry> written = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();

        List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            final int id = w;
            Thread t = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                    for (int i = 0; i < perWriter; i++) {
                        written.add(cluster.write("req-" + id + "-" + i, "v-" + id + "-" + i));
                    }
                } catch (Throwable e) {
                    failures.add(e);
                }
            });
            threads.add(t);
            t.start();
        }
        assertTrue(ready.await(10, TimeUnit.SECONDS));
        go.countDown();
        for (Thread t : threads) {
            t.join(30_000);
        }

        assertTrue(failures.isEmpty(), "并发写入出现失败: " + failures);
        assertEquals(writers * perWriter, written.size());

        // 索引连续无跳变、无重复
        Set<Long> indexes = new HashSet<>();
        for (LogEntry e : written) {
            assertTrue(indexes.add(e.index()), "索引重复: " + e.index());
        }
        for (long i = 1; i <= writers * perWriter; i++) {
            assertTrue(indexes.contains(i), "索引跳变: 缺少 " + i);
        }
        // 提交点与可见条目数一致，无半更新状态
        assertEquals(writers * perWriter, cluster.readCommitted().size());
        assertEquals(writers * perWriter, cluster.leader().commitIndex());
    }
}
