package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.cluster.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.cluster.ReplicaNode;
import com.github.highcumontoa.quorumlogreplicationjava.log.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发写入与提交点推进的一致性：索引连续、无错位、无半更新。
 */
class ConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);

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
    void concurrentWritesProduceContiguousIndexes() throws Exception {
        cluster = new Cluster(List.of("n1", "n2", "n3"), dir, 10000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();

        int writers = 8;
        int perWriter = 25;
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        CountDownLatch start = new CountDownLatch(1);
        Set<Long> indexes = ConcurrentHashMap.newKeySet();
        Set<String> failures = ConcurrentHashMap.newKeySet();
        for (int w = 0; w < writers; w++) {
            int wid = w;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perWriter; i++) {
                        LogEntry e = leader.clientWrite("w" + wid + "-" + i, "k" + wid + "-" + i, "v");
                        indexes.add(e.index());
                    }
                } catch (Exception e) {
                    failures.add(e.getMessage());
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        assertTrue(failures.isEmpty(), "并发写入不应失败: " + failures);

        int total = writers * perWriter;
        assertEquals(total, indexes.size(), "索引不得重复");
        assertEquals(total, leader.commitIndex(), "提交点应推进到末尾");
        // 索引连续无跳变
        for (long i = 1; i <= total; i++) {
            assertTrue(indexes.contains(i), "索引 " + i + " 缺失");
            LogEntry e = leader.logStore().get(i).orElseThrow();
            assertEquals(i, e.index(), "日志位置不得错位");
        }
        // 各副本状态机一致
        for (String id : List.of("n2", "n3")) {
            assertEquals(leader.stateMachine(), cluster.node(id).stateMachine(),
                    id + " 状态机应与主副本一致");
        }
        log.info("并发写入完成 total={} commitIndex={} 索引连续", total, leader.commitIndex());
    }
}
