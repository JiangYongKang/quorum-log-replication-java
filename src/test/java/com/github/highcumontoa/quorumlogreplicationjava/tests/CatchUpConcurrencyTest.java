package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.cluster.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.cluster.ReplicaNode;
import com.github.highcumontoa.quorumlogreplicationjava.log.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
 * 追平与正常写入/回收真正并发：写入线程持续提交的同时，一个历史被回收干净的
 * 副本重新加入，最终必须追平且各副本结论一致。
 */
class CatchUpConcurrencyTest {

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
    void catchUpRacesWithConcurrentWritesAndConverges() throws Exception {
        cluster = new Cluster(List.of("n1", "n2", "n3"), dir, 8, 1024 * 1024);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        for (int i = 1; i <= 10; i++) {
            leader.clientWrite("seed-" + i, "k" + i, "初始值😀" + i);
        }
        // n3 离线，主继续写入直到其本地历史被回收干净
        cluster.setReachable("n3", false);
        for (int i = 11; i <= 30; i++) {
            leader.clientWrite("offline-" + i, "k" + i, "离线期间值" + i);
        }
        assertTrue(leader.logStore().baseIndex() > 10, "n3 的历史已落在主回收点之前");
        cluster.setReachable("n3", true);

        int writers = 4;
        int perWriter = 30;
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
                        String rid = "w" + wid + "-" + i;
                        LogEntry e = leader.clientWrite(rid, "并发键-" + wid + "-" + i,
                                "并发值-中文😀-" + wid + "-" + i);
                        indexes.add(e.index());
                    }
                } catch (Exception e) {
                    failures.add(e.toString());
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        assertTrue(failures.isEmpty(), "并发写入不应失败: " + failures);

        int total = writers * perWriter;
        ReplicaNode n3 = cluster.node("n3");
        assertEquals(30 + total, leader.commitIndex(), "全部写入必须提交");
        assertEquals(leader.commitIndex(), n3.commitIndex(), "并发追平后提交点必须一致");
        assertEquals(leader.stateMachine(), n3.stateMachine(), "状态机必须逐键逐字一致");
        // 并发期间的索引连续
        for (long i = 31; i <= 30 + total; i++) {
            assertTrue(indexes.contains(i), "索引 " + i + " 缺失");
        }
        // 抽样验证多字节内容
        assertEquals("初始值😀1", n3.clientRead("k1").orElseThrow(), "最早的快照数据必须保留");
        assertEquals("离线期间值30", n3.clientRead("k30").orElseThrow());
    }
}
