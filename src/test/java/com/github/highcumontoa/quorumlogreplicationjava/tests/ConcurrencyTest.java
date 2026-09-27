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

    @Test
    void concurrentReadsNeverReturnHalfStaleResults() throws Exception {
        // 写入与读取并发：跟随者每次成功读取的内容都必须与"读取时刻本地提交点"
        // 在主副本上的结论一致——即成功读取只可能发生在读校验确认追平之后；
        // 任何不确定时刻只能拿到 STALE_READ，绝不允许把已提交键读成"不存在"。
        cluster = new Cluster(List.of("n1", "n2", "n3"), dir, 10000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();

        int writes = 100;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        java.util.concurrent.atomic.AtomicBoolean writeDone = new java.util.concurrent.atomic.AtomicBoolean();
        java.util.Set<String> violations = ConcurrentHashMap.newKeySet();
        java.util.concurrent.atomic.AtomicLong staleCount = new java.util.concurrent.atomic.AtomicLong();
        java.util.concurrent.atomic.AtomicLong trustedCount = new java.util.concurrent.atomic.AtomicLong();
        // 读取线程先启动：写入尚未开始时读未写键（追平水位 0），写入进行中持续并发
        pool.submit(() -> {
            ReplicaNode reader = cluster.node("n3");
            java.util.Random rnd = new java.util.Random(42);
            // 有界读取：与写入全程并发，覆盖已写/未写各种位置
            for (long n = 0; n < 5000; n++) {
                if (!violations.isEmpty()) {
                    return;
                }
                long upper = Math.min(writes, Math.max(5, leader.commitIndex() + 2));
                int i = 1 + rnd.nextInt((int) upper);
                try {
                    java.util.Optional<String> v = reader.clientRead("ck" + i);
                    trustedCount.incrementAndGet();
                    // 成功读取说明读校验已确认本地追平主副本：
                    // 值若返回必须正确；空值只允许出现在键的索引尚未进入本地提交点时
                    long readerCommit = reader.commitIndex();
                    if (v.isPresent()) {
                        if (!("cv" + i).equals(v.get())) {
                            violations.add("半旧/错误值 ck" + i + "=" + v);
                        }
                    } else if (i <= readerCommit) {
                        violations.add("已提交键被读成不存在 ck" + i
                                + " readerCommit=" + readerCommit);
                    }
                } catch (com.github.highcumontoa.quorumlogreplicationjava.error.ReplicationException e) {
                    if (e.code() == com.github.highcumontoa.quorumlogreplicationjava.error.ErrorCode.STALE_READ) {
                        staleCount.incrementAndGet();
                    } else {
                        violations.add("非预期错误: " + e.code());
                    }
                }
            }
        });
        pool.submit(() -> {
            leader.becomeLeader();
            for (int i = 1; i <= writes; i++) {
                leader.clientWrite("c-" + i, "ck" + i, "cv" + i);
            }
            writeDone.set(true);
        });
        pool.shutdown();
        assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS));
        assertTrue(violations.isEmpty(), "并发读取不得出现半旧结果或假不存在: " + violations);
        assertTrue(staleCount.get() + trustedCount.get() > 0, "应实际发生过读取");

        // 追平完成后：所有入口（主副本与各跟随者）读到的内容和提交结论完全一致
        cluster.triggerCatchUp();
        for (String id : List.of("n2", "n3")) {
            ReplicaNode n = cluster.node(id);
            assertEquals(leader.commitIndex(), n.commitIndex(), id + " 应已追平");
            for (int i = 1; i <= writes; i++) {
                assertEquals("cv" + i, n.clientRead("ck" + i).orElseThrow(),
                        id + " 追平后 ck" + i + " 与主副本一致");
            }
        }
        log.info("并发读取完成 拒绝(STALE_READ)次数={} 成功读取={} 无半旧结果, 追平后各入口一致",
                staleCount.get(), trustedCount.get());
    }
}
