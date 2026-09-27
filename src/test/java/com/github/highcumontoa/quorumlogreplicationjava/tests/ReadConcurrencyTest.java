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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 读取语义在并发写入与网络抖动下的保持：
 * 任何被服务的读取都必须与主副本已提交结论一致（绝不半旧），状态不足时只能得到 STALE_READ；
 * 追平完成后各入口与主副本对齐。读写交叉不应发生锁序死锁。
 */
class ReadConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(ReadConcurrencyTest.class);
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
    void concurrentWritesAndReadsNeverServeStaleValuesAndConverge() throws Exception {
        cluster = new Cluster(IDS, dir, 10000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();

        int writers = 4;
        int perWriter = 50;
        ExecutorService pool = Executors.newFixedThreadPool(writers + 2);
        CountDownLatch start = new CountDownLatch(1);
        Set<String> failures = ConcurrentHashMap.newKeySet();
        AtomicInteger staleReads = new AtomicInteger();
        AtomicInteger servedReads = new AtomicInteger();

        // 写入线程：只写主副本
        for (int w = 0; w < writers; w++) {
            int wid = w;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perWriter; i++) {
                        String key = "k-" + wid + "-" + i;
                        leader.clientWrite("w" + wid + "-" + i, key, "v-" + wid + "-" + i);
                    }
                } catch (Exception e) {
                    failures.add("writer: " + e.getMessage());
                }
            });
        }
        // 读取线程：持续读两个跟随者。跟随者始终可达，故只要给出结论，
        // 其提交点必已得到主副本实时确认，读到的值必须与主副本当时状态机一致。
        for (String readerId : List.of("n2", "n3")) {
            pool.submit(() -> {
                try {
                    start.await();
                    ReplicaNode reader = cluster.node(readerId);
                    int rounds = 0;
                    while (rounds++ < 500) {
                        // 周期性主动追平，制造“追平窗口”，使被服务的读取（而非仅拒绝）也被覆盖
                        if (rounds % 25 == 0) {
                            cluster.triggerCatchUp();
                        }
                        int wid = rounds % writers;
                        int i = rounds % perWriter;
                        String key = "k-" + wid + "-" + i;
                        try {
                            Optional<String> got = reader.clientRead(key);
                            servedReads.incrementAndGet();
                            // 每个键只写一次，值一旦提交永不改变：凡被服务的非空读取，
                            // 值必须与主副本一致（半旧值/错位值都会在此暴露）。
                            // 空结果只可能出现在读校验通过的瞬间该键尚未提交，属合法结论
                            // （提交点单调前进，本地提交点 >= 主副本提交点时不可能漏掉已提交键）。
                            if (got.isPresent()) {
                                Optional<String> authoritative = Optional.ofNullable(
                                        leader.stateMachine().get(key));
                                if (authoritative.isEmpty() || !authoritative.equals(got)) {
                                    failures.add(readerId + " 读到错误/半旧值 key=" + key
                                            + " got=" + got + " leader=" + authoritative);
                                }
                            }
                        } catch (ReplicationException e) {
                            if (e.code() != ErrorCode.STALE_READ) {
                                failures.add(readerId + " 非预期错误: " + e.code());
                            } else {
                                staleReads.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    failures.add("reader: " + e.getMessage());
                }
            });
        }

        start.countDown();
        pool.shutdown();
        // 30 秒护栏：若读路径（持跟随者锁申请主副本锁）与写路径（持主副本锁申请跟随者锁）
        // 形成锁序死锁，这里会超时
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "读写并发发生死锁或长时间阻塞");
        assertTrue(failures.isEmpty(), "并发读取不一致: " + failures);

        int total = writers * perWriter;
        assertEquals(total, leader.commitIndex());
        cluster.triggerCatchUp();
        // 追平完成后各入口读到的内容和提交结论都与主副本一致，且不会再被拒绝
        for (String id : List.of("n2", "n3")) {
            ReplicaNode n = cluster.node(id);
            assertEquals(leader.commitIndex(), n.commitIndex(), id + " 提交点应追平");
            assertEquals(leader.stateMachine(), n.stateMachine(), id + " 状态机应与主副本一致");
            for (int w = 0; w < writers; w++) {
                for (int i = 0; i < perWriter; i++) {
                    assertEquals("v-" + w + "-" + i,
                            n.clientRead("k-" + w + "-" + i).orElseThrow(),
                            id + " 追平后读到键 k-" + w + "-" + i);
                }
            }
        }
        log.info("并发读写完成 total={} servedReads={} staleReads={}",
                total, servedReads.get(), staleReads.get());
    }

    @Test
    void readsWhileLeaderHoldsLockDuringReplicationDoNotDeadlock() throws Exception {
        cluster = new Cluster(IDS, dir, 1000);
        ReplicaNode leader = cluster.node("n1");
        leader.becomeLeader();
        leader.clientWrite("req-1", "k1", "v1");
        ReplicaNode n2 = cluster.node("n2");

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch done = new CountDownLatch(2);
        Set<String> errors = ConcurrentHashMap.newKeySet();
        // 写线程持续在主副本锁内向 n2 复制；读线程持续从 n2 持 n2 锁向主副本做读校验
        pool.submit(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    leader.clientWrite("r-" + i, "wk" + i, "wv" + i);
                }
            } catch (Exception e) {
                errors.add("writer: " + e.getMessage());
            } finally {
                done.countDown();
            }
        });
        pool.submit(() -> {
            try {
                for (int i = 0; i < 2000; i++) {
                    try {
                        n2.clientRead("k1");
                    } catch (ReplicationException e) {
                        assertEquals(ErrorCode.STALE_READ, e.code());
                    }
                }
            } catch (Throwable t) {
                errors.add("reader: " + t);
            } finally {
                done.countDown();
            }
        });
        assertTrue(done.await(15, TimeUnit.SECONDS), "主副本持锁复制时跟随者读取死锁");
        pool.shutdownNow();
        assertTrue(errors.isEmpty(), "并发过程出现错误: " + errors);
        assertEquals("v1", n2.clientRead("k1").orElseThrow(), "追平后读取正常");
    }
}
