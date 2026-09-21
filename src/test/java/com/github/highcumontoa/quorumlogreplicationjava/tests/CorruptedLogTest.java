package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.raft.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.raft.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.raft.LogStore;
import com.github.highcumontoa.quorumlogreplicationjava.raft.RaftException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 异常归一化：日志损坏映射为可区分的 CORRUPTED_LOG，底层 IO 异常不直接透出。 */
class CorruptedLogTest {

    @TempDir
    Path dataDir;

    @Test
    void corruptedLogIsDetectedOnLoad() throws Exception {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        cluster.write("req-1", "value-1");

        // 篡改落盘日志内容（破坏校验和）
        Path logFile = dataDir.resolve("node2/log.dat");
        byte[] bytes = Files.readAllBytes(logFile);
        bytes[10] = (byte) (bytes[10] ^ 0xFF);
        Files.write(logFile, bytes);

        LogStore store = new LogStore(dataDir.resolve("node2"));
        RaftException ex = assertThrows(RaftException.class, store::load);
        assertEquals(ErrorCode.CORRUPTED_LOG, ex.code());
    }

    @Test
    void truncatedLogLineIsDetected() throws Exception {
        Cluster cluster = ClusterTestSupport.newCluster(dataDir);
        cluster.write("req-1", "value-1");

        Path logFile = dataDir.resolve("node3/log.dat");
        String content = Files.readString(logFile, StandardCharsets.UTF_8);
        Files.writeString(logFile, content.substring(0, content.length() / 2), StandardCharsets.UTF_8);

        LogStore store = new LogStore(dataDir.resolve("node3"));
        RaftException ex = assertThrows(RaftException.class, store::load);
        assertEquals(ErrorCode.CORRUPTED_LOG, ex.code());
    }
}
