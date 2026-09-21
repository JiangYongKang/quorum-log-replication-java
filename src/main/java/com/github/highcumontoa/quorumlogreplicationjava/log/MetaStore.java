package com.github.highcumontoa.quorumlogreplicationjava.log;

import com.github.highcumontoa.quorumlogreplicationjava.error.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ReplicationException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * 任期/投票/提交点元数据持久化。每次更新整体写临时文件后原子替换，避免半更新状态。
 */
public class MetaStore {

    private final Path file;
    private long currentTerm;
    private String votedFor;
    private long commitIndex;

    public MetaStore(Path dir) {
        try {
            Files.createDirectories(dir);
            this.file = dir.resolve("meta.properties");
            if (Files.exists(file)) {
                Properties p = new Properties();
                try (InputStream in = Files.newInputStream(file)) {
                    p.load(in);
                }
                this.currentTerm = Long.parseLong(p.getProperty("currentTerm", "0"));
                this.votedFor = p.getProperty("votedFor", null);
                this.commitIndex = Long.parseLong(p.getProperty("commitIndex", "0"));
            }
        } catch (IOException | NumberFormatException e) {
            throw new ReplicationException(ErrorCode.LOG_CORRUPTED, "元数据文件损坏: " + dir, e);
        }
    }

    private void persist() {
        Properties p = new Properties();
        p.setProperty("currentTerm", String.valueOf(currentTerm));
        if (votedFor != null) {
            p.setProperty("votedFor", votedFor);
        }
        p.setProperty("commitIndex", String.valueOf(commitIndex));
        Path tmp = file.resolveSibling("meta.properties.tmp");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            p.store(out, null);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new ReplicationException(ErrorCode.INTERNAL, "元数据持久化失败", e);
        }
    }

    public synchronized long currentTerm() {
        return currentTerm;
    }

    public synchronized void currentTerm(long term) {
        this.currentTerm = term;
        persist();
    }

    public synchronized String votedFor() {
        return votedFor;
    }

    public synchronized void votedFor(String nodeId) {
        this.votedFor = nodeId;
        persist();
    }

    public synchronized long commitIndex() {
        return commitIndex;
    }

    public synchronized void commitIndex(long index) {
        if (index > this.commitIndex) {
            this.commitIndex = index;
            persist();
        }
    }
}
