package com.github.highcumontoa.quorumlogreplicationjava.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.zip.CRC32;

/**
 * 本地持久化日志存储。
 *
 * <p>每个节点一个目录，包含：
 * <ul>
 *   <li>{@code meta.properties}：currentTerm / votedFor / commitIndex</li>
 *   <li>{@code log.dat}：每行一条 {@code term|index|requestId|base64(payload)|crc32}</li>
 * </ul>
 * 加载时逐条校验 CRC 与索引连续性，损坏即抛出 {@link ErrorCode#CORRUPTED_LOG}，
 * 底层 IO 异常一律包装为 {@link RaftException}，不直接透出。
 */
public class LogStore {

    private static final Logger log = LoggerFactory.getLogger(LogStore.class);

    private final Path nodeDir;
    private final Path metaFile;
    private final Path logFile;

    private final List<LogEntry> entries = new ArrayList<>();
    private long currentTerm;
    private String votedFor;
    private long commitIndex;

    public LogStore(Path nodeDir) {
        this.nodeDir = nodeDir;
        this.metaFile = nodeDir.resolve("meta.properties");
        this.logFile = nodeDir.resolve("log.dat");
    }

    /** 从磁盘加载状态；目录不存在时按空状态初始化。 */
    public synchronized void load() {
        entries.clear();
        currentTerm = 0;
        votedFor = null;
        commitIndex = 0;
        try {
            Files.createDirectories(nodeDir);
            loadMeta();
            loadLog();
        } catch (RaftException e) {
            throw e;
        } catch (IOException | UncheckedIOException e) {
            throw new RaftException(ErrorCode.UNAVAILABLE, "日志存储读写失败: " + nodeDir, e);
        }
        log.info("store loaded dir={} term={} commitIndex={} entries={}",
                nodeDir, currentTerm, commitIndex, entries.size());
    }

    private void loadMeta() throws IOException {
        if (!Files.exists(metaFile)) {
            return;
        }
        Properties props = new Properties();
        try (var in = Files.newInputStream(metaFile)) {
            props.load(in);
        }
        currentTerm = Long.parseLong(props.getProperty("term", "0"));
        String voted = props.getProperty("votedFor", "");
        votedFor = voted.isEmpty() ? null : voted;
        commitIndex = Long.parseLong(props.getProperty("commitIndex", "0"));
    }

    private void loadLog() throws IOException {
        if (!Files.exists(logFile)) {
            return;
        }
        final List<String> lines;
        try {
            lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
        } catch (java.io.UncheckedIOException | java.nio.charset.CharacterCodingException e) {
            throw new RaftException(ErrorCode.CORRUPTED_LOG, "日志内容无法解码: " + logFile, e);
        }
        long expectedIndex = -1;
        for (String line : lines) {
            if (line.isBlank()) {
                continue;
            }
            try {
                expectedIndex = parseLine(line, expectedIndex);
            } catch (RaftException e) {
                throw e;
            } catch (RuntimeException e) {
                throw new RaftException(ErrorCode.CORRUPTED_LOG, "日志行解析失败: " + line, e);
            }
        }
    }

    private long parseLine(String line, long expectedIndex) {
        String[] parts = line.split("\\|", -1);
        if (parts.length != 5) {
            throw new RaftException(ErrorCode.CORRUPTED_LOG, "日志行格式非法: " + line);
        }
        CRC32 crc = new CRC32();
        String body = parts[0] + "|" + parts[1] + "|" + parts[2] + "|" + parts[3];
        crc.update(body.getBytes(StandardCharsets.UTF_8));
        if (crc.getValue() != Long.parseUnsignedLong(parts[4])) {
            throw new RaftException(ErrorCode.CORRUPTED_LOG,
                    "日志校验和失败 index=" + parts[1]);
        }
        long term = Long.parseLong(parts[0]);
        long index = Long.parseLong(parts[1]);
        if (expectedIndex >= 0 && index != expectedIndex + 1) {
            throw new RaftException(ErrorCode.CORRUPTED_LOG,
                    "日志索引不连续 expected=" + (expectedIndex + 1) + " actual=" + index);
        }
        String payload = new String(Base64.getDecoder().decode(parts[3]), StandardCharsets.UTF_8);
        entries.add(new LogEntry(term, index, parts[2], payload));
        return index;
    }

    public synchronized List<LogEntry> entries() {
        return List.copyOf(entries);
    }

    public synchronized long currentTerm() {
        return currentTerm;
    }

    public synchronized String votedFor() {
        return votedFor;
    }

    public synchronized long commitIndex() {
        return commitIndex;
    }

    /** 持久化元数据（先写临时文件再原子替换，避免半更新状态）。 */
    public synchronized void saveMeta(long term, String votedFor, long commitIndex) {
        this.currentTerm = term;
        this.votedFor = votedFor;
        this.commitIndex = commitIndex;
        Properties props = new Properties();
        props.setProperty("term", Long.toString(term));
        props.setProperty("votedFor", votedFor == null ? "" : votedFor);
        props.setProperty("commitIndex", Long.toString(commitIndex));
        Path tmp = nodeDir.resolve("meta.properties.tmp");
        try {
            try (var out = Files.newOutputStream(tmp)) {
                props.store(out, "raft meta");
            }
            Files.move(tmp, metaFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new RaftException(ErrorCode.UNAVAILABLE, "元数据持久化失败", e);
        }
    }

    /** 追加条目并落盘。 */
    public synchronized void append(List<LogEntry> newEntries) {
        if (newEntries.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (LogEntry e : newEntries) {
            sb.append(encode(e)).append('\n');
        }
        try {
            Files.writeString(logFile, sb.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RaftException(ErrorCode.UNAVAILABLE, "日志追加落盘失败", e);
        }
        entries.addAll(newEntries);
    }

    /** 截断 {@code fromIndex} 及之后的所有条目（用于覆盖冲突后缀）。 */
    public synchronized void truncateSuffix(long fromIndex) {
        entries.removeIf(e -> e.index() >= fromIndex);
        rewriteLogFile();
    }

    /**
     * 回收已提交的历史前缀：index &lt; upToIndexExclusive 的条目从活跃日志移除，
     * 但追加到 {@code archive.log} 归档，已提交数据不丢失、仍可在本机读取核对。
     */
    public synchronized void truncatePrefix(long upToIndexExclusive) {
        List<LogEntry> archived = entries.stream().filter(e -> e.index() < upToIndexExclusive).toList();
        if (archived.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (LogEntry e : archived) {
            sb.append(encode(e)).append('\n');
        }
        try {
            Files.writeString(nodeDir.resolve("archive.log"), sb.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RaftException(ErrorCode.UNAVAILABLE, "归档落盘失败", e);
        }
        entries.removeIf(e -> e.index() < upToIndexExclusive);
        rewriteLogFile();
    }

    private void rewriteLogFile() {
        StringBuilder sb = new StringBuilder();
        for (LogEntry e : entries) {
            sb.append(encode(e)).append('\n');
        }
        Path tmp = nodeDir.resolve("log.dat.tmp");
        try {
            Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new RaftException(ErrorCode.UNAVAILABLE, "日志重写落盘失败", e);
        }
    }

    private static String encode(LogEntry e) {
        String body = e.term() + "|" + e.index() + "|" + e.requestId() + "|"
                + Base64.getEncoder().encodeToString(e.payload().getBytes(StandardCharsets.UTF_8));
        CRC32 crc = new CRC32();
        crc.update(body.getBytes(StandardCharsets.UTF_8));
        return body + "|" + crc.getValue();
    }
}
