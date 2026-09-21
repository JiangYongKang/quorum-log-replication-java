package com.github.highcumontoa.quorumlogreplicationjava.log;

import com.github.highcumontoa.quorumlogreplicationjava.error.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ReplicationException;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * 持久化日志存储。
 *
 * <p>记录格式：[crc32(4)][term(8)][index(8)][requestIdLen(4)][keyLen(4)][valueLen(4)][payload]，
 * crc 覆盖头部与载荷。启动时全量扫描重建索引：
 * <ul>
 *   <li>尾部不完整记录视为崩溃时的撕裂写，直接截断；</li>
 *   <li>完整记录但 CRC 不匹配视为日志损坏，抛出 {@link ErrorCode#LOG_CORRUPTED}。</li>
 * </ul>
 */
public class LogStore {

    private static final int HEADER = 4 + 8 + 8 + 4 + 4 + 4;

    private final Path file;
    private final List<LogEntry> entries = new ArrayList<>();
    private final List<Long> offsets = new ArrayList<>();
    private RandomAccessFile raf;
    private final Path baseFile;
    /** 已被回收的前缀中最后一条的索引（日志基址）。 */
    private long baseIndex;
    /** 基址条目的任期（用于复制前序一致性检查）。 */
    private long baseTerm;

    public LogStore(Path dir) {
        try {
            Files.createDirectories(dir);
            this.file = dir.resolve("log.dat");
            this.baseFile = dir.resolve("log.base");
            boolean exists = Files.exists(file);
            this.raf = new RandomAccessFile(file.toFile(), "rw");
            if (Files.exists(baseFile)) {
                byte[] b = Files.readAllBytes(baseFile);
                ByteBuffer bb = ByteBuffer.wrap(b);
                this.baseIndex = bb.getLong();
                this.baseTerm = bb.getLong();
            }
            if (exists) {
                recover();
            }
        } catch (IOException e) {
            throw new ReplicationException(ErrorCode.INTERNAL, "无法打开日志文件: " + dir, e);
        }
    }

    private void recover() throws IOException {
        long offset = 0;
        long len = raf.length();
        while (offset < len) {
            if (len - offset < HEADER) {
                truncateFile(offset); // 撕裂写
                break;
            }
            raf.seek(offset);
            int crc = raf.readInt();
            long term = raf.readLong();
            long index = raf.readLong();
            int ridLen = raf.readInt();
            int keyLen = raf.readInt();
            int valLen = raf.readInt();
            int payloadLen = ridLen + keyLen + valLen;
            if (ridLen < 0 || keyLen < 0 || valLen < 0 || len - offset < HEADER + payloadLen) {
                truncateFile(offset); // 撕裂写
                break;
            }
            byte[] payload = new byte[payloadLen];
            raf.readFully(payload);
            CRC32 crc32 = new CRC32();
            ByteBuffer head = ByteBuffer.allocate(HEADER - 4);
            head.putLong(term).putLong(index).putInt(ridLen).putInt(keyLen).putInt(valLen);
            crc32.update(head.array());
            crc32.update(payload);
            if ((int) crc32.getValue() != crc) {
                throw new ReplicationException(ErrorCode.LOG_CORRUPTED,
                        "日志记录在 offset=" + offset + " 处 CRC 校验失败");
            }
            ByteBuffer p = ByteBuffer.wrap(payload);
            String rid = readString(p, ridLen);
            String key = readString(p, keyLen);
            String value = readString(p, valLen);
            entries.add(new LogEntry(term, index, rid, key, value));
            offsets.add(offset);
            offset += HEADER + payloadLen;
        }
        raf.seek(raf.length());
    }

    private static String readString(ByteBuffer p, int len) {
        byte[] b = new byte[len];
        p.get(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private void truncateFile(long offset) throws IOException {
        raf.setLength(offset);
        raf.seek(offset);
    }

    public synchronized void append(LogEntry entry) {
        long expected = entries.isEmpty() ? baseIndex + 1 : entries.get(entries.size() - 1).index() + 1;
        if (entry.index() != expected) {
            throw new ReplicationException(ErrorCode.LOG_MISMATCH,
                    "追加索引不连续: 期望 " + expected + " 实际 " + entry.index());
        }
        try {
            byte[] rid = entry.requestId().getBytes(StandardCharsets.UTF_8);
            byte[] key = entry.key().getBytes(StandardCharsets.UTF_8);
            byte[] val = entry.value().getBytes(StandardCharsets.UTF_8);
            ByteBuffer head = ByteBuffer.allocate(HEADER - 4);
            head.putLong(entry.term()).putLong(entry.index())
                    .putInt(rid.length).putInt(key.length).putInt(val.length);
            CRC32 crc32 = new CRC32();
            crc32.update(head.array());
            crc32.update(rid);
            crc32.update(key);
            crc32.update(val);
            ByteBuffer rec = ByteBuffer.allocate(HEADER + rid.length + key.length + val.length);
            rec.putInt((int) crc32.getValue());
            rec.put(head.array());
            rec.put(rid).put(key).put(val);
            long offset = raf.length();
            raf.seek(offset);
            raf.write(rec.array());
            raf.getFD().sync();
            entries.add(entry);
            offsets.add(offset);
        } catch (IOException e) {
            throw new ReplicationException(ErrorCode.INTERNAL, "日志追加失败", e);
        }
    }

    public synchronized Optional<LogEntry> get(long index) {
        int i = toPos(index);
        return i < 0 ? Optional.empty() : Optional.of(entries.get(i));
    }

    public synchronized long lastIndex() {
        return entries.isEmpty() ? baseIndex : entries.get(entries.size() - 1).index();
    }

    public synchronized long lastTerm() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
    }

    public synchronized long baseIndex() {
        return baseIndex;
    }

    private int toPos(long index) {
        if (entries.isEmpty()) {
            return -1;
        }
        long first = entries.get(0).index();
        int pos = (int) (index - first);
        return (pos < 0 || pos >= entries.size()) ? -1 : pos;
    }

    /** 删除 index 及之后的所有条目（冲突截断）。 */
    public synchronized void truncateSuffix(long index) {
        int pos = toPos(index);
        if (pos < 0) {
            return;
        }
        try {
            truncateFile(offsets.get(pos));
            entries.subList(pos, entries.size()).clear();
            offsets.subList(pos, offsets.size()).clear();
        } catch (IOException e) {
            throw new ReplicationException(ErrorCode.INTERNAL, "日志截断失败", e);
        }
    }

    /** 回收已提交的历史前缀，保留 [fromIndex, ...]。 */
    public synchronized void truncatePrefix(long fromIndex) {
        if (entries.isEmpty()) {
            return;
        }
        // pos = 索引小于 fromIndex 的条目数
        int pos = (int) Math.min(Math.max(fromIndex - entries.get(0).index(), 0), entries.size());
        if (pos <= 0) {
            return; // 没有可回收的前缀
        }
        long newBase = entries.get(pos - 1).index();
        List<LogEntry> kept = new ArrayList<>(entries.subList(pos, entries.size()));
        try {
            Path tmp = file.resolveSibling("log.dat.tmp");
            Files.deleteIfExists(tmp);
            LogStore fresh = new LogStore(tmp.getParent(), "log.dat.tmp");
            fresh.persistBase(entries.get(pos - 1).index(), entries.get(pos - 1).term());
            for (LogEntry e : kept) {
                fresh.append(e);
            }
            fresh.close();
            raf.close();
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            this.raf = new RandomAccessFile(file.toFile(), "rw");
            this.raf.seek(raf.length());
        } catch (IOException e) {
            throw new ReplicationException(ErrorCode.INTERNAL, "日志前缀回收失败", e);
        }
        persistBase(newBase, entries.get(pos - 1).term());
        entries.clear();
        entries.addAll(kept);
        offsets.clear();
        long off = 0;
        for (LogEntry e : entries) {
            offsets.add(off);
            off += HEADER + e.requestId().length() + e.key().length() + e.value().length();
        }
    }

    private void persistBase(long index, long term) {
        this.baseIndex = index;
        this.baseTerm = term;
        try {
            Files.write(baseFile, ByteBuffer.allocate(16).putLong(index).putLong(term).array());
        } catch (IOException e) {
            throw new ReplicationException(ErrorCode.INTERNAL, "日志基址持久化失败", e);
        }
    }

    public synchronized long baseTerm() {
        return baseTerm;
    }

    // 供前缀回收时以临时文件名重建
    private LogStore(Path dir, String fileName) {
        try {
            this.file = dir.resolve(fileName);
            this.baseFile = dir.resolve(fileName + ".base.tmp");
            this.raf = new RandomAccessFile(file.toFile(), "rw");
        } catch (IOException e) {
            throw new ReplicationException(ErrorCode.INTERNAL, "无法创建临时日志文件", e);
        }
    }

    public synchronized List<LogEntry> entriesFrom(long index) {
        int pos = toPos(index);
        if (pos < 0) {
            return List.of();
        }
        return List.copyOf(entries.subList(pos, entries.size()));
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized void close() {
        try {
            if (raf != null) {
                raf.close();
            }
        } catch (IOException e) {
            throw new ReplicationException(ErrorCode.INTERNAL, "日志关闭失败", e);
        }
    }
}
