package com.github.highcumontoa.quorumlogreplicationjava.cluster;

import com.github.highcumontoa.quorumlogreplicationjava.error.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.error.ReplicationException;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群管理：创建、崩溃、重启副本；进程内传输层。
 */
public class Cluster implements AutoCloseable {

    private final List<String> nodeIds;
    private final Path rootDir;
    private final long maxLogEntries;
    private final int maxEntryBytes;
    private final Map<String, ReplicaNode> nodes = new ConcurrentHashMap<>();
    private final Set<String> unreachable = ConcurrentHashMap.newKeySet();
    private final Transport transport = new InProcessTransport();

    public Cluster(List<String> nodeIds, Path rootDir, long maxLogEntries, int maxEntryBytes) {
        this.nodeIds = List.copyOf(nodeIds);
        this.rootDir = rootDir;
        this.maxLogEntries = maxLogEntries;
        this.maxEntryBytes = maxEntryBytes;
        for (String id : nodeIds) {
            nodes.put(id, createNode(id));
        }
    }

    /** 兼容入口：单条写入大小上限使用默认值 1 MiB。 */
    public Cluster(List<String> nodeIds, Path rootDir, long maxLogEntries) {
        this(nodeIds, rootDir, maxLogEntries, 1024 * 1024);
    }

    private ReplicaNode createNode(String id) {
        List<String> peers = nodeIds.stream().filter(n -> !n.equals(id)).toList();
        return new ReplicaNode(id, peers, rootDir.resolve(id), transport,
                maxLogEntries, maxEntryBytes);
    }

    public ReplicaNode node(String id) {
        ReplicaNode n = nodes.get(id);
        if (n == null) {
            throw new ReplicationException(ErrorCode.INVALID_ARGUMENT, "未知节点: " + id);
        }
        return n;
    }

    public List<String> nodeIds() {
        return nodeIds;
    }

    /** 模拟崩溃：节点停止响应，数据保留在磁盘。 */
    public void crash(String id) {
        ReplicaNode n = node(id);
        n.simulateCrash();
        unreachable.add(id);
    }

    /** 从磁盘恢复节点。 */
    public ReplicaNode restart(String id) {
        node(id); // 校验存在
        ReplicaNode n = createNode(id);
        nodes.put(id, n);
        unreachable.remove(id);
        return n;
    }

    /** 网络隔离/恢复（不销毁内存状态）。 */
    public void setReachable(String id, boolean reachable) {
        if (reachable) {
            unreachable.remove(id);
        } else {
            unreachable.add(id);
        }
    }

    @Override
    public void close() {
        for (ReplicaNode n : nodes.values()) {
            n.logStore().close();
        }
    }

    /** 进程内传输：不可达节点抛出 {@link ErrorCode#REPLICA_UNREACHABLE}。 */
    private class InProcessTransport implements Transport {
        @Override
        public AppendResponse append(String targetId, AppendRequest request) {
            if (unreachable.contains(targetId)) {
                throw new ReplicationException(ErrorCode.REPLICA_UNREACHABLE,
                        "节点 " + targetId + " 不可达");
            }
            ReplicaNode target = nodes.get(targetId);
            if (target == null) {
                throw new ReplicationException(ErrorCode.REPLICA_UNREACHABLE,
                        "节点 " + targetId + " 不存在");
            }
            return target.onAppend(request);
        }

        @Override
        public SnapshotResponse installSnapshot(String targetId, SnapshotRequest request) {
            if (unreachable.contains(targetId)) {
                throw new ReplicationException(ErrorCode.REPLICA_UNREACHABLE,
                        "节点 " + targetId + " 不可达");
            }
            ReplicaNode target = nodes.get(targetId);
            if (target == null) {
                throw new ReplicationException(ErrorCode.REPLICA_UNREACHABLE,
                        "节点 " + targetId + " 不存在");
            }
            return target.onInstallSnapshot(request);
        }

        @Override
        public ReadIndexResponse queryReadIndex(String targetId, ReadIndexRequest request) {
            if (unreachable.contains(targetId)) {
                throw new ReplicationException(ErrorCode.REPLICA_UNREACHABLE,
                        "节点 " + targetId + " 不可达");
            }
            ReplicaNode target = nodes.get(targetId);
            if (target == null) {
                throw new ReplicationException(ErrorCode.REPLICA_UNREACHABLE,
                        "节点 " + targetId + " 不存在");
            }
            return target.onReadIndex(request);
        }

        @Override
        public void setReachable(String nodeId, boolean reachable) {
            Cluster.this.setReachable(nodeId, reachable);
        }
    }
}
