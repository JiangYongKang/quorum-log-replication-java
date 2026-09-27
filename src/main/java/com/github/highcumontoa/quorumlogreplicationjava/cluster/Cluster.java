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
    private final long maxWriteBytes;
    private final Map<String, ReplicaNode> nodes = new ConcurrentHashMap<>();
    private final Set<String> unreachable = ConcurrentHashMap.newKeySet();
    private final Transport transport = new InProcessTransport();

    public Cluster(List<String> nodeIds, Path rootDir, long maxLogEntries) {
        this(nodeIds, rootDir, maxLogEntries, 1_000_000);
    }

    public Cluster(List<String> nodeIds, Path rootDir, long maxLogEntries, long maxWriteBytes) {
        this.nodeIds = List.copyOf(nodeIds);
        this.rootDir = rootDir;
        this.maxLogEntries = maxLogEntries;
        this.maxWriteBytes = maxWriteBytes;
        for (String id : nodeIds) {
            nodes.put(id, createNode(id));
        }
    }

    private ReplicaNode createNode(String id) {
        List<String> peers = nodeIds.stream().filter(n -> !n.equals(id)).toList();
        return new ReplicaNode(id, peers, rootDir.resolve(id), transport, maxLogEntries, maxWriteBytes);
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

    /** 清空数据目录并重建节点（模拟本地历史完全丢失、由空目录重建的副本）。 */
    public ReplicaNode rebuild(String id) {
        ReplicaNode old = node(id);
        old.logStore().close();
        deleteRecursively(rootDir.resolve(id));
        ReplicaNode n = createNode(id);
        nodes.put(id, n);
        unreachable.remove(id);
        return n;
    }

    /** 让当前主副本立即对所有跟随者发起一轮复制/快照追平。 */
    public void triggerCatchUp() {
        for (ReplicaNode n : nodes.values()) {
            if (n.role() == com.github.highcumontoa.quorumlogreplicationjava.cluster.Role.LEADER) {
                n.syncFollowers();
            }
        }
    }

    private static void deleteRecursively(Path dir) {
        if (!java.nio.file.Files.exists(dir)) {
            return;
        }
        try (var stream = java.nio.file.Files.walk(dir)) {
            stream.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            java.nio.file.Files.delete(p);
                        } catch (java.io.IOException e) {
                            throw new ReplicationException(ErrorCode.INTERNAL, "无法删除 " + p, e);
                        }
                    });
        } catch (java.io.IOException e) {
            throw new ReplicationException(ErrorCode.INTERNAL, "无法清空数据目录 " + dir, e);
        }
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
            return target(targetId).onAppend(request);
        }

        @Override
        public ReadCheckResponse readCheck(String targetId, ReadCheckRequest request) {
            return target(targetId).onReadCheck(request);
        }

        @Override
        public SnapshotResponse snapshot(String targetId, SnapshotRequest request) {
            return target(targetId).onSnapshot(request);
        }

        @Override
        public void setReachable(String nodeId, boolean reachable) {
            Cluster.this.setReachable(nodeId, reachable);
        }

        private ReplicaNode target(String targetId) {
            if (unreachable.contains(targetId)) {
                throw new ReplicationException(ErrorCode.REPLICA_UNREACHABLE,
                        "节点 " + targetId + " 不可达");
            }
            ReplicaNode target = nodes.get(targetId);
            if (target == null) {
                throw new ReplicationException(ErrorCode.REPLICA_UNREACHABLE,
                        "节点 " + targetId + " 不存在");
            }
            return target;
        }
    }
}
