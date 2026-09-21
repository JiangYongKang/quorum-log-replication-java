package com.github.highcumontoa.quorumlogreplicationjava.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群编排：在本机单 JVM 内模拟多副本、网络故障与崩溃重启。
 * 客户端入口路由到当前主副本；无主时拒绝并给出可区分原因。
 */
public class Cluster {

    private static final Logger log = LoggerFactory.getLogger(Cluster.class);

    private final RaftConfig config;
    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final InMemoryTransport transport = new InMemoryTransport();

    public Cluster(RaftConfig config) {
        this.config = config;
    }

    /** 创建全部节点并上线，随后由首个节点发起选主。 */
    public void start() {
        List<String> ids = Arrays.stream(config.getNodes().split(",")).map(String::trim).toList();
        for (String id : ids) {
            LogStore store = new LogStore(Path.of(config.getDataDir(), id));
            RaftNode node = new RaftNode(id, ids, config, store);
            node.setTransport(transport);
            nodes.put(id, node);
            transport.register(id, node);
            node.recover();
        }
        elect(ids.get(0));
    }

    /** 让指定节点发起选主；用于初始选主与主副本更替。 */
    public void elect(String candidateId) {
        RaftNode candidate = nodes.get(candidateId);
        if (candidate == null) {
            throw new RaftException(ErrorCode.INVALID_ARGUMENT, "未知节点: " + candidateId);
        }
        candidate.startElection();
    }

    public RaftNode node(String id) {
        RaftNode node = nodes.get(id);
        if (node == null) {
            throw new RaftException(ErrorCode.INVALID_ARGUMENT, "未知节点: " + id);
        }
        return node;
    }

    /** 当前在线主副本；无主时返回 null。 */
    public RaftNode leader() {
        for (RaftNode node : nodes.values()) {
            if (node.isUp() && node.role() == NodeRole.LEADER) {
                return node;
            }
        }
        return null;
    }

    public List<RaftNode> nodes() {
        return List.copyOf(nodes.values());
    }

    /** 客户端写入：路由到主副本。 */
    public LogEntry write(String requestId, String payload) {
        RaftNode leader = leader();
        if (leader == null) {
            throw new RaftException(ErrorCode.NOT_LEADER, "集群当前无可用主副本");
        }
        return leader.clientWrite(requestId, payload);
    }

    /** 客户端读取：主副本的已提交前缀。 */
    public List<LogEntry> readCommitted() {
        RaftNode leader = leader();
        if (leader == null) {
            throw new RaftException(ErrorCode.NOT_LEADER, "集群当前无可用主副本");
        }
        return leader.committedEntries();
    }

    public void crashNode(String id) {
        node(id).crash();
        transport.setReachable(id, false);
    }

    public void restartNode(String id) {
        transport.setReachable(id, true);
        node(id).recover();
    }

    /** 模拟网络分区：切断指定节点与其余节点的通信。 */
    public void isolate(String id) {
        transport.setReachable(id, false);
    }

    public void heal(String id) {
        transport.setReachable(id, true);
    }

    /** 内存传输：按目标节点路由，可注入不可达故障。 */
    static final class InMemoryTransport implements Transport {

        private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
        private final Set<String> unreachable = ConcurrentHashMap.newKeySet();

        void register(String id, RaftNode node) {
            nodes.put(id, node);
        }

        @Override
        public AppendEntriesResult sendAppendEntries(String targetId, AppendEntriesRequest request) {
            RaftNode target = reachable(targetId);
            return target.handleAppendEntries(request);
        }

        @Override
        public VoteResult sendRequestVote(String targetId, VoteRequest request) {
            RaftNode target = reachable(targetId);
            return target.handleRequestVote(request);
        }

        @Override
        public void setReachable(String nodeId, boolean reachable) {
            if (reachable) {
                unreachable.remove(nodeId);
            } else {
                unreachable.add(nodeId);
            }
        }

        private RaftNode reachable(String targetId) {
            RaftNode target = nodes.get(targetId);
            if (target == null || unreachable.contains(targetId) || !target.isUp()) {
                throw new RaftException(ErrorCode.UNAVAILABLE, "副本不可达: " + targetId);
            }
            return target;
        }
    }
}
