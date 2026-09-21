package com.github.highcumontoa.quorumlogreplicationjava.api;

import com.github.highcumontoa.quorumlogreplicationjava.cluster.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.cluster.ReplicaNode;
import com.github.highcumontoa.quorumlogreplicationjava.log.LogEntry;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 客户端与运维 REST 入口。
 */
@RestController
@RequestMapping("/api")
public class NodeController {

    private final Cluster cluster;

    public NodeController(Cluster cluster) {
        this.cluster = cluster;
    }

    public record WriteRequest(String requestId, String key, String value) {
    }

    public record WriteResponse(boolean ok, long term, long index, long commitIndex) {
    }

    public record ReadResponse(boolean ok, String value, long commitIndex) {
    }

    /** 客户端写入：仅主副本受理，多数派确认后返回。 */
    @PostMapping("/nodes/{id}/write")
    public WriteResponse write(@PathVariable String id, @RequestBody WriteRequest request) {
        LogEntry e = cluster.node(id).clientWrite(request.requestId(), request.key(), request.value());
        return new WriteResponse(true, e.term(), e.index(), cluster.node(id).commitIndex());
    }

    /** 客户端读取：只返回已提交数据（committed-read）。 */
    @GetMapping("/nodes/{id}/read")
    public ReadResponse read(@PathVariable String id, @RequestParam String key) {
        ReplicaNode node = cluster.node(id);
        Optional<String> value = node.clientRead(key);
        return new ReadResponse(value.isPresent(), value.orElse(null), node.commitIndex());
    }

    /** 节点状态：角色、任期、提交点、日志范围。 */
    @GetMapping("/nodes/{id}/status")
    public Map<String, Object> status(@PathVariable String id) {
        ReplicaNode node = cluster.node(id);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", node.id());
        m.put("role", node.role().name());
        m.put("term", node.currentTerm());
        m.put("leaderId", node.leaderId());
        m.put("commitIndex", node.commitIndex());
        m.put("lastIndex", node.logStore().lastIndex());
        m.put("baseIndex", node.logStore().baseIndex());
        m.put("stateMachine", node.stateMachine());
        return m;
    }

    /** 运维：指定节点成为主副本（模拟选主）。 */
    @PostMapping("/nodes/{id}/elect")
    public Map<String, Object> elect(@PathVariable String id) {
        ReplicaNode node = cluster.node(id);
        node.becomeLeader();
        return status(id);
    }

    /** 运维：模拟崩溃。 */
    @PostMapping("/nodes/{id}/crash")
    public Map<String, String> crash(@PathVariable String id) {
        cluster.crash(id);
        return Map.of("ok", "true", "node", id);
    }

    /** 运维：从磁盘恢复。 */
    @PostMapping("/nodes/{id}/restart")
    public Map<String, Object> restart(@PathVariable String id) {
        cluster.restart(id);
        return status(id);
    }

    /** 运维：网络隔离/恢复。 */
    @PostMapping("/nodes/{id}/reachable")
    public Map<String, String> reachable(@PathVariable String id, @RequestParam boolean value) {
        cluster.setReachable(id, value);
        return Map.of("ok", "true", "node", id, "reachable", String.valueOf(value));
    }
}
