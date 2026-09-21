package com.github.highcumontoa.quorumlogreplicationjava.api;

import com.github.highcumontoa.quorumlogreplicationjava.raft.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.raft.ErrorCode;
import com.github.highcumontoa.quorumlogreplicationjava.raft.RaftException;
import com.github.highcumontoa.quorumlogreplicationjava.raft.Transport;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 节点间入口：复制与选主请求的边界校验。
 * 目标副本通过 {@code node} 参数显式指定；未知目标、非法参数一律拒绝且不改变既有状态。
 */
@RestController
@RequestMapping("/raft")
public class NodeController {

    private final Cluster cluster;

    public NodeController(Cluster cluster) {
        this.cluster = cluster;
    }

    @PostMapping("/append-entries")
    public Transport.AppendEntriesResult appendEntries(@RequestParam String node,
                                                       @RequestBody Transport.AppendEntriesRequest request) {
        if (request.leaderId() == null || request.leaderId().isBlank()) {
            throw new RaftException(ErrorCode.INVALID_ARGUMENT, "leaderId 不能为空");
        }
        return cluster.node(node).handleAppendEntries(request);
    }

    @PostMapping("/vote")
    public Transport.VoteResult vote(@RequestParam String node,
                                     @RequestBody Transport.VoteRequest request) {
        if (request.candidateId() == null || request.candidateId().isBlank()) {
            throw new RaftException(ErrorCode.INVALID_ARGUMENT, "candidateId 不能为空");
        }
        return cluster.node(node).handleRequestVote(request);
    }
}
