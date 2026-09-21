package com.github.highcumontoa.quorumlogreplicationjava.api;

import com.github.highcumontoa.quorumlogreplicationjava.raft.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.raft.LogEntry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** 客户端入口：写入与读取。 */
@RestController
@RequestMapping("/client")
public class ClientController {

    private final Cluster cluster;

    public ClientController(Cluster cluster) {
        this.cluster = cluster;
    }

    @PostMapping("/write")
    public Dtos.WriteResponse write(@RequestBody Dtos.WriteRequest request) {
        LogEntry entry = cluster.write(request.requestId(), request.payload());
        return new Dtos.WriteResponse(entry.term(), entry.index(), entry.requestId(), true);
    }

    @GetMapping("/read")
    public Dtos.ReadResponse read() {
        List<LogEntry> committed = cluster.readCommitted();
        List<Dtos.EntryView> views = committed.stream()
                .map(e -> new Dtos.EntryView(e.term(), e.index(), e.requestId(), e.payload()))
                .toList();
        long commitIndex = cluster.leader().commitIndex();
        return new Dtos.ReadResponse(commitIndex, views);
    }
}
