package com.github.highcumontoa.quorumlogreplicationjava.tests;

import com.github.highcumontoa.quorumlogreplicationjava.raft.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.raft.RaftConfig;

import java.nio.file.Path;

/** 测试支撑：在临时目录内构建三副本集群。 */
final class ClusterTestSupport {

    private ClusterTestSupport() {
    }

    static RaftConfig config(Path dataDir) {
        RaftConfig config = new RaftConfig();
        config.setNodeId("node1");
        config.setNodes("node1,node2,node3");
        config.setDataDir(dataDir.toString());
        config.setMaxPayloadBytes(4096);
        config.setMaxUncommittedEntries(1000);
        config.setMaxLogEntries(10000);
        config.setCompactionRetain(100);
        return config;
    }

    static Cluster newCluster(Path dataDir) {
        Cluster cluster = new Cluster(config(dataDir));
        cluster.start();
        return cluster;
    }
}
