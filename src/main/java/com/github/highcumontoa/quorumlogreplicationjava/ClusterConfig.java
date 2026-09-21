package com.github.highcumontoa.quorumlogreplicationjava;

import com.github.highcumontoa.quorumlogreplicationjava.raft.Cluster;
import com.github.highcumontoa.quorumlogreplicationjava.raft.RaftConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 装配集群 Bean。 */
@Configuration
@EnableConfigurationProperties(RaftConfig.class)
public class ClusterConfig {

    @Bean
    public Cluster cluster(RaftConfig config) {
        Cluster cluster = new Cluster(config);
        cluster.start();
        return cluster;
    }
}
