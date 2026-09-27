package com.github.highcumontoa.quorumlogreplicationjava.config;

import com.github.highcumontoa.quorumlogreplicationjava.cluster.Cluster;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.List;

/** 集群装配：本机三副本，数据目录与保留策略来自配置。 */
@Configuration
@EnableConfigurationProperties(ReplicationProperties.class)
public class ClusterConfig {

    @Bean(destroyMethod = "close")
    public Cluster cluster(ReplicationProperties props) {
        return new Cluster(List.of("n1", "n2", "n3"),
                Path.of(props.getDataDir()), props.getMaxLogEntries(), props.getMaxEntryBytes());
    }
}
