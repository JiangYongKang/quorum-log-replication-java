package com.github.highcumontoa.quorumlogreplicationjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 可配置项。 */
@ConfigurationProperties(prefix = "replication")
public class ReplicationProperties {

    /** 日志保留的最大条目数，超过则回收已提交前缀。 */
    private long maxLogEntries = 1000;

    /** 数据目录。 */
    private String dataDir = "data";

    public long getMaxLogEntries() {
        return maxLogEntries;
    }

    public void setMaxLogEntries(long maxLogEntries) {
        this.maxLogEntries = maxLogEntries;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }
}
