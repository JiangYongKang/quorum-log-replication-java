package com.github.highcumontoa.quorumlogreplicationjava.raft;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 可配置项：限流、回收与集群拓扑。 */
@ConfigurationProperties(prefix = "raft")
public class RaftConfig {

    /** 本节点标识。 */
    private String nodeId = "node1";
    /** 集群全部节点标识（逗号分隔）。 */
    private String nodes = "node1,node2,node3";
    /** 数据目录（每个节点一个子目录）。 */
    private String dataDir = "data";
    /** 单条 payload 最大字节数，超限拒绝。 */
    private int maxPayloadBytes = 4096;
    /** 未提交积压上限（条数），超限拒绝新写入。 */
    private int maxUncommittedEntries = 1000;
    /** 日志总条数上限，超限拒绝新写入（回收后仍超限才拒绝）。 */
    private int maxLogEntries = 10000;
    /** 已提交前缀回收：保留最近 N 条已提交条目，其余截断。 */
    private int compactionRetain = 100;

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public String getNodes() { return nodes; }
    public void setNodes(String nodes) { this.nodes = nodes; }
    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public int getMaxPayloadBytes() { return maxPayloadBytes; }
    public void setMaxPayloadBytes(int maxPayloadBytes) { this.maxPayloadBytes = maxPayloadBytes; }
    public int getMaxUncommittedEntries() { return maxUncommittedEntries; }
    public void setMaxUncommittedEntries(int maxUncommittedEntries) { this.maxUncommittedEntries = maxUncommittedEntries; }
    public int getMaxLogEntries() { return maxLogEntries; }
    public void setMaxLogEntries(int maxLogEntries) { this.maxLogEntries = maxLogEntries; }
    public int getCompactionRetain() { return compactionRetain; }
    public void setCompactionRetain(int compactionRetain) { this.compactionRetain = compactionRetain; }
}
