package com.osw.dmp.ns.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 節點資訊
 * 用於 Ignite Cache 中追蹤叢集中的所有節點
 * 
 * Ignite Cache: ns-nodes
 * Key: nodeId
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NodeInfo implements Serializable {

    /**
     * 節點唯一識別碼
     */
    private String nodeId;

    /**
     * gRPC 服務地址 (host:port)
     */
    private String grpcAddress;

    /**
     * 節點啟動時間 (epoch millis)
     */
    private long startedAtMillis;

    /**
     * 最後心跳時間 (epoch millis)
     */
    private long lastHeartbeatMillis;

    /**
     * 節點狀態
     */
    private NodeStatus status;

    /**
     * 活躍連線數
     */
    private int activeConnections;

    /**
     * 活躍訂閱數
     */
    private int activeSubscriptions;

    /**
     * 檢查節點是否在線
     */
    public boolean isOnline() {
        return status == NodeStatus.ONLINE;
    }

    /**
     * 檢查節點是否可用於推送（ONLINE 或 SUSPECTED 都嘗試）
     */
    public boolean isAvailableForPush() {
        return status == NodeStatus.ONLINE || status == NodeStatus.SUSPECTED;
    }

    /**
     * 更新心跳時間
     */
    public void updateHeartbeat() {
        this.lastHeartbeatMillis = System.currentTimeMillis();
        this.status = NodeStatus.ONLINE;
    }
}
