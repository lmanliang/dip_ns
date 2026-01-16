package com.osw.dmp.ns.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 節點配置屬性
 * 
 * 用於配置節點身份識別與心跳相關參數
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "app.node")
public class NodeProperties {

    /**
     * 節點 ID（可透過環境變數設定）
     * 預設會根據 hostname 和 port 自動生成
     */
    private String id;

    /**
     * 心跳配置
     */
    private HeartbeatConfig heartbeat = new HeartbeatConfig();

    @Data
    public static class HeartbeatConfig {
        /**
         * 心跳間隔 (毫秒)
         */
        private long intervalMs = 10000; // 10 seconds

        /**
         * 超時判定時間 (毫秒) - 超過此時間無心跳則標記為 SUSPECTED
         */
        private long timeoutMs = 30000; // 30 seconds

        /**
         * 離線判定時間 (毫秒) - 超過此時間無心跳則標記為 OFFLINE
         */
        private long offlineMs = 60000; // 60 seconds
    }
}
