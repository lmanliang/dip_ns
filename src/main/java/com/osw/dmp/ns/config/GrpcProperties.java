package com.osw.dmp.ns.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * gRPC 配置屬性
 * 
 * 用於配置 gRPC Server/Client 相關參數
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "app.grpc")
public class GrpcProperties {

    /**
     * 是否啟用 gRPC Server
     */
    private boolean enabled = true;

    /**
     * gRPC Server 監聽埠
     */
    private int port = 9090;

    /**
     * gRPC Client 配置
     */
    private ClientConfig client = new ClientConfig();

    @Data
    public static class ClientConfig {
        /**
         * 請求超時時間 (毫秒)
         */
        private long timeoutMs = 5000;

        /**
         * 重試次數
         */
        private int retryCount = 2;

        /**
         * 重試延遲 (毫秒)
         */
        private long retryDelayMs = 500;

        /**
         * 連線池大小
         */
        private int poolSize = 10;

        /**
         * 連線閒置時間 (毫秒)
         */
        private long idleTimeoutMs = 300000; // 5 minutes
    }
}
