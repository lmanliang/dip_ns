package com.osw.dmp.ns.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Ignite Configuration Properties
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "app.ignite")
public class IgniteConfig {

    /**
     * Ignite thin client addresses (comma-separated)
     * Example: localhost:10800,192.168.1.100:10800
     */
    private String addresses = "localhost:10800";

    /**
     * Cluster name
     */
    private String clusterName = "nhi-cluster";

    /**
     * Key prefix for cache entries (e.g., environment or service identifier)
     */
    private String keyPrefix = "ns";

    /**
     * Subscription routing cache configuration
     */
    private CacheConfig subscriptionRouting = new CacheConfig();

    /**
     * Notification log cache configuration
     */
    private CacheConfig notificationLog = new CacheConfig();

    /**
     * Messaging topic for cross-node communication
     */
    private MessagingConfig messaging = new MessagingConfig();

    @Data
    public static class CacheConfig {
        private String name;
        private int backups = 1;
        private int expirySeconds = 300;
    }

    @Data
    public static class MessagingConfig {
        private String topic = "ns-notifications";
    }

    /**
     * Get cache config for subscription routing
     */
    public CacheConfig getCache() {
        return subscriptionRouting;
    }
}
