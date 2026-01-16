package com.osw.dmp.ns.health;

import com.osw.dmp.ns.ignite.IgniteCacheService;
import com.osw.dmp.ns.kafka.KafkaConsumerStatus;
import com.osw.dmp.ns.websocket.NotificationWebSocketHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Custom health indicators for Notification Service dependencies
 */
public class HealthIndicators {

    /**
     * Kafka health indicator
     */
    @Component
    @RequiredArgsConstructor
    public static class KafkaHealthIndicator implements HealthIndicator {

        private final KafkaConsumerStatus kafkaConsumer;

        @Override
        public Health health() {
            if (kafkaConsumer.isRunning()) {
                return Health.up()
                        .withDetail("status", "Consumer running")
                        .build();
            }
            return Health.down()
                    .withDetail("status", "Consumer not running")
                    .build();
        }
    }

    /**
     * Ignite health indicator
     */
    @Component
    @RequiredArgsConstructor
    public static class IgniteHealthIndicator implements HealthIndicator {

        private final IgniteCacheService igniteCache;

        @Override
        public Health health() {
            if (igniteCache.isConnected()) {
                return Health.up()
                        .withDetail("status", "Cache connected")
                        .withDetail("subscriptions", igniteCache.getTotalSubscriptionCount())
                        .build();
            }
            return Health.down()
                    .withDetail("status", "Cache disconnected")
                    .build();
        }
    }

    /**
     * WebSocket health indicator
     */
    @Component
    @RequiredArgsConstructor
    public static class WebSocketHealthIndicator implements HealthIndicator {

        private final NotificationWebSocketHandler webSocketHandler;

        @Override
        public Health health() {
            int connections = webSocketHandler.getSessionStore().getConnectionCount();
            int subscriptions = webSocketHandler.getSessionStore().getSubscriptionCount();

            return Health.up()
                    .withDetail("connections", connections)
                    .withDetail("subscriptions", subscriptions)
                    .withDetail("nodeId", webSocketHandler.getNodeId())
                    .build();
        }
    }
}
