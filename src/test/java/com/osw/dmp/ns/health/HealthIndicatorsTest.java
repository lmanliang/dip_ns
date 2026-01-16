package com.osw.dmp.ns.health;

import com.osw.dmp.ns.health.HealthIndicators.*;
import com.osw.dmp.ns.ignite.IgniteCacheService;
import com.osw.dmp.ns.kafka.KafkaConsumerStatus;
import com.osw.dmp.ns.websocket.LocalSessionStore;
import com.osw.dmp.ns.websocket.NotificationWebSocketHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * HealthIndicators 單元測試
 * 
 * 測試各個健康檢查指標:
 * - KafkaHealthIndicator
 * - IgniteHealthIndicator
 * - WebSocketHealthIndicator
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("HealthIndicators 單元測試")
class HealthIndicatorsTest {

    // ==================== KafkaHealthIndicator 測試 ====================

    @Nested
    @DisplayName("KafkaHealthIndicator 測試")
    class KafkaHealthIndicatorTests {

        @Mock
        private KafkaConsumerStatus kafkaConsumer;

        @InjectMocks
        private KafkaHealthIndicator healthIndicator;

        @Test
        @DisplayName("Kafka Consumer 運行中應返回 UP")
        void shouldReturnUpWhenKafkaIsRunning() {
            // Given
            when(kafkaConsumer.isRunning()).thenReturn(true);

            // When
            Health health = healthIndicator.health();

            // Then
            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsEntry("status", "Consumer running");
        }

        @Test
        @DisplayName("Kafka Consumer 未運行應返回 DOWN")
        void shouldReturnDownWhenKafkaIsNotRunning() {
            // Given
            when(kafkaConsumer.isRunning()).thenReturn(false);

            // When
            Health health = healthIndicator.health();

            // Then
            assertThat(health.getStatus()).isEqualTo(Status.DOWN);
            assertThat(health.getDetails()).containsEntry("status", "Consumer not running");
        }
    }

    // ==================== IgniteHealthIndicator 測試 ====================

    @Nested
    @DisplayName("IgniteHealthIndicator 測試")
    class IgniteHealthIndicatorTests {

        @Mock
        private IgniteCacheService igniteCache;

        @InjectMocks
        private IgniteHealthIndicator healthIndicator;

        @Test
        @DisplayName("Ignite 已連線應返回 UP 並包含訂閱數")
        void shouldReturnUpWhenIgniteIsConnected() {
            // Given
            when(igniteCache.isConnected()).thenReturn(true);
            when(igniteCache.getTotalSubscriptionCount()).thenReturn(100);

            // When
            Health health = healthIndicator.health();

            // Then
            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsEntry("status", "Cache connected");
            assertThat(health.getDetails()).containsEntry("subscriptions", 100);
        }

        @Test
        @DisplayName("Ignite 未連線應返回 DOWN")
        void shouldReturnDownWhenIgniteIsDisconnected() {
            // Given
            when(igniteCache.isConnected()).thenReturn(false);

            // When
            Health health = healthIndicator.health();

            // Then
            assertThat(health.getStatus()).isEqualTo(Status.DOWN);
            assertThat(health.getDetails()).containsEntry("status", "Cache disconnected");
        }

        @Test
        @DisplayName("零訂閱時應正確顯示")
        void shouldShowZeroSubscriptions() {
            // Given
            when(igniteCache.isConnected()).thenReturn(true);
            when(igniteCache.getTotalSubscriptionCount()).thenReturn(0);

            // When
            Health health = healthIndicator.health();

            // Then
            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsEntry("subscriptions", 0);
        }
    }

    // ==================== WebSocketHealthIndicator 測試 ====================

    @Nested
    @DisplayName("WebSocketHealthIndicator 測試")
    class WebSocketHealthIndicatorTests {

        @Mock
        private NotificationWebSocketHandler webSocketHandler;

        @Mock
        private LocalSessionStore sessionStore;

        @InjectMocks
        private WebSocketHealthIndicator healthIndicator;

        @Test
        @DisplayName("應返回 UP 並包含連線與訂閱資訊")
        void shouldReturnUpWithConnectionInfo() {
            // Given
            when(webSocketHandler.getSessionStore()).thenReturn(sessionStore);
            when(webSocketHandler.getNodeId()).thenReturn("node-001");
            when(sessionStore.getConnectionCount()).thenReturn(50);
            when(sessionStore.getSubscriptionCount()).thenReturn(100);

            // When
            Health health = healthIndicator.health();

            // Then
            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsEntry("connections", 50);
            assertThat(health.getDetails()).containsEntry("subscriptions", 100);
            assertThat(health.getDetails()).containsEntry("nodeId", "node-001");
        }

        @Test
        @DisplayName("零連線時應正確顯示")
        void shouldShowZeroConnections() {
            // Given
            when(webSocketHandler.getSessionStore()).thenReturn(sessionStore);
            when(webSocketHandler.getNodeId()).thenReturn("node-001");
            when(sessionStore.getConnectionCount()).thenReturn(0);
            when(sessionStore.getSubscriptionCount()).thenReturn(0);

            // When
            Health health = healthIndicator.health();

            // Then
            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsEntry("connections", 0);
            assertThat(health.getDetails()).containsEntry("subscriptions", 0);
        }

        @Test
        @DisplayName("高連線數時應正確顯示")
        void shouldShowHighConnectionCount() {
            // Given
            when(webSocketHandler.getSessionStore()).thenReturn(sessionStore);
            when(webSocketHandler.getNodeId()).thenReturn("node-001");
            when(sessionStore.getConnectionCount()).thenReturn(10000);
            when(sessionStore.getSubscriptionCount()).thenReturn(15000);

            // When
            Health health = healthIndicator.health();

            // Then
            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsEntry("connections", 10000);
            assertThat(health.getDetails()).containsEntry("subscriptions", 15000);
        }
    }
}
