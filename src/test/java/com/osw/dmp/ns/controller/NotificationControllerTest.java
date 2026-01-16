package com.osw.dmp.ns.controller;

import com.osw.dmp.ns.ignite.IgniteCacheService;
import com.osw.dmp.ns.kafka.KafkaConsumerStatus;
import com.osw.dmp.ns.kafka.SimulatedKafkaConsumer;
import com.osw.dmp.ns.websocket.LocalSessionStore;
import com.osw.dmp.ns.websocket.NotificationWebSocketHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import reactor.test.StepVerifier;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * NotificationController 單元測試
 * 
 * 測試 REST API 端點:
 * - Health check
 * - Readiness check
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("NotificationController 單元測試")
class NotificationControllerTest {

    @Mock
    private KafkaConsumerStatus kafkaConsumer;

    @Mock
    private IgniteCacheService igniteCache;

    @Mock
    private NotificationWebSocketHandler webSocketHandler;

    @Mock
    private SimulatedKafkaConsumer simulatedKafkaConsumer;

    @Mock
    private LocalSessionStore sessionStore;

    private NotificationController controller;

    @BeforeEach
    void setUp() {
        controller = new NotificationController(
                kafkaConsumer,
                igniteCache,
                webSocketHandler,
                simulatedKafkaConsumer);
    }

    // ==================== Health Check 測試 ====================

    @Nested
    @DisplayName("/api/health 端點")
    class HealthEndpointTests {

        @Test
        @DisplayName("應返回 UP 狀態")
        void shouldReturnUpStatus() {
            // When
            StepVerifier.create(controller.health())
                    .assertNext(response -> {
                        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                        assertThat(response.getBody()).containsEntry("status", "UP");
                        assertThat(response.getBody()).containsKey("timestamp");
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("timestamp 應為當前時間")
        void shouldContainCurrentTimestamp() {
            // Given
            long before = System.currentTimeMillis();

            // When
            StepVerifier.create(controller.health())
                    .assertNext(response -> {
                        long after = System.currentTimeMillis();
                        Long timestamp = (Long) response.getBody().get("timestamp");
                        assertThat(timestamp).isBetween(before, after);
                    })
                    .verifyComplete();
        }
    }

    // ==================== Readiness Check 測試 ====================

    @Nested
    @DisplayName("/api/ready 端點")
    class ReadinessEndpointTests {

        @Test
        @DisplayName("所有元件正常時應返回 UP")
        void shouldReturnUpWhenAllComponentsAreHealthy() {
            // Given
            when(kafkaConsumer.isRunning()).thenReturn(true);
            when(igniteCache.isConnected()).thenReturn(true);

            // When
            StepVerifier.create(controller.ready())
                    .assertNext(response -> {
                        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                        assertThat(response.getBody()).containsEntry("status", "UP");

                        @SuppressWarnings("unchecked")
                        Map<String, String> components = (Map<String, String>) response.getBody().get("components");
                        assertThat(components).containsEntry("kafka", "UP");
                        assertThat(components).containsEntry("ignite", "UP");
                        assertThat(components).containsEntry("websocket", "UP");
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("Kafka 未運行時應返回 DOWN 和 503")
        void shouldReturnDownWhenKafkaIsNotRunning() {
            // Given
            when(kafkaConsumer.isRunning()).thenReturn(false);
            when(igniteCache.isConnected()).thenReturn(true);

            // When
            StepVerifier.create(controller.ready())
                    .assertNext(response -> {
                        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                        assertThat(response.getBody()).containsEntry("status", "DOWN");

                        @SuppressWarnings("unchecked")
                        Map<String, String> components = (Map<String, String>) response.getBody().get("components");
                        assertThat(components).containsEntry("kafka", "DOWN");
                        assertThat(components).containsEntry("ignite", "UP");
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("Ignite 未連線時應返回 DOWN 和 503")
        void shouldReturnDownWhenIgniteIsDisconnected() {
            // Given
            when(kafkaConsumer.isRunning()).thenReturn(true);
            when(igniteCache.isConnected()).thenReturn(false);

            // When
            StepVerifier.create(controller.ready())
                    .assertNext(response -> {
                        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                        assertThat(response.getBody()).containsEntry("status", "DOWN");

                        @SuppressWarnings("unchecked")
                        Map<String, String> components = (Map<String, String>) response.getBody().get("components");
                        assertThat(components).containsEntry("kafka", "UP");
                        assertThat(components).containsEntry("ignite", "DOWN");
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("所有元件都 DOWN 時應返回 503")
        void shouldReturnDownWhenAllComponentsAreDown() {
            // Given
            when(kafkaConsumer.isRunning()).thenReturn(false);
            when(igniteCache.isConnected()).thenReturn(false);

            // When
            StepVerifier.create(controller.ready())
                    .assertNext(response -> {
                        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                        assertThat(response.getBody()).containsEntry("status", "DOWN");

                        @SuppressWarnings("unchecked")
                        Map<String, String> components = (Map<String, String>) response.getBody().get("components");
                        assertThat(components).containsEntry("kafka", "DOWN");
                        assertThat(components).containsEntry("ignite", "DOWN");
                        // WebSocket 總是 UP
                        assertThat(components).containsEntry("websocket", "UP");
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("timestamp 應包含在回應中")
        void shouldContainTimestamp() {
            // Given
            when(kafkaConsumer.isRunning()).thenReturn(true);
            when(igniteCache.isConnected()).thenReturn(true);

            // When
            StepVerifier.create(controller.ready())
                    .assertNext(response -> {
                        assertThat(response.getBody()).containsKey("timestamp");
                        assertThat(response.getBody().get("timestamp")).isInstanceOf(Long.class);
                    })
                    .verifyComplete();
        }
    }
}
