package com.osw.dmp.ns.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SubscriptionRouting 單元測試
 * 
 * 測試訂閱路由模型
 */
@DisplayName("SubscriptionRouting 單元測試")
class SubscriptionRoutingTest {

    @Nested
    @DisplayName("Builder 測試")
    class BuilderTests {

        @Test
        @DisplayName("應正確建構 SubscriptionRouting")
        void shouldBuildSubscriptionRouting() {
            // Given
            long now = System.currentTimeMillis();

            // When
            SubscriptionRouting routing = SubscriptionRouting.builder()
                    .eventId("event-123")
                    .nodeId("node-001")
                    .sessionId("session-001")
                    .subscribedAtMillis(now)
                    .build();

            // Then
            assertThat(routing.getEventId()).isEqualTo("event-123");
            assertThat(routing.getNodeId()).isEqualTo("node-001");
            assertThat(routing.getSessionId()).isEqualTo("session-001");
            assertThat(routing.getSubscribedAtMillis()).isEqualTo(now);
        }

        @Test
        @DisplayName("應支援 NoArgsConstructor")
        void shouldSupportNoArgsConstructor() {
            // When
            SubscriptionRouting routing = new SubscriptionRouting();
            routing.setEventId("event-123");
            routing.setNodeId("node-001");

            // Then
            assertThat(routing.getEventId()).isEqualTo("event-123");
            assertThat(routing.getNodeId()).isEqualTo("node-001");
        }

        @Test
        @DisplayName("應支援 AllArgsConstructor")
        void shouldSupportAllArgsConstructor() {
            // When
            long now = System.currentTimeMillis();
            SubscriptionRouting routing = new SubscriptionRouting(
                    "event-123", "node-001", "localhost:9090", "session-001", now);

            // Then
            assertThat(routing.getEventId()).isEqualTo("event-123");
            assertThat(routing.getNodeId()).isEqualTo("node-001");
            assertThat(routing.getGrpcAddress()).isEqualTo("localhost:9090");
            assertThat(routing.getSessionId()).isEqualTo("session-001");
            assertThat(routing.getSubscribedAtMillis()).isEqualTo(now);
        }
    }

    @Nested
    @DisplayName("Equals 與 HashCode 測試")
    class EqualsHashCodeTests {

        @Test
        @DisplayName("相同內容的 SubscriptionRouting 應相等")
        void shouldBeEqualForSameContent() {
            long now = System.currentTimeMillis();

            SubscriptionRouting routing1 = SubscriptionRouting.builder()
                    .eventId("event-123")
                    .nodeId("node-001")
                    .sessionId("session-001")
                    .subscribedAtMillis(now)
                    .build();

            SubscriptionRouting routing2 = SubscriptionRouting.builder()
                    .eventId("event-123")
                    .nodeId("node-001")
                    .sessionId("session-001")
                    .subscribedAtMillis(now)
                    .build();

            assertThat(routing1).isEqualTo(routing2);
            assertThat(routing1.hashCode()).isEqualTo(routing2.hashCode());
        }

        @Test
        @DisplayName("不同 eventId 的 SubscriptionRouting 不應相等")
        void shouldNotBeEqualForDifferentEventId() {
            SubscriptionRouting routing1 = SubscriptionRouting.builder()
                    .eventId("event-123")
                    .nodeId("node-001")
                    .build();

            SubscriptionRouting routing2 = SubscriptionRouting.builder()
                    .eventId("event-456")
                    .nodeId("node-001")
                    .build();

            assertThat(routing1).isNotEqualTo(routing2);
        }

        @Test
        @DisplayName("不同 nodeId 的 SubscriptionRouting 不應相等")
        void shouldNotBeEqualForDifferentNodeId() {
            SubscriptionRouting routing1 = SubscriptionRouting.builder()
                    .eventId("event-123")
                    .nodeId("node-001")
                    .build();

            SubscriptionRouting routing2 = SubscriptionRouting.builder()
                    .eventId("event-123")
                    .nodeId("node-002")
                    .build();

            assertThat(routing1).isNotEqualTo(routing2);
        }
    }

    @Nested
    @DisplayName("Serializable 測試")
    class SerializableTests {

        @Test
        @DisplayName("應實作 Serializable 介面")
        void shouldImplementSerializable() {
            SubscriptionRouting routing = SubscriptionRouting.builder()
                    .eventId("event-123")
                    .build();

            assertThat(routing).isInstanceOf(java.io.Serializable.class);
        }
    }

    @Nested
    @DisplayName("時間戳測試")
    class TimestampTests {

        @Test
        @DisplayName("subscribedAtMillis 應使用 epoch millis")
        void shouldUseEpochMillis() {
            // Given
            long expectedMillis = 1704067200000L; // 2024-01-01 00:00:00 UTC

            // When
            SubscriptionRouting routing = SubscriptionRouting.builder()
                    .eventId("event-123")
                    .subscribedAtMillis(expectedMillis)
                    .build();

            // Then
            assertThat(routing.getSubscribedAtMillis()).isEqualTo(expectedMillis);
        }

        @Test
        @DisplayName("可以計算訂閱時長")
        void shouldCalculateSubscriptionDuration() {
            // Given
            long subscribedAt = System.currentTimeMillis() - 5000; // 5 秒前
            SubscriptionRouting routing = SubscriptionRouting.builder()
                    .eventId("event-123")
                    .subscribedAtMillis(subscribedAt)
                    .build();

            // When
            long duration = System.currentTimeMillis() - routing.getSubscribedAtMillis();

            // Then
            assertThat(duration).isGreaterThanOrEqualTo(5000);
            assertThat(duration).isLessThan(6000); // 給一些誤差
        }
    }
}
