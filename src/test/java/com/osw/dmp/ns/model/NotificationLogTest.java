package com.osw.dmp.ns.model;

import com.osw.dmp.ns.model.NotificationLog.PushStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * NotificationLog 單元測試
 * 
 * 測試通知日誌模型
 */
@DisplayName("NotificationLog 單元測試")
class NotificationLogTest {

    @Nested
    @DisplayName("Builder 測試")
    class BuilderTests {

        @Test
        @DisplayName("應正確建構 NotificationLog")
        void shouldBuildNotificationLog() {
            // Given
            ResultPayload payload = ResultPayload.builder()
                    .status(StatusCode.SUCCESS)
                    .message("處理成功")
                    .build();

            long now = System.currentTimeMillis();

            // When
            NotificationLog log = NotificationLog.builder()
                    .eventId("event-123")
                    .pushStatus(PushStatus.PENDING)
                    .resultPayload(payload)
                    .attempts(0)
                    .createdAt(now)
                    .updatedAt(now)
                    .build();

            // Then
            assertThat(log.getEventId()).isEqualTo("event-123");
            assertThat(log.getPushStatus()).isEqualTo(PushStatus.PENDING);
            assertThat(log.getResultPayload()).isEqualTo(payload);
            assertThat(log.getAttempts()).isZero();
            assertThat(log.getCreatedAt()).isEqualTo(now);
            assertThat(log.getUpdatedAt()).isEqualTo(now);
        }
    }

    @Nested
    @DisplayName("generateKey 方法測試")
    class GenerateKeyTests {

        @Test
        @DisplayName("應正確生成快取 Key")
        void shouldGenerateCorrectKey() {
            // When
            String key = NotificationLog.generateKey("ns", "event-123");

            // Then
            assertThat(key).isEqualTo("ns:event-123");
        }

        @Test
        @DisplayName("不同 prefix 應生成不同 Key")
        void shouldGenerateDifferentKeysForDifferentPrefix() {
            // When
            String key1 = NotificationLog.generateKey("ns", "event-123");
            String key2 = NotificationLog.generateKey("app", "event-123");

            // Then
            assertThat(key1).isNotEqualTo(key2);
            assertThat(key1).isEqualTo("ns:event-123");
            assertThat(key2).isEqualTo("app:event-123");
        }

        @Test
        @DisplayName("空 prefix 應仍然生成 Key")
        void shouldGenerateKeyWithEmptyPrefix() {
            // When
            String key = NotificationLog.generateKey("", "event-123");

            // Then
            assertThat(key).isEqualTo(":event-123");
        }
    }

    @Nested
    @DisplayName("PushStatus 列舉測試")
    class PushStatusTests {

        @Test
        @DisplayName("應有三種狀態")
        void shouldHaveThreeStatuses() {
            assertThat(PushStatus.values()).hasSize(3);
            assertThat(PushStatus.values()).containsExactly(
                    PushStatus.PENDING,
                    PushStatus.SENT,
                    PushStatus.FAILED);
        }

        @Test
        @DisplayName("valueOf 應正確解析")
        void shouldParseFromString() {
            assertThat(PushStatus.valueOf("PENDING")).isEqualTo(PushStatus.PENDING);
            assertThat(PushStatus.valueOf("SENT")).isEqualTo(PushStatus.SENT);
            assertThat(PushStatus.valueOf("FAILED")).isEqualTo(PushStatus.FAILED);
        }
    }

    @Nested
    @DisplayName("狀態轉換測試")
    class StateTransitionTests {

        @Test
        @DisplayName("可以從 PENDING 轉為 SENT")
        void shouldTransitionFromPendingToSent() {
            // Given
            NotificationLog log = NotificationLog.builder()
                    .eventId("event-123")
                    .pushStatus(PushStatus.PENDING)
                    .build();

            // When
            log.setPushStatus(PushStatus.SENT);

            // Then
            assertThat(log.getPushStatus()).isEqualTo(PushStatus.SENT);
        }

        @Test
        @DisplayName("可以從 PENDING 轉為 FAILED")
        void shouldTransitionFromPendingToFailed() {
            // Given
            NotificationLog log = NotificationLog.builder()
                    .eventId("event-123")
                    .pushStatus(PushStatus.PENDING)
                    .build();

            // When
            log.setPushStatus(PushStatus.FAILED);

            // Then
            assertThat(log.getPushStatus()).isEqualTo(PushStatus.FAILED);
        }

        @Test
        @DisplayName("attempts 應可遞增")
        void shouldIncrementAttempts() {
            // Given
            NotificationLog log = NotificationLog.builder()
                    .eventId("event-123")
                    .attempts(0)
                    .build();

            // When
            log.setAttempts(log.getAttempts() + 1);
            log.setAttempts(log.getAttempts() + 1);
            log.setAttempts(log.getAttempts() + 1);

            // Then
            assertThat(log.getAttempts()).isEqualTo(3);
        }
    }
}
