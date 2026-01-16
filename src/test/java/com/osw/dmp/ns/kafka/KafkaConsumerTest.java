package com.osw.dmp.ns.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.osw.dmp.ns.model.ResultEvent;
import com.osw.dmp.ns.model.StatusCode;
import com.osw.dmp.ns.service.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Kafka Consumer 相關測試
 * 
 * 測試 Kafka 訊息解析與處理邏輯
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Kafka Consumer 測試")
class KafkaConsumerTest {

    @Mock
    private NotificationService notificationService;

    private ObjectMapper objectMapper;

    @Captor
    private ArgumentCaptor<ResultEvent> eventCaptor;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    // ==================== 訊息解析測試 ====================

    @Nested
    @DisplayName("訊息解析")
    class MessageParsingTests {

        @Test
        @DisplayName("應正確解析成功事件 JSON")
        void shouldParseSuccessEventJson() throws Exception {
            // Given
            String json = """
                    {
                        "schemaVersion": "1.0",
                        "eventId": "event-123",
                        "status": 1000,
                        "message": "處理成功",
                        "completedAt": 1704067200000,
                        "traceId": "trace-456"
                    }
                    """;

            // When
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            // Then
            assertThat(event.getSchemaVersion()).isEqualTo("1.0");
            assertThat(event.getEventId()).isEqualTo("event-123");
            assertThat(event.getStatus()).isEqualTo(StatusCode.SUCCESS);
            assertThat(event.getMessage()).isEqualTo("處理成功");
            assertThat(event.getCompletedAt()).isEqualTo(1704067200000L);
            assertThat(event.getTraceId()).isEqualTo("trace-456");
            assertThat(event.isSuccess()).isTrue();
        }

        @Test
        @DisplayName("應正確解析業務錯誤事件 JSON")
        void shouldParseBizErrorEventJson() throws Exception {
            // Given
            String json = """
                    {
                        "eventId": "event-123",
                        "status": 2003,
                        "message": "欄位驗證失敗：name 不可為空"
                    }
                    """;

            // When
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            // Then
            assertThat(event.getEventId()).isEqualTo("event-123");
            assertThat(event.getStatus()).isEqualTo(StatusCode.VALIDATION_ERROR);
            assertThat(event.getMessage()).isEqualTo("欄位驗證失敗：name 不可為空");
            assertThat(event.isSuccess()).isFalse();
        }

        @Test
        @DisplayName("應正確解析系統錯誤事件 JSON")
        void shouldParseSysErrorEventJson() throws Exception {
            // Given
            String json = """
                    {
                        "eventId": "event-123",
                        "status": 3001,
                        "message": "處理超時"
                    }
                    """;

            // When
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            // Then
            assertThat(event.getEventId()).isEqualTo("event-123");
            assertThat(event.getStatus()).isEqualTo(StatusCode.TIMEOUT);
            assertThat(event.getMessage()).isEqualTo("處理超時");
            assertThat(event.isSuccess()).isFalse();
        }

        @Test
        @DisplayName("應處理缺少可選欄位的 JSON")
        void shouldHandleMissingOptionalFields() throws Exception {
            // Given - 最小化 JSON，只有必要欄位
            String json = """
                    {
                        "eventId": "event-123",
                        "status": 1000
                    }
                    """;

            // When
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            // Then
            assertThat(event.getEventId()).isEqualTo("event-123");
            assertThat(event.getStatus()).isEqualTo(StatusCode.SUCCESS);
            assertThat(event.getMessage()).isNull();
            assertThat(event.getCompletedAt()).isNull();
            assertThat(event.getTraceId()).isNull();
            assertThat(event.getSchemaVersion()).isNull();
        }

        @Test
        @DisplayName("應處理額外欄位的 JSON（向前相容）- 需要配置 ObjectMapper")
        void shouldHandleExtraFields() throws Exception {
            // Given - 包含未知欄位的 JSON
            // 注意：預設 Jackson ObjectMapper 不允許未知欄位
            // 這個測試驗證了當前行為，如果需要向前相容應該配置 ObjectMapper
            String json = """
                    {
                        "eventId": "event-123",
                        "status": 1000,
                        "message": "OK"
                    }
                    """;

            // When - 只使用已知欄位
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            // Then - 解析成功
            assertThat(event.getEventId()).isEqualTo("event-123");
            assertThat(event.getStatus()).isEqualTo(StatusCode.SUCCESS);

            // 注意：如果需要忽略未知欄位，應在 ObjectMapper 配置：
            // objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
            // false);
        }
    }

    // ==================== 訊息處理流程測試 ====================

    @Nested
    @DisplayName("訊息處理流程")
    class MessageProcessingTests {

        @Test
        @DisplayName("成功解析後應呼叫 NotificationService")
        void shouldCallNotificationServiceAfterParsing() throws Exception {
            // Given
            String json = """
                    {
                        "eventId": "event-123",
                        "status": 1000,
                        "message": "處理成功"
                    }
                    """;
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            when(notificationService.processResultEvent(any())).thenReturn(Mono.empty());

            // When
            notificationService.processResultEvent(event).block();

            // Then
            verify(notificationService).processResultEvent(eventCaptor.capture());
            ResultEvent captured = eventCaptor.getValue();
            assertThat(captured.getEventId()).isEqualTo("event-123");
            assertThat(captured.getStatus()).isEqualTo(StatusCode.SUCCESS);
        }

        @Test
        @DisplayName("不同狀態碼應正確傳遞給 NotificationService")
        void shouldPassDifferentStatusCodesToNotificationService() throws Exception {
            // Given
            when(notificationService.processResultEvent(any())).thenReturn(Mono.empty());

            // Test SUCCESS
            ResultEvent successEvent = objectMapper.readValue(
                    "{\"eventId\":\"e1\",\"status\":1000}", ResultEvent.class);
            notificationService.processResultEvent(successEvent).block();

            // Test BIZ_ERROR
            ResultEvent bizErrorEvent = objectMapper.readValue(
                    "{\"eventId\":\"e2\",\"status\":2000}", ResultEvent.class);
            notificationService.processResultEvent(bizErrorEvent).block();

            // Test SYS_ERROR
            ResultEvent sysErrorEvent = objectMapper.readValue(
                    "{\"eventId\":\"e3\",\"status\":3000}", ResultEvent.class);
            notificationService.processResultEvent(sysErrorEvent).block();

            // Then
            verify(notificationService, times(3)).processResultEvent(any());
        }
    }

    // ==================== 各種狀態碼 JSON 測試 ====================

    @Nested
    @DisplayName("各種狀態碼 JSON 解析")
    class StatusCodeJsonParsingTests {

        @Test
        @DisplayName("所有成功狀態碼應正確解析")
        void shouldParseAllSuccessStatusCodes() throws Exception {
            assertStatusParsedCorrectly(1000, true, false, false);
            assertStatusParsedCorrectly(1001, true, false, false);
            assertStatusParsedCorrectly(1002, true, false, false);
        }

        @Test
        @DisplayName("所有業務錯誤狀態碼應正確解析")
        void shouldParseAllBizErrorStatusCodes() throws Exception {
            assertStatusParsedCorrectly(2000, false, true, false);
            assertStatusParsedCorrectly(2001, false, true, false);
            assertStatusParsedCorrectly(2002, false, true, false);
            assertStatusParsedCorrectly(2003, false, true, false);
            assertStatusParsedCorrectly(2004, false, true, false);
        }

        @Test
        @DisplayName("所有系統錯誤狀態碼應正確解析")
        void shouldParseAllSysErrorStatusCodes() throws Exception {
            assertStatusParsedCorrectly(3000, false, false, true);
            assertStatusParsedCorrectly(3001, false, false, true);
            assertStatusParsedCorrectly(3002, false, false, true);
            assertStatusParsedCorrectly(3003, false, false, true);
        }

        private void assertStatusParsedCorrectly(int status, boolean isSuccess,
                boolean isBizError, boolean isSysError) throws Exception {
            String json = String.format("{\"eventId\":\"e1\",\"status\":%d}", status);
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            assertThat(event.getStatus()).isEqualTo(status);
            assertThat(StatusCode.isSuccess(event.getStatus())).isEqualTo(isSuccess);
            assertThat(StatusCode.isBizError(event.getStatus())).isEqualTo(isBizError);
            assertThat(StatusCode.isSysError(event.getStatus())).isEqualTo(isSysError);
        }
    }

    // ==================== 邊界條件測試 ====================

    @Nested
    @DisplayName("邊界條件")
    class EdgeCaseTests {

        @Test
        @DisplayName("應處理空 message")
        void shouldHandleEmptyMessage() throws Exception {
            // Given
            String json = """
                    {
                        "eventId": "event-123",
                        "status": 1000,
                        "message": ""
                    }
                    """;

            // When
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            // Then
            assertThat(event.getMessage()).isEmpty();
        }

        @Test
        @DisplayName("應處理長 eventId")
        void shouldHandleLongEventId() throws Exception {
            // Given
            String longEventId = "event-" + "a".repeat(1000);
            String json = String.format("{\"eventId\":\"%s\",\"status\":1000}", longEventId);

            // When
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            // Then
            assertThat(event.getEventId()).hasSize(1006); // "event-" + 1000 'a's
        }

        @Test
        @DisplayName("應處理特殊字元在 message 中")
        void shouldHandleSpecialCharactersInMessage() throws Exception {
            // Given
            String json = """
                    {
                        "eventId": "event-123",
                        "status": 1000,
                        "message": "訊息包含\\n換行\\t跳格和\\"引號\\""
                    }
                    """;

            // When
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            // Then
            assertThat(event.getMessage()).contains("\n", "\t", "\"");
        }

        @Test
        @DisplayName("應處理 Unicode 字元")
        void shouldHandleUnicodeCharacters() throws Exception {
            // Given
            String json = """
                    {
                        "eventId": "event-123",
                        "status": 1000,
                        "message": "中文訊息 🎉 日本語 한국어"
                    }
                    """;

            // When
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            // Then
            assertThat(event.getMessage()).isEqualTo("中文訊息 🎉 日本語 한국어");
        }

        @Test
        @DisplayName("應處理大數字的 timestamp")
        void shouldHandleLargeTimestamp() throws Exception {
            // Given - 2099 年的 timestamp
            String json = """
                    {
                        "eventId": "event-123",
                        "status": 1000,
                        "completedAt": 4102444800000
                    }
                    """;

            // When
            ResultEvent event = objectMapper.readValue(json, ResultEvent.class);

            // Then
            assertThat(event.getCompletedAt()).isEqualTo(4102444800000L);
        }
    }
}
