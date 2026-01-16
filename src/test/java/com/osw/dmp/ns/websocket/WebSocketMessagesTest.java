package com.osw.dmp.ns.websocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.osw.dmp.ns.model.StatusCode;
import com.osw.dmp.ns.websocket.WebSocketMessages.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WebSocketMessages 單元測試
 * 
 * 測試 WebSocket 訊息模型的序列化與反序列化
 */
@DisplayName("WebSocketMessages 單元測試")
class WebSocketMessagesTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    // ==================== Client → Server 訊息測試 ====================

    @Nested
    @DisplayName("SubscribeRequest 測試")
    class SubscribeRequestTests {

        @Test
        @DisplayName("應正確序列化 SubscribeRequest")
        void shouldSerializeSubscribeRequest() throws JsonProcessingException {
            // Given
            SubscribeRequest request = SubscribeRequest.builder()
                    .type("subscribe")
                    .eventId("event-123")
                    .build();

            // When
            String json = objectMapper.writeValueAsString(request);

            // Then
            assertThat(json).contains("\"type\":\"subscribe\"");
            assertThat(json).contains("\"eventId\":\"event-123\"");
        }

        @Test
        @DisplayName("應正確反序列化 SubscribeRequest")
        void shouldDeserializeSubscribeRequest() throws JsonProcessingException {
            // Given
            String json = "{\"type\":\"subscribe\",\"eventId\":\"event-123\"}";

            // When
            SubscribeRequest request = objectMapper.readValue(json, SubscribeRequest.class);

            // Then
            assertThat(request.getType()).isEqualTo("subscribe");
            assertThat(request.getEventId()).isEqualTo("event-123");
        }
    }

    @Nested
    @DisplayName("UnsubscribeRequest 測試")
    class UnsubscribeRequestTests {

        @Test
        @DisplayName("應正確序列化 UnsubscribeRequest")
        void shouldSerializeUnsubscribeRequest() throws JsonProcessingException {
            // Given
            UnsubscribeRequest request = UnsubscribeRequest.builder()
                    .type("unsubscribe")
                    .eventId("event-123")
                    .build();

            // When
            String json = objectMapper.writeValueAsString(request);

            // Then
            assertThat(json).contains("\"type\":\"unsubscribe\"");
            assertThat(json).contains("\"eventId\":\"event-123\"");
        }

        @Test
        @DisplayName("應正確反序列化 UnsubscribeRequest")
        void shouldDeserializeUnsubscribeRequest() throws JsonProcessingException {
            // Given
            String json = "{\"type\":\"unsubscribe\",\"eventId\":\"event-123\"}";

            // When
            UnsubscribeRequest request = objectMapper.readValue(json, UnsubscribeRequest.class);

            // Then
            assertThat(request.getType()).isEqualTo("unsubscribe");
            assertThat(request.getEventId()).isEqualTo("event-123");
        }
    }

    @Nested
    @DisplayName("PingMessage 測試")
    class PingMessageTests {

        @Test
        @DisplayName("應正確序列化 PingMessage")
        void shouldSerializePingMessage() throws JsonProcessingException {
            // Given
            PingMessage ping = PingMessage.builder()
                    .type("ping")
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(ping);

            // Then
            assertThat(json).contains("\"type\":\"ping\"");
            assertThat(json).contains("\"timestamp\":1704067200000");
        }

        @Test
        @DisplayName("應正確反序列化 PingMessage")
        void shouldDeserializePingMessage() throws JsonProcessingException {
            // Given
            String json = "{\"type\":\"ping\",\"timestamp\":1704067200000}";

            // When
            PingMessage ping = objectMapper.readValue(json, PingMessage.class);

            // Then
            assertThat(ping.getType()).isEqualTo("ping");
            assertThat(ping.getTimestamp()).isEqualTo(1704067200000L);
        }
    }

    // ==================== Server → Client 訊息測試 ====================

    @Nested
    @DisplayName("SubscribedResponse 測試")
    class SubscribedResponseTests {

        @Test
        @DisplayName("應正確序列化 SubscribedResponse")
        void shouldSerializeSubscribedResponse() throws JsonProcessingException {
            // Given
            SubscribedResponse response = SubscribedResponse.builder()
                    .eventId("event-123")
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(response);

            // Then
            assertThat(json).contains("\"type\":\"subscribed\"");
            assertThat(json).contains("\"eventId\":\"event-123\"");
            assertThat(json).contains("\"timestamp\":1704067200000");
        }

        @Test
        @DisplayName("type 應預設為 subscribed")
        void shouldHaveDefaultType() {
            // When
            SubscribedResponse response = SubscribedResponse.builder()
                    .eventId("event-123")
                    .build();

            // Then
            assertThat(response.getType()).isEqualTo("subscribed");
        }
    }

    @Nested
    @DisplayName("UnsubscribedResponse 測試")
    class UnsubscribedResponseTests {

        @Test
        @DisplayName("應正確序列化 UnsubscribedResponse")
        void shouldSerializeUnsubscribedResponse() throws JsonProcessingException {
            // Given
            UnsubscribedResponse response = UnsubscribedResponse.builder()
                    .eventId("event-123")
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(response);

            // Then
            assertThat(json).contains("\"type\":\"unsubscribed\"");
            assertThat(json).contains("\"eventId\":\"event-123\"");
        }

        @Test
        @DisplayName("type 應預設為 unsubscribed")
        void shouldHaveDefaultType() {
            // When
            UnsubscribedResponse response = UnsubscribedResponse.builder()
                    .eventId("event-123")
                    .build();

            // Then
            assertThat(response.getType()).isEqualTo("unsubscribed");
        }
    }

    @Nested
    @DisplayName("NotificationMessage 測試")
    class NotificationMessageTests {

        @Test
        @DisplayName("應正確序列化成功通知")
        void shouldSerializeSuccessNotification() throws JsonProcessingException {
            // Given
            NotificationMessage notification = NotificationMessage.builder()
                    .eventId("event-123")
                    .status(StatusCode.SUCCESS)
                    .message("處理成功")
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(notification);

            // Then
            assertThat(json).contains("\"type\":\"notification\"");
            assertThat(json).contains("\"eventId\":\"event-123\"");
            assertThat(json).contains("\"status\":1000");
            assertThat(json).contains("\"message\":\"處理成功\"");
            assertThat(json).contains("\"timestamp\":1704067200000");
        }

        @Test
        @DisplayName("應正確序列化業務錯誤通知")
        void shouldSerializeBizErrorNotification() throws JsonProcessingException {
            // Given
            NotificationMessage notification = NotificationMessage.builder()
                    .eventId("event-123")
                    .status(StatusCode.VALIDATION_ERROR)
                    .message("欄位驗證失敗")
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(notification);

            // Then
            assertThat(json).contains("\"status\":2003");
            assertThat(json).contains("\"message\":\"欄位驗證失敗\"");
        }

        @Test
        @DisplayName("應正確序列化系統錯誤通知")
        void shouldSerializeSysErrorNotification() throws JsonProcessingException {
            // Given
            NotificationMessage notification = NotificationMessage.builder()
                    .eventId("event-123")
                    .status(StatusCode.TIMEOUT)
                    .message("處理超時")
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(notification);

            // Then
            assertThat(json).contains("\"status\":3001");
            assertThat(json).contains("\"message\":\"處理超時\"");
        }

        @Test
        @DisplayName("type 應預設為 notification")
        void shouldHaveDefaultType() {
            // When
            NotificationMessage notification = NotificationMessage.builder()
                    .eventId("event-123")
                    .status(StatusCode.SUCCESS)
                    .build();

            // Then
            assertThat(notification.getType()).isEqualTo("notification");
        }

        @Test
        @DisplayName("null message 不應包含在 JSON 中")
        void shouldExcludeNullMessage() throws JsonProcessingException {
            // Given
            NotificationMessage notification = NotificationMessage.builder()
                    .eventId("event-123")
                    .status(StatusCode.SUCCESS)
                    .message(null)
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(notification);

            // Then
            assertThat(json).doesNotContain("\"message\"");
        }

        @Test
        @DisplayName("應正確反序列化 NotificationMessage")
        void shouldDeserializeNotificationMessage() throws JsonProcessingException {
            // Given
            String json = "{\"type\":\"notification\",\"eventId\":\"event-123\",\"status\":1000,\"message\":\"OK\",\"timestamp\":1704067200000}";

            // When
            NotificationMessage notification = objectMapper.readValue(json, NotificationMessage.class);

            // Then
            assertThat(notification.getType()).isEqualTo("notification");
            assertThat(notification.getEventId()).isEqualTo("event-123");
            assertThat(notification.getStatus()).isEqualTo(StatusCode.SUCCESS);
            assertThat(notification.getMessage()).isEqualTo("OK");
            assertThat(notification.getTimestamp()).isEqualTo(1704067200000L);
        }
    }

    @Nested
    @DisplayName("PongMessage 測試")
    class PongMessageTests {

        @Test
        @DisplayName("應正確序列化 PongMessage")
        void shouldSerializePongMessage() throws JsonProcessingException {
            // Given
            PongMessage pong = PongMessage.builder()
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(pong);

            // Then
            assertThat(json).contains("\"type\":\"pong\"");
            assertThat(json).contains("\"timestamp\":1704067200000");
        }

        @Test
        @DisplayName("type 應預設為 pong")
        void shouldHaveDefaultType() {
            // When
            PongMessage pong = PongMessage.builder()
                    .timestamp(System.currentTimeMillis())
                    .build();

            // Then
            assertThat(pong.getType()).isEqualTo("pong");
        }
    }

    @Nested
    @DisplayName("ErrorMessage 測試")
    class ErrorMessageTests {

        @Test
        @DisplayName("應正確序列化 ErrorMessage")
        void shouldSerializeErrorMessage() throws JsonProcessingException {
            // Given
            ErrorMessage error = ErrorMessage.builder()
                    .code("INVALID_EVENT_ID")
                    .message("eventId is required")
                    .eventId("event-123")
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(error);

            // Then
            assertThat(json).contains("\"type\":\"error\"");
            assertThat(json).contains("\"code\":\"INVALID_EVENT_ID\"");
            assertThat(json).contains("\"message\":\"eventId is required\"");
            assertThat(json).contains("\"eventId\":\"event-123\"");
        }

        @Test
        @DisplayName("type 應預設為 error")
        void shouldHaveDefaultType() {
            // When
            ErrorMessage error = ErrorMessage.builder()
                    .code("TEST")
                    .message("test")
                    .build();

            // Then
            assertThat(error.getType()).isEqualTo("error");
        }

        @Test
        @DisplayName("null eventId 不應包含在 JSON 中")
        void shouldExcludeNullEventId() throws JsonProcessingException {
            // Given
            ErrorMessage error = ErrorMessage.builder()
                    .code("ERROR")
                    .message("Error occurred")
                    .eventId(null)
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(error);

            // Then
            assertThat(json).doesNotContain("\"eventId\"");
        }
    }

    // ==================== 邊界條件測試 ====================

    @Nested
    @DisplayName("邊界條件")
    class EdgeCaseTests {

        @Test
        @DisplayName("應處理特殊字元")
        void shouldHandleSpecialCharacters() throws JsonProcessingException {
            // Given
            NotificationMessage notification = NotificationMessage.builder()
                    .eventId("event-123")
                    .status(StatusCode.SUCCESS)
                    .message("訊息包含特殊字元: \"引號\" 和 \\反斜線\\")
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(notification);
            NotificationMessage deserialized = objectMapper.readValue(json, NotificationMessage.class);

            // Then
            assertThat(deserialized.getMessage())
                    .isEqualTo("訊息包含特殊字元: \"引號\" 和 \\反斜線\\");
        }

        @Test
        @DisplayName("應處理 Unicode 字元")
        void shouldHandleUnicodeCharacters() throws JsonProcessingException {
            // Given
            NotificationMessage notification = NotificationMessage.builder()
                    .eventId("event-123")
                    .status(StatusCode.SUCCESS)
                    .message("中文訊息 🎉 日本語 한국어")
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(notification);
            NotificationMessage deserialized = objectMapper.readValue(json, NotificationMessage.class);

            // Then
            assertThat(deserialized.getMessage()).isEqualTo("中文訊息 🎉 日本語 한국어");
        }

        @Test
        @DisplayName("應處理長字串")
        void shouldHandleLongMessage() throws JsonProcessingException {
            // Given
            String longMessage = "A".repeat(10000);
            NotificationMessage notification = NotificationMessage.builder()
                    .eventId("event-123")
                    .status(StatusCode.SUCCESS)
                    .message(longMessage)
                    .timestamp(1704067200000L)
                    .build();

            // When
            String json = objectMapper.writeValueAsString(notification);
            NotificationMessage deserialized = objectMapper.readValue(json, NotificationMessage.class);

            // Then
            assertThat(deserialized.getMessage()).hasSize(10000);
        }
    }
}
