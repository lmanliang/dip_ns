package com.osw.dmp.ns.websocket;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * WebSocket Message Models
 * All WebSocket messages are JSON with 'type' field
 */
public class WebSocketMessages {

    /**
     * Base message with type
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BaseMessage {
        private String type;
    }

    // ==================== Client → Server ====================

    /**
     * Subscribe request from client
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SubscribeRequest {
        private String type;
        private String eventId;
    }

    /**
     * Unsubscribe request from client
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UnsubscribeRequest {
        private String type;
        private String eventId;
    }

    /**
     * Ping from client
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PingMessage {
        private String type;
        private Long timestamp;
    }

    // ==================== Server → Client ====================

    /**
     * Subscription confirmed
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SubscribedResponse {
        @Builder.Default
        private String type = "subscribed";
        private String eventId;
        private Long timestamp;
    }

    /**
     * Unsubscription confirmed
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UnsubscribedResponse {
        @Builder.Default
        private String type = "unsubscribed";
        private String eventId;
        private Long timestamp;
    }

    /**
     * Notification result (core message)
     * 
     * 格式: {"type":"notification", "eventId":"xxx", "status":1000, "message":"...",
     * "timestamp":1736668800000}
     * 
     * Status Code:
     * 1xxx = 成功 (info)
     * 2xxx = 業務錯誤 (warn)
     * 3xxx = 系統錯誤 (error)
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class NotificationMessage {
        @Builder.Default
        private String type = "notification";

        /**
         * 事件識別碼
         */
        private String eventId;

        /**
         * 業務狀態碼 (1xxx成功/2xxx業務錯誤/3xxx系統錯誤)
         */
        private int status;

        /**
         * 訊息內容
         */
        private String message;

        /**
         * 時間戳 (epoch millis)
         */
        private Long timestamp;
    }

    /**
     * Pong response
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PongMessage {
        @Builder.Default
        private String type = "pong";
        private Long timestamp;
    }

    /**
     * Error message
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ErrorMessage {
        @Builder.Default
        private String type = "error";
        private String code;
        private String message;
        private String eventId;
        private Long timestamp;
    }

    /**
     * Error codes
     */
    public static final String ERROR_INVALID_EVENT_ID = "INVALID_EVENT_ID";
    public static final String ERROR_SUBSCRIPTION_LIMIT_EXCEEDED = "SUBSCRIPTION_LIMIT_EXCEEDED";
    public static final String ERROR_UNAUTHORIZED = "UNAUTHORIZED";
    public static final String ERROR_INTERNAL_ERROR = "INTERNAL_ERROR";
}
