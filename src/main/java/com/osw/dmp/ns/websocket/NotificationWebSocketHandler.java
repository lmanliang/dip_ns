package com.osw.dmp.ns.websocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.osw.dmp.ns.grpc.NodeIdentity;
import com.osw.dmp.ns.ignite.IgniteCacheService;
import com.osw.dmp.ns.model.NotificationLog;
import com.osw.dmp.ns.model.StatusCode;
import com.osw.dmp.ns.model.SubscriptionRouting;
import com.osw.dmp.ns.websocket.WebSocketMessages.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.osw.dmp.ns.websocket.WebSocketMessages.*;

/**
 * WebSocket Handler for Notification Service
 * Handles client connections, subscriptions, and message routing
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationWebSocketHandler implements WebSocketHandler {

    private final ObjectMapper objectMapper;
    private final IgniteCacheService igniteCache;
    private final NodeIdentity nodeIdentity;

    // Local session store
    private final LocalSessionStore sessionStore = new LocalSessionStore();

    // Sinks for each session to send outgoing messages
    private final Map<String, Sinks.Many<String>> sessionSinks = new ConcurrentHashMap<>();

    @Value("${app.websocket.max-connections:10000}")
    private int maxConnections;

    @Override
    @NonNull
    @SuppressWarnings("null")
    public Mono<Void> handle(@NonNull WebSocketSession session) {
        String sessionId = session.getId();
        log.info("🔌 WebSocket connection opened: sessionId={}", sessionId);

        // Check connection limit
        if (sessionStore.getConnectionCount() >= maxConnections) {
            log.warn("⚠️ Connection limit reached, rejecting: sessionId={}", sessionId);
            return sendError(session, ERROR_INTERNAL_ERROR, "Connection limit reached", null)
                    .then(session.close());
        }

        // Register session
        sessionStore.registerSession(session);
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
        sessionSinks.put(sessionId, sink);

        // Handle incoming messages - process each message but don't complete when input
        // stream ends
        // The connection should stay open until explicitly closed or output sink
        // completes
        session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .flatMap(payload -> handleMessage(session, payload))
                .doOnError(
                        e -> log.error("❌ Error handling message: sessionId={}, error={}", sessionId, e.getMessage()))
                .subscribe(); // Subscribe but don't use for connection lifecycle

        // Send outgoing messages - this controls the connection lifecycle
        return session.send(
                sink.asFlux()
                        .map(session::textMessage))
                .doFinally(signal -> {
                    log.info("🔌 WebSocket connection closed: sessionId={}, signal={}", sessionId, signal);
                    cleanup(session);
                });
    }

    /**
     * Handle incoming WebSocket message
     */
    private Mono<Void> handleMessage(WebSocketSession session, String payload) {
        try {
            JsonNode node = objectMapper.readTree(payload);
            String type = node.path("type").asText();

            return switch (type) {
                case "subscribe" -> handleSubscribe(session, node);
                case "unsubscribe" -> handleUnsubscribe(session, node);
                case "ping" -> handlePing(session);
                default -> {
                    log.warn("Unknown message type: {}", type);
                    yield sendError(session, ERROR_INTERNAL_ERROR, "Unknown message type: " + type, null);
                }
            };
        } catch (JsonProcessingException e) {
            log.error("Failed to parse message: {}", e.getMessage());
            return sendError(session, ERROR_INTERNAL_ERROR, "Invalid JSON format", null);
        }
    }

    /**
     * Handle subscribe request
     */
    private Mono<Void> handleSubscribe(WebSocketSession session, JsonNode node) {
        String eventId = node.path("eventId").asText();
        String sessionId = session.getId();

        // Validate eventId
        if (eventId == null || eventId.isBlank()) {
            return sendError(session, ERROR_INVALID_EVENT_ID, "eventId is required", null);
        }

        log.info("📥 Subscribe request: sessionId={}, eventId={}", sessionId, eventId);

        // 1. Store in local memory
        sessionStore.subscribe(eventId, session);

        // 2. Store in Ignite routing table
        SubscriptionRouting routing = SubscriptionRouting.builder()
                .eventId(eventId)
                .nodeId(nodeIdentity.getNodeId())
                .grpcAddress(nodeIdentity.getGrpcAddress())
                .sessionId(sessionId)
                .subscribedAtMillis(System.currentTimeMillis())
                .build();
        igniteCache.putSubscriptionRouting(eventId, routing);

        // 3. Check for pending notifications (late subscription)
        Optional<NotificationLog> pendingLog = igniteCache.findPendingLog(eventId);
        if (pendingLog.isPresent()) {
            log.info("📨 Found pending notification for eventId={}, pushing...", eventId);
            NotificationLog pending = pendingLog.get();
            int status = pending.getResultPayload().getStatus();

            // Push the pending result
            NotificationMessage notification = NotificationMessage.builder()
                    .eventId(pending.getEventId())
                    .status(status)
                    .message(pending.getResultPayload().getMessage())
                    .timestamp(System.currentTimeMillis())
                    .build();

            // Mark as sent
            igniteCache.markSent(pending.getEventId());

            // Send notification, then close if terminal
            return sendMessage(session, notification)
                    .then(sendSubscribed(session, eventId))
                    .then(Mono.defer(() -> {
                        if (StatusCode.isTerminal(status)) {
                            log.info(
                                    "🔒 Terminal status in pending notification, closing WebSocket: eventId={}, status={}",
                                    eventId, status);
                            return Mono.delay(Duration.ofMillis(100))
                                    .then(session.close())
                                    .doOnSuccess(v -> log.info(
                                            "🔌 WebSocket closed after pending notification: eventId={}", eventId))
                                    .onErrorResume(e -> {
                                        log.warn("⚠️ Error closing WebSocket: eventId={}, error={}", eventId,
                                                e.getMessage());
                                        return Mono.empty();
                                    });
                        }
                        return Mono.empty();
                    }));
        }

        // 4. Send subscription confirmation
        return sendSubscribed(session, eventId);
    }

    /**
     * Handle unsubscribe request
     */
    private Mono<Void> handleUnsubscribe(WebSocketSession session, JsonNode node) {
        String eventId = node.path("eventId").asText();
        String sessionId = session.getId();

        log.info("📤 Unsubscribe request: sessionId={}, eventId={}", sessionId, eventId);

        // Remove from local memory
        sessionStore.unsubscribe(eventId, session);

        // Remove from Ignite
        igniteCache.removeSubscriptionRouting(eventId);

        // Send confirmation
        UnsubscribedResponse response = UnsubscribedResponse.builder()
                .eventId(eventId)
                .timestamp(System.currentTimeMillis())
                .build();

        return sendMessage(session, response);
    }

    /**
     * Handle ping (heartbeat)
     */
    private Mono<Void> handlePing(WebSocketSession session) {
        PongMessage pong = PongMessage.builder()
                .timestamp(System.currentTimeMillis())
                .build();
        return sendMessage(session, pong);
    }

    /**
     * Send subscribed confirmation
     */
    private Mono<Void> sendSubscribed(WebSocketSession session, String eventId) {
        SubscribedResponse response = SubscribedResponse.builder()
                .eventId(eventId)
                .timestamp(System.currentTimeMillis())
                .build();
        return sendMessage(session, response);
    }

    /**
     * Send error message
     */
    private Mono<Void> sendError(WebSocketSession session, String code, String message, String eventId) {
        ErrorMessage error = ErrorMessage.builder()
                .code(code)
                .message(message)
                .eventId(eventId)
                .timestamp(System.currentTimeMillis())
                .build();
        return sendMessage(session, error);
    }

    /**
     * Send message to session
     */
    public Mono<Void> sendMessage(WebSocketSession session, Object message) {
        try {
            String json = objectMapper.writeValueAsString(message);
            Sinks.Many<String> sink = sessionSinks.get(session.getId());
            if (sink != null) {
                sink.tryEmitNext(json);
            }
            return Mono.empty();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize message: {}", e.getMessage());
            return Mono.error(e);
        }
    }

    /**
     * Push notification to eventId
     * Returns true if pushed locally, false if needs cross-node push
     * Will close WebSocket connection if status is terminal (task completed)
     */
    public boolean pushNotification(String eventId, NotificationMessage notification) {
        Optional<WebSocketSession> sessionOpt = sessionStore.getSessionByEventId(eventId);
        if (sessionOpt.isPresent()) {
            WebSocketSession session = sessionOpt.get();
            sendMessage(session, notification).subscribe();
            log.info("✅ Pushed notification locally: eventId={}, status={}", eventId, notification.getStatus());

            // Close connection if terminal status (task completed)
            if (StatusCode.isTerminal(notification.getStatus())) {
                log.info("🔒 Terminal status received, closing WebSocket: eventId={}, status={}",
                        eventId, notification.getStatus());
                // Delay close slightly to ensure message is sent
                Mono.delay(Duration.ofMillis(100))
                        .then(session.close())
                        .subscribe(
                                v -> log.info("🔌 WebSocket closed after terminal notification: eventId={}", eventId),
                                e -> log.warn("⚠️ Error closing WebSocket: eventId={}, error={}", eventId,
                                        e.getMessage()));
            }
            return true;
        }
        return false;
    }

    /**
     * Cleanup on session close
     */
    private void cleanup(WebSocketSession session) {
        String sessionId = session.getId();

        // Get all eventIds for this session
        var eventIds = sessionStore.removeSession(session);

        // Remove from Ignite
        eventIds.forEach(igniteCache::removeSubscriptionRouting);

        // Remove sink
        Sinks.Many<String> sink = sessionSinks.remove(sessionId);
        if (sink != null) {
            sink.tryEmitComplete();
        }

        log.info("🧹 Cleaned up session: sessionId={}, eventIds={}", sessionId, eventIds.size());
    }

    // ==================== Accessors for metrics ====================

    public LocalSessionStore getSessionStore() {
        return sessionStore;
    }

    public String getNodeId() {
        return nodeIdentity.getNodeId();
    }
}
