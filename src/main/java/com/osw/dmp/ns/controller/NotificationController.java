package com.osw.dmp.ns.controller;

import com.osw.dmp.ns.ignite.IgniteCacheService;
import com.osw.dmp.ns.kafka.KafkaConsumerStatus;
import com.osw.dmp.ns.kafka.SimulatedKafkaConsumer;
import com.osw.dmp.ns.model.NotificationLog.PushStatus;
import com.osw.dmp.ns.model.StatusCode;
import com.osw.dmp.ns.websocket.NotificationWebSocketHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * REST API for operations and simulation testing
 */
@RestController
@RequestMapping("/api")
public class NotificationController {

    private final KafkaConsumerStatus kafkaConsumer;
    private final IgniteCacheService igniteCache;
    private final NotificationWebSocketHandler webSocketHandler;

    // Optional: only available when app.kafka.enabled=false
    private final SimulatedKafkaConsumer simulatedKafkaConsumer;

    @Autowired
    public NotificationController(
            KafkaConsumerStatus kafkaConsumer,
            IgniteCacheService igniteCache,
            NotificationWebSocketHandler webSocketHandler,
            @Autowired(required = false) SimulatedKafkaConsumer simulatedKafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
        this.igniteCache = igniteCache;
        this.webSocketHandler = webSocketHandler;
        this.simulatedKafkaConsumer = simulatedKafkaConsumer;
    }

    // ==================== Health & Readiness ====================

    /**
     * Liveness check
     */
    @GetMapping("/health")
    public Mono<ResponseEntity<Map<String, Object>>> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("timestamp", System.currentTimeMillis());
        return Mono.just(ResponseEntity.ok(response));
    }

    /**
     * Readiness check
     */
    @GetMapping("/ready")
    public Mono<ResponseEntity<Map<String, Object>>> ready() {
        Map<String, Object> response = new HashMap<>();

        Map<String, String> components = new HashMap<>();
        components.put("kafka", kafkaConsumer.isRunning() ? "UP" : "DOWN");
        components.put("ignite", igniteCache.isConnected() ? "UP" : "DOWN");
        components.put("websocket", "UP");

        boolean allUp = components.values().stream().allMatch("UP"::equals);

        response.put("status", allUp ? "UP" : "DOWN");
        response.put("components", components);
        response.put("timestamp", System.currentTimeMillis());

        return Mono.just(allUp ? ResponseEntity.ok(response) : ResponseEntity.status(503).body(response));
    }

    // ==================== Metrics ====================

    /**
     * Get metrics
     */
    @GetMapping("/metrics/custom")
    public Mono<ResponseEntity<Map<String, Object>>> metrics() {
        Map<String, Object> metrics = new HashMap<>();

        // WebSocket metrics
        metrics.put("websocket_connections_total", webSocketHandler.getSessionStore().getConnectionCount());
        metrics.put("websocket_subscriptions_total", webSocketHandler.getSessionStore().getSubscriptionCount());

        // Ignite metrics
        metrics.put("ignite_subscriptions_total", igniteCache.getTotalSubscriptionCount());
        metrics.put("ignite_subscriptions_by_node", igniteCache.getSubscriptionCountByNode());
        metrics.put("ignite_subscriptions_slow", igniteCache.getSlowWaitingCount(Duration.ofSeconds(10)));

        // Notification stats
        Map<PushStatus, Long> notificationStats = igniteCache.getNotificationStats();
        metrics.put("notifications_sent", notificationStats.getOrDefault(PushStatus.SENT, 0L));
        metrics.put("notifications_pending", notificationStats.getOrDefault(PushStatus.PENDING, 0L));
        metrics.put("notifications_failed", notificationStats.getOrDefault(PushStatus.FAILED, 0L));

        metrics.put("node_id", webSocketHandler.getNodeId());
        metrics.put("timestamp", System.currentTimeMillis());

        return Mono.just(ResponseEntity.ok(metrics));
    }

    // ==================== Simulation API ====================

    /**
     * Simulate a successful result event (status = 1000)
     * Only available when app.kafka.enabled=false
     */
    @PostMapping("/simulate/success/{eventId}")
    public Mono<ResponseEntity<Map<String, Object>>> simulateSuccess(
            @PathVariable String eventId,
            @RequestParam(defaultValue = "處理完成") String message) {

        if (simulatedKafkaConsumer == null) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Simulation API not available when real Kafka is enabled");
            return Mono.just(ResponseEntity.badRequest().body(error));
        }

        simulatedKafkaConsumer.simulateSuccess(eventId, message);

        Map<String, Object> response = new HashMap<>();
        response.put("eventId", eventId);
        response.put("status", StatusCode.SUCCESS);
        response.put("message", message);
        response.put("timestamp", System.currentTimeMillis());

        return Mono.just(ResponseEntity.ok(response));
    }

    /**
     * Simulate a failed result event
     * Only available when app.kafka.enabled=false
     */
    @PostMapping("/simulate/failed/{eventId}")
    public Mono<ResponseEntity<Map<String, Object>>> simulateFailed(
            @PathVariable String eventId,
            @RequestParam(defaultValue = "3001") int status,
            @RequestParam(defaultValue = "處理超時") String message) {

        if (simulatedKafkaConsumer == null) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Simulation API not available when real Kafka is enabled");
            return Mono.just(ResponseEntity.badRequest().body(error));
        }

        simulatedKafkaConsumer.simulateFailed(eventId, status, message);

        Map<String, Object> response = new HashMap<>();
        response.put("eventId", eventId);
        response.put("status", status);
        response.put("message", message);
        response.put("timestamp", System.currentTimeMillis());

        return Mono.just(ResponseEntity.ok(response));
    }

    /**
     * Simulate a delayed result (for testing async behavior)
     * Only available when app.kafka.enabled=false
     */
    @PostMapping("/simulate/delayed/{eventId}")
    public Mono<ResponseEntity<Map<String, Object>>> simulateDelayed(
            @PathVariable String eventId,
            @RequestParam(defaultValue = "3") int delaySeconds) {

        if (simulatedKafkaConsumer == null) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Simulation API not available when real Kafka is enabled");
            return Mono.just(ResponseEntity.badRequest().body(error));
        }

        simulatedKafkaConsumer.simulateDelayedSuccess(eventId, Duration.ofSeconds(delaySeconds));

        Map<String, Object> response = new HashMap<>();
        response.put("eventId", eventId);
        response.put("delaySeconds", delaySeconds);
        response.put("timestamp", System.currentTimeMillis());

        return Mono.just(ResponseEntity.ok(response));
    }

    /**
     * Get status overview
     */
    @GetMapping("/status")
    public Mono<ResponseEntity<Map<String, Object>>> status() {
        Map<String, Object> status = new HashMap<>();

        status.put("service", "Notification Service");
        status.put("nodeId", webSocketHandler.getNodeId());
        status.put("kafkaConsumer", kafkaConsumer.isRunning() ? "RUNNING" : "STOPPED");
        status.put("igniteCache", igniteCache.isConnected() ? "CONNECTED" : "DISCONNECTED");
        status.put("websocketConnections", webSocketHandler.getSessionStore().getConnectionCount());
        status.put("activeSubscriptions", igniteCache.getTotalSubscriptionCount());
        status.put("timestamp", System.currentTimeMillis());

        return Mono.just(ResponseEntity.ok(status));
    }
}
