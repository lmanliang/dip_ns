package com.osw.dmp.ns.service;

import com.osw.dmp.ns.grpc.CrossNodePushService;
import com.osw.dmp.ns.grpc.NodeIdentity;
import com.osw.dmp.ns.ignite.IgniteCacheService;
import com.osw.dmp.ns.model.ResultEvent;
import com.osw.dmp.ns.model.ResultPayload;
import com.osw.dmp.ns.model.StatusCode;
import com.osw.dmp.ns.model.SubscriptionRouting;
import com.osw.dmp.ns.websocket.NotificationWebSocketHandler;
import com.osw.dmp.ns.websocket.WebSocketMessages.NotificationMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Optional;

/**
 * Core Notification Service
 * Processes ResultEvents from Kafka and pushes to WebSocket clients
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final IgniteCacheService igniteCache;
    private final NotificationWebSocketHandler webSocketHandler;
    private final CrossNodePushService crossNodePushService;
    private final NodeIdentity nodeIdentity;

    @Value("${app.notification.max-retries:3}")
    private int maxRetries;

    @Value("${app.notification.retry-delay-ms:1000}")
    private long retryDelayMs;

    /**
     * Process ResultEvent from Kafka
     * Main entry point for notification processing
     */
    public Mono<Void> processResultEvent(ResultEvent event) {
        String eventId = event.getEventId();

        // 根據 status 記錄適當等級的日誌
        StatusCode.logByStatus(log, event.getStatus(), eventId,
                String.format("Processing ResultEvent: status=%s", event.getStatus()));

        return Mono.defer(() -> {
            // 1. Idempotency check
            if (igniteCache.isAlreadySent(eventId)) {
                log.info("⏭️ Already sent, skipping: eventId={}", eventId);
                return Mono.empty();
            }

            // 2. Build notification message
            NotificationMessage notification = buildNotification(event);

            // 3. Try to push notification
            return pushNotification(event, notification);
        });
    }

    /**
     * Build NotificationMessage from ResultEvent
     */
    private NotificationMessage buildNotification(ResultEvent event) {
        return NotificationMessage.builder()
                .eventId(event.getEventId())
                .status(event.getStatus())
                .message(event.getMessage())
                .timestamp(System.currentTimeMillis())
                .build();
    }

    /**
     * Push notification with retry logic
     */
    private Mono<Void> pushNotification(ResultEvent event, NotificationMessage notification) {
        String eventId = event.getEventId();

        return Mono.defer(() -> {
            // Try local push first
            boolean localPushed = webSocketHandler.pushNotification(eventId, notification);

            if (localPushed) {
                // Mark as sent
                igniteCache.markSent(eventId);
                log.info("✅ Notification sent locally: eventId={}, status={}", eventId, event.getStatus());
                return Mono.empty();
            }

            // Check Ignite routing table for cross-node push
            Optional<SubscriptionRouting> routingOpt = igniteCache.getSubscriptionRouting(eventId);

            if (routingOpt.isPresent()) {
                SubscriptionRouting routing = routingOpt.get();
                log.info("🔀 Found routing in Ignite: eventId={}, nodeId={}, grpcAddress={}",
                        eventId, routing.getNodeId(), routing.getGrpcAddress());

                // 檢查是否為當前節點（Routing 指向本節點但找不到 Session）
                if (nodeIdentity.isCurrentNode(routing.getNodeId())) {
                    log.warn("⚠️ Routing points to this node but no session found: eventId={}", eventId);
                    return markAsPending(event);
                }

                // 嘗試跨節點推送
                CrossNodePushService.PushResult pushResult = crossNodePushService.push(
                        routing, event.getStatus(), event.getMessage());

                switch (pushResult) {
                    case SUCCESS:
                        igniteCache.markSent(eventId);
                        log.info("✅ Cross-node push success: eventId={}, targetNode={}",
                                eventId, routing.getNodeId());
                        return Mono.empty();

                    case SESSION_NOT_FOUND:
                        // Session 已不存在，清除過期路由
                        igniteCache.removeSubscriptionRouting(eventId);
                        log.warn("⚠️ Session not found on target node, removed routing: eventId={}", eventId);
                        return markAsPending(event);

                    case NODE_UNREACHABLE:
                    case FAILED:
                    case ERROR:
                        log.warn("⚠️ Cross-node push failed, marking pending: eventId={}, result={}",
                                eventId, pushResult);
                        return markAsPending(event);

                    case LOCAL_NODE:
                    default:
                        // 不應該發生，但作為安全處理
                        return markAsPending(event);
                }
            }

            // No subscription found - mark as PENDING for late subscription
            log.info("⏳ No subscription found, marking PENDING: eventId={}", eventId);
            return markAsPending(event);

        }).retryWhen(Retry.backoff(maxRetries, Duration.ofMillis(retryDelayMs))
                .doBeforeRetry(signal -> log.warn("🔄 Retrying notification: eventId={}, attempt={}",
                        eventId, signal.totalRetriesInARow() + 1))
                .onRetryExhaustedThrow((spec, signal) -> {
                    log.error("❌ Retry exhausted for: eventId={}", eventId);
                    igniteCache.markFailed(eventId);
                    return signal.failure();
                }));
    }

    /**
     * Mark event as pending with result payload
     */
    private Mono<Void> markAsPending(ResultEvent event) {
        ResultPayload payload = ResultPayload.builder()
                .status(event.getStatus())
                .message(event.getMessage())
                .build();

        igniteCache.markPending(event.getEventId(), payload);

        return Mono.empty();
    }
}
