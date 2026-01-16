package com.osw.dmp.ns.ignite;

import com.osw.dmp.ns.config.IgniteConfig;
import com.osw.dmp.ns.model.NodeInfo;
import com.osw.dmp.ns.model.NotificationLog;
import com.osw.dmp.ns.model.NotificationLog.PushStatus;
import com.osw.dmp.ns.model.ResultPayload;
import com.osw.dmp.ns.model.SubscriptionRouting;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Simulated Apache Ignite Cache for development/testing
 * Activated when app.ignite.enabled=false (default)
 * In production, set app.ignite.enabled=true to use real Ignite cluster
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "app.ignite.enabled", havingValue = "false", matchIfMissing = true)
public class SimulatedIgniteCache implements IgniteCacheService {

    private final IgniteConfig igniteConfig;

    // Subscription Routing Cache: eventId -> SubscriptionRouting
    private final Map<String, SubscriptionRouting> subscriptionRoutingCache = new ConcurrentHashMap<>();

    // Notification Log Cache: compositeKey -> NotificationLog
    private final Map<String, NotificationLog> notificationLogCache = new ConcurrentHashMap<>();

    // Node Registry Cache: nodeId -> NodeInfo
    private final Map<String, NodeInfo> nodeRegistryCache = new ConcurrentHashMap<>();

    // TTL settings (simulated)
    private final Duration subscriptionTtl = Duration.ofMinutes(30);
    private final Duration notificationLogTtl = Duration.ofMinutes(30);

    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor();

    private volatile boolean connected = true;

    public SimulatedIgniteCache(IgniteConfig igniteConfig) {
        this.igniteConfig = igniteConfig;
    }

    @PostConstruct
    public void init() {
        log.info("🔥 Simulated Ignite Cache initialized (keyPrefix={})", igniteConfig.getKeyPrefix());

        // Schedule TTL cleanup
        cleanupExecutor.scheduleAtFixedRate(this::cleanupExpired, 1, 1, TimeUnit.MINUTES);
    }

    /**
     * Generate notification log key with prefix
     */
    private String getNotificationLogKey(String eventId) {
        return NotificationLog.generateKey(igniteConfig.getKeyPrefix(), eventId);
    }

    // ==================== Subscription Routing Operations ====================

    /**
     * Store subscription routing
     */
    @Override
    public void putSubscriptionRouting(String eventId, SubscriptionRouting routing) {
        subscriptionRoutingCache.put(eventId, routing);
        log.debug("📝 Stored subscription routing: eventId={}, nodeId={}", eventId, routing.getNodeId());
    }

    /**
     * Get subscription routing by eventId
     */
    @Override
    public Optional<SubscriptionRouting> getSubscriptionRouting(String eventId) {
        return Optional.ofNullable(subscriptionRoutingCache.get(eventId));
    }

    /**
     * Remove subscription routing
     */
    @Override
    public void removeSubscriptionRouting(String eventId) {
        SubscriptionRouting removed = subscriptionRoutingCache.remove(eventId);
        if (removed != null) {
            log.debug("🗑️ Removed subscription routing: eventId={}", eventId);
        }
    }

    /**
     * Remove all routings for a session
     */
    @Override
    public void removeRoutingsBySessionId(String sessionId) {
        subscriptionRoutingCache.entrySet().removeIf(entry -> {
            boolean match = sessionId.equals(entry.getValue().getSessionId());
            if (match) {
                log.debug("🗑️ Removed routing for session: eventId={}, sessionId={}",
                        entry.getKey(), sessionId);
            }
            return match;
        });
    }

    /**
     * Remove all routings for a node
     */
    @Override
    public void removeRoutingsByNodeId(String nodeId) {
        subscriptionRoutingCache.entrySet().removeIf(entry -> {
            boolean match = nodeId.equals(entry.getValue().getNodeId());
            if (match) {
                log.debug("🗑️ Removed routing for node: eventId={}, nodeId={}",
                        entry.getKey(), nodeId);
            }
            return match;
        });
    }

    /**
     * Get total subscription count
     */
    @Override
    public int getTotalSubscriptionCount() {
        return subscriptionRoutingCache.size();
    }

    /**
     * Get count by node
     */
    @Override
    public Map<String, Long> getSubscriptionCountByNode() {
        return subscriptionRoutingCache.values().stream()
                .collect(Collectors.groupingBy(SubscriptionRouting::getNodeId, Collectors.counting()));
    }

    /**
     * Get slow waiting count (subscriptions older than threshold)
     */
    @Override
    public int getSlowWaitingCount(Duration threshold) {
        long cutoff = System.currentTimeMillis() - threshold.toMillis();
        return (int) subscriptionRoutingCache.values().stream()
                .filter(r -> r.getSubscribedAtMillis() < cutoff)
                .count();
    }

    // ==================== Notification Log Operations ====================

    /**
     * Try to put notification log (idempotent - putIfAbsent)
     * Returns existing log if already exists
     */
    @Override
    public Optional<NotificationLog> putIfAbsent(NotificationLog logEntry) {
        String key = getNotificationLogKey(logEntry.getEventId());
        NotificationLog existing = notificationLogCache.putIfAbsent(key, logEntry);
        if (existing != null) {
            return Optional.of(existing);
        }
        log.debug("📝 Created notification log: key={}, pushStatus={}", key, logEntry.getPushStatus());
        return Optional.empty();
    }

    /**
     * Update notification log
     */
    @Override
    public void updateNotificationLog(NotificationLog logEntry) {
        logEntry.setUpdatedAt(System.currentTimeMillis());
        String key = getNotificationLogKey(logEntry.getEventId());
        notificationLogCache.put(key, logEntry);
        log.debug("📝 Updated notification log: key={}, pushStatus={}", key, logEntry.getPushStatus());
    }

    /**
     * Get notification log by eventId
     */
    @Override
    public Optional<NotificationLog> getNotificationLog(String eventId) {
        String key = getNotificationLogKey(eventId);
        return Optional.ofNullable(notificationLogCache.get(key));
    }

    /**
     * Find pending notification log for eventId
     */
    @Override
    public Optional<NotificationLog> findPendingLog(String eventId) {
        return getNotificationLog(eventId)
                .filter(logEntry -> logEntry.getPushStatus() == PushStatus.PENDING);
    }

    /**
     * Check if already sent
     */
    @Override
    public boolean isAlreadySent(String eventId) {
        return getNotificationLog(eventId)
                .map(logEntry -> logEntry.getPushStatus() == PushStatus.SENT)
                .orElse(false);
    }

    /**
     * Mark as sent
     */
    @Override
    public void markSent(String eventId) {
        String key = getNotificationLogKey(eventId);
        NotificationLog logEntry = notificationLogCache.get(key);
        if (logEntry != null) {
            logEntry.setPushStatus(PushStatus.SENT);
            logEntry.setUpdatedAt(System.currentTimeMillis());
            logEntry.setAttempts(logEntry.getAttempts() + 1);
            log.debug("✅ Marked as SENT: key={}", key);
        }
    }

    /**
     * Mark as pending with result payload
     */
    @Override
    public void markPending(String eventId, ResultPayload payload) {
        String key = getNotificationLogKey(eventId);
        NotificationLog logEntry = NotificationLog.builder()
                .eventId(eventId)
                .pushStatus(PushStatus.PENDING)
                .resultPayload(payload)
                .attempts(0)
                .createdAt(System.currentTimeMillis())
                .updatedAt(System.currentTimeMillis())
                .build();
        notificationLogCache.put(key, logEntry);
        log.debug("⏳ Marked as PENDING: key={}", key);
    }

    /**
     * Mark as failed
     */
    @Override
    public void markFailed(String eventId) {
        String key = getNotificationLogKey(eventId);
        NotificationLog logEntry = notificationLogCache.get(key);
        if (logEntry != null) {
            logEntry.setPushStatus(PushStatus.FAILED);
            logEntry.setUpdatedAt(System.currentTimeMillis());
            log.debug("❌ Marked as FAILED: key={}", key);
        }
    }

    /**
     * Get notification stats
     */
    @Override
    public Map<PushStatus, Long> getNotificationStats() {
        return notificationLogCache.values().stream()
                .collect(Collectors.groupingBy(NotificationLog::getPushStatus, Collectors.counting()));
    }

    // ==================== Health & Maintenance ====================

    /**
     * Cleanup expired entries
     */
    private void cleanupExpired() {
        long subCutoff = System.currentTimeMillis() - subscriptionTtl.toMillis();
        long logCutoff = System.currentTimeMillis() - notificationLogTtl.toMillis();

        int subRemoved = 0;
        for (var iterator = subscriptionRoutingCache.entrySet().iterator(); iterator.hasNext();) {
            var entry = iterator.next();
            if (entry.getValue().getSubscribedAtMillis() < subCutoff) {
                iterator.remove();
                subRemoved++;
            }
        }

        int logRemoved = 0;
        for (var iterator = notificationLogCache.entrySet().iterator(); iterator.hasNext();) {
            var entry = iterator.next();
            if (entry.getValue().getCreatedAt() < logCutoff) {
                iterator.remove();
                logRemoved++;
            }
        }

        if (subRemoved > 0 || logRemoved > 0) {
            log.info("🧹 Cleanup: removed {} subscriptions, {} logs", subRemoved, logRemoved);
        }
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    // ==================== Node Registry Operations ====================

    @Override
    public void registerNode(NodeInfo nodeInfo) {
        nodeRegistryCache.put(nodeInfo.getNodeId(), nodeInfo);
        log.info("📋 Node registered in Ignite: nodeId={}, grpcAddress={}",
                nodeInfo.getNodeId(), nodeInfo.getGrpcAddress());
    }

    @Override
    public void unregisterNode(String nodeId) {
        NodeInfo removed = nodeRegistryCache.remove(nodeId);
        if (removed != null) {
            log.info("📋 Node unregistered from Ignite: nodeId={}", nodeId);
        }
    }

    @Override
    public Optional<NodeInfo> getNode(String nodeId) {
        return Optional.ofNullable(nodeRegistryCache.get(nodeId));
    }

    @Override
    public Collection<NodeInfo> getAllNodes() {
        return nodeRegistryCache.values();
    }

    @Override
    public void updateNodeHeartbeat(String nodeId) {
        NodeInfo nodeInfo = nodeRegistryCache.get(nodeId);
        if (nodeInfo != null) {
            nodeInfo.updateHeartbeat();
            log.debug("💓 Node heartbeat updated in Ignite: nodeId={}", nodeId);
        }
    }
}
