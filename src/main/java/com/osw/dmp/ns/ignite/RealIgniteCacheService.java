package com.osw.dmp.ns.ignite;

import com.osw.dmp.ns.config.IgniteConfig;
import com.osw.dmp.ns.model.NodeInfo;
import com.osw.dmp.ns.model.NotificationLog;
import com.osw.dmp.ns.model.NotificationLog.PushStatus;
import com.osw.dmp.ns.model.ResultPayload;
import com.osw.dmp.ns.model.SubscriptionRouting;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Real Apache Ignite Cache Service Implementation
 * 
 * Uses Ignite Thin Client to connect to Ignite cluster
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "app.ignite.enabled", havingValue = "true", matchIfMissing = false)
public class RealIgniteCacheService implements IgniteCacheService {

    private final IgniteConfig igniteConfig;

    private IgniteClient client;
    private ClientCache<String, SubscriptionRouting> subscriptionRoutingCache;
    private ClientCache<String, NotificationLog> notificationLogCache;
    private ClientCache<String, NodeInfo> nodeRegistryCache;

    private volatile boolean connected = false;

    public RealIgniteCacheService(IgniteConfig igniteConfig) {
        this.igniteConfig = igniteConfig;
    }

    @PostConstruct
    public void init() {
        try {
            log.info("🔥 Connecting to Ignite cluster at: {}", igniteConfig.getAddresses());

            // Create thin client configuration
            ClientConfiguration cfg = new ClientConfiguration()
                    .setAddresses(igniteConfig.getAddresses().split(","));

            // Start client
            client = Ignition.startClient(cfg);

            // Get or create caches with configuration
            String subRoutingCacheName = igniteConfig.getCache().getName() != null
                    ? igniteConfig.getCache().getName()
                    : "subscription-routing";
            String notifLogCacheName = igniteConfig.getNotificationLog().getName() != null
                    ? igniteConfig.getNotificationLog().getName()
                    : "notification-logs";
            String nodeRegistryCacheName = "ns-nodes";

            // Configure caches with backups
            ClientCacheConfiguration subRoutingCfg = new ClientCacheConfiguration()
                    .setName(subRoutingCacheName)
                    .setBackups(igniteConfig.getCache().getBackups());

            ClientCacheConfiguration notifLogCfg = new ClientCacheConfiguration()
                    .setName(notifLogCacheName)
                    .setBackups(igniteConfig.getNotificationLog().getBackups());

            ClientCacheConfiguration nodeRegistryCfg = new ClientCacheConfiguration()
                    .setName(nodeRegistryCacheName)
                    .setBackups(2); // Node registry should have higher redundancy

            subscriptionRoutingCache = client.getOrCreateCache(subRoutingCfg);
            notificationLogCache = client.getOrCreateCache(notifLogCfg);
            nodeRegistryCache = client.getOrCreateCache(nodeRegistryCfg);

            connected = true;
            log.info("✅ Connected to Ignite cluster successfully!");
            log.info("   - Subscription Routing Cache: {}", subRoutingCacheName);
            log.info("   - Notification Log Cache: {}", notifLogCacheName);
            log.info("   - Node Registry Cache: {}", nodeRegistryCacheName);

        } catch (Exception e) {
            log.error("❌ Failed to connect to Ignite cluster: {}", e.getMessage(), e);
            connected = false;
        }
    }

    @PreDestroy
    public void shutdown() {
        if (client != null) {
            try {
                client.close();
                log.info("🔌 Ignite client disconnected");
            } catch (Exception e) {
                log.warn("Error closing Ignite client: {}", e.getMessage());
            }
        }
    }

    // ==================== Helper method to iterate cache entries
    // ====================

    private <K, V> List<Cache.Entry<K, V>> getAllEntries(ClientCache<K, V> cache) {
        List<Cache.Entry<K, V>> entries = new ArrayList<>();
        try (var cursor = cache.query(new org.apache.ignite.cache.query.ScanQuery<K, V>())) {
            cursor.forEach(entries::add);
        } catch (Exception e) {
            log.warn("Error scanning cache: {}", e.getMessage());
        }
        return entries;
    }

    // ==================== Subscription Routing Operations ====================

    @Override
    public void putSubscriptionRouting(String eventId, SubscriptionRouting routing) {
        checkConnection();
        subscriptionRoutingCache.put(eventId, routing);
        log.debug("📝 Stored subscription routing: eventId={}, nodeId={}", eventId, routing.getNodeId());
    }

    @Override
    public Optional<SubscriptionRouting> getSubscriptionRouting(String eventId) {
        checkConnection();
        return Optional.ofNullable(subscriptionRoutingCache.get(eventId));
    }

    @Override
    public void removeSubscriptionRouting(String eventId) {
        checkConnection();
        boolean removed = subscriptionRoutingCache.remove(eventId);
        if (removed) {
            log.debug("🗑️ Removed subscription routing: eventId={}", eventId);
        }
    }

    @Override
    public void removeRoutingsBySessionId(String sessionId) {
        checkConnection();
        try {
            for (var entry : getAllEntries(subscriptionRoutingCache)) {
                if (sessionId.equals(entry.getValue().getSessionId())) {
                    subscriptionRoutingCache.remove(entry.getKey());
                    log.debug("🗑️ Removed routing for session: eventId={}, sessionId={}",
                            entry.getKey(), sessionId);
                }
            }
        } catch (Exception e) {
            log.warn("Error removing routings by sessionId: {}", e.getMessage());
        }
    }

    @Override
    public void removeRoutingsByNodeId(String nodeId) {
        checkConnection();
        try {
            for (var entry : getAllEntries(subscriptionRoutingCache)) {
                if (nodeId.equals(entry.getValue().getNodeId())) {
                    subscriptionRoutingCache.remove(entry.getKey());
                    log.debug("🗑️ Removed routing for node: eventId={}, nodeId={}",
                            entry.getKey(), nodeId);
                }
            }
        } catch (Exception e) {
            log.warn("Error removing routings by nodeId: {}", e.getMessage());
        }
    }

    @Override
    public int getTotalSubscriptionCount() {
        checkConnection();
        return subscriptionRoutingCache.size();
    }

    @Override
    public Map<String, Long> getSubscriptionCountByNode() {
        checkConnection();
        Map<String, Long> counts = new HashMap<>();
        try {
            for (var entry : getAllEntries(subscriptionRoutingCache)) {
                String nodeId = entry.getValue().getNodeId();
                counts.merge(nodeId, 1L, (a, b) -> a + b);
            }
        } catch (Exception e) {
            log.warn("Error getting subscription count by node: {}", e.getMessage());
        }
        return counts;
    }

    @Override
    public int getSlowWaitingCount(java.time.Duration threshold) {
        checkConnection();
        long cutoff = System.currentTimeMillis() - threshold.toMillis();
        int count = 0;
        try {
            for (var entry : getAllEntries(subscriptionRoutingCache)) {
                if (entry.getValue().getSubscribedAtMillis() < cutoff) {
                    count++;
                }
            }
        } catch (Exception e) {
            log.warn("Error getting slow waiting count: {}", e.getMessage());
        }
        return count;
    }

    // ==================== Notification Log Operations ====================

    private String getNotificationLogKey(String eventId) {
        return NotificationLog.generateKey(igniteConfig.getKeyPrefix(), eventId);
    }

    @Override
    public Optional<NotificationLog> putIfAbsent(NotificationLog logEntry) {
        checkConnection();
        String key = getNotificationLogKey(logEntry.getEventId());
        NotificationLog existing = notificationLogCache.getAndPutIfAbsent(key, logEntry);
        if (existing != null) {
            return Optional.of(existing);
        }
        log.debug("📝 Created notification log: key={}, pushStatus={}", key, logEntry.getPushStatus());
        return Optional.empty();
    }

    @Override
    public void updateNotificationLog(NotificationLog logEntry) {
        checkConnection();
        String key = getNotificationLogKey(logEntry.getEventId());
        logEntry.setUpdatedAt(System.currentTimeMillis());
        notificationLogCache.put(key, logEntry);
        log.debug("📝 Updated notification log: key={}, pushStatus={}", key, logEntry.getPushStatus());
    }

    @Override
    public Optional<NotificationLog> getNotificationLog(String eventId) {
        checkConnection();
        String key = getNotificationLogKey(eventId);
        return Optional.ofNullable(notificationLogCache.get(key));
    }

    @Override
    public Optional<NotificationLog> findPendingLog(String eventId) {
        checkConnection();
        try {
            for (var entry : getAllEntries(notificationLogCache)) {
                NotificationLog logEntry = entry.getValue();
                if (eventId.equals(logEntry.getEventId()) && logEntry.getPushStatus() == PushStatus.PENDING) {
                    return Optional.of(logEntry);
                }
            }
        } catch (Exception e) {
            log.warn("Error finding pending log: {}", e.getMessage());
        }
        return Optional.empty();
    }

    @Override
    public boolean isAlreadySent(String eventId) {
        return getNotificationLog(eventId)
                .map(logEntry -> logEntry.getPushStatus() == PushStatus.SENT)
                .orElse(false);
    }

    @Override
    public void markSent(String eventId) {
        checkConnection();
        String key = getNotificationLogKey(eventId);
        NotificationLog logEntry = notificationLogCache.get(key);
        if (logEntry != null) {
            logEntry.setPushStatus(PushStatus.SENT);
            logEntry.setUpdatedAt(System.currentTimeMillis());
            logEntry.setAttempts(logEntry.getAttempts() + 1);
            notificationLogCache.put(key, logEntry);
            log.debug("✅ Marked as SENT: key={}", key);
        }
    }

    @Override
    public void markPending(String eventId, ResultPayload payload) {
        checkConnection();
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

    @Override
    public void markFailed(String eventId) {
        checkConnection();
        String key = getNotificationLogKey(eventId);
        NotificationLog logEntry = notificationLogCache.get(key);
        if (logEntry != null) {
            logEntry.setPushStatus(PushStatus.FAILED);
            logEntry.setUpdatedAt(System.currentTimeMillis());
            notificationLogCache.put(key, logEntry);
            log.debug("❌ Marked as FAILED: key={}", key);
        }
    }

    @Override
    public Map<PushStatus, Long> getNotificationStats() {
        checkConnection();
        Map<PushStatus, Long> stats = new HashMap<>();
        try {
            for (var entry : getAllEntries(notificationLogCache)) {
                PushStatus status = entry.getValue().getPushStatus();
                stats.merge(status, 1L, (a, b) -> a + b);
            }
        } catch (Exception e) {
            log.warn("Error getting notification stats: {}", e.getMessage());
        }
        return stats;
    }

    // ==================== Health & Status ====================

    @Override
    public boolean isConnected() {
        return connected && client != null;
    }

    private void checkConnection() {
        if (!isConnected()) {
            throw new IllegalStateException("Ignite client is not connected");
        }
    }

    // ==================== Node Registry Operations ====================

    @Override
    public void registerNode(NodeInfo nodeInfo) {
        checkConnection();
        nodeRegistryCache.put(nodeInfo.getNodeId(), nodeInfo);
        log.info("📋 Node registered in Ignite: nodeId={}, grpcAddress={}",
                nodeInfo.getNodeId(), nodeInfo.getGrpcAddress());
    }

    @Override
    public void unregisterNode(String nodeId) {
        checkConnection();
        boolean removed = nodeRegistryCache.remove(nodeId);
        if (removed) {
            log.info("📋 Node unregistered from Ignite: nodeId={}", nodeId);
        }
    }

    @Override
    public Optional<NodeInfo> getNode(String nodeId) {
        checkConnection();
        return Optional.ofNullable(nodeRegistryCache.get(nodeId));
    }

    @Override
    public Collection<NodeInfo> getAllNodes() {
        checkConnection();
        List<NodeInfo> nodes = new ArrayList<>();
        try {
            for (var entry : getAllEntries(nodeRegistryCache)) {
                nodes.add(entry.getValue());
            }
        } catch (Exception e) {
            log.warn("Error getting all nodes: {}", e.getMessage());
        }
        return nodes;
    }

    @Override
    public void updateNodeHeartbeat(String nodeId) {
        checkConnection();
        NodeInfo nodeInfo = nodeRegistryCache.get(nodeId);
        if (nodeInfo != null) {
            nodeInfo.updateHeartbeat();
            nodeRegistryCache.put(nodeId, nodeInfo);
            log.debug("💓 Node heartbeat updated in Ignite: nodeId={}", nodeId);
        }
    }
}
