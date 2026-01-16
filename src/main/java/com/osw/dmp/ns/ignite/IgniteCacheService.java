package com.osw.dmp.ns.ignite;

import com.osw.dmp.ns.model.NodeInfo;
import com.osw.dmp.ns.model.NotificationLog;
import com.osw.dmp.ns.model.ResultPayload;
import com.osw.dmp.ns.model.SubscriptionRouting;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Ignite Cache Service Interface
 * 
 * Defines operations for both simulated and real Ignite implementations
 */
public interface IgniteCacheService {

    // ==================== Subscription Routing Operations ====================

    /**
     * Store subscription routing
     */
    void putSubscriptionRouting(String eventId, SubscriptionRouting routing);

    /**
     * Get subscription routing by eventId
     */
    Optional<SubscriptionRouting> getSubscriptionRouting(String eventId);

    /**
     * Remove subscription routing
     */
    void removeSubscriptionRouting(String eventId);

    /**
     * Remove all routings for a session
     */
    void removeRoutingsBySessionId(String sessionId);

    /**
     * Remove all routings for a node
     */
    void removeRoutingsByNodeId(String nodeId);

    /**
     * Get total subscription count
     */
    int getTotalSubscriptionCount();

    /**
     * Get count by node
     */
    Map<String, Long> getSubscriptionCountByNode();

    /**
     * Get slow waiting count (subscriptions older than threshold)
     */
    int getSlowWaitingCount(Duration threshold);

    // ==================== Notification Log Operations ====================

    /**
     * Try to put notification log (idempotent - putIfAbsent)
     * Returns existing log if already exists
     */
    Optional<NotificationLog> putIfAbsent(NotificationLog log);

    /**
     * Update notification log
     */
    void updateNotificationLog(NotificationLog log);

    /**
     * Get notification log by eventId
     */
    Optional<NotificationLog> getNotificationLog(String eventId);

    /**
     * Find pending notification log for eventId
     */
    Optional<NotificationLog> findPendingLog(String eventId);

    /**
     * Check if already sent
     */
    boolean isAlreadySent(String eventId);

    /**
     * Mark as sent
     */
    void markSent(String eventId);

    /**
     * Mark as pending with result payload
     */
    void markPending(String eventId, ResultPayload payload);

    /**
     * Mark as failed
     */
    void markFailed(String eventId);

    /**
     * Get notification stats
     */
    Map<NotificationLog.PushStatus, Long> getNotificationStats();

    // ==================== Health & Status ====================

    /**
     * Check if connected to Ignite
     */
    boolean isConnected();

    // ==================== Node Registry Operations ====================

    /**
     * Register or update a node in the cluster
     * 
     * @param nodeInfo the node information to register
     */
    void registerNode(NodeInfo nodeInfo);

    /**
     * Unregister a node from the cluster
     * 
     * @param nodeId the node ID to unregister
     */
    void unregisterNode(String nodeId);

    /**
     * Get node information by nodeId
     * 
     * @param nodeId the node ID to look up
     * @return the node information if found
     */
    Optional<NodeInfo> getNode(String nodeId);

    /**
     * Get all registered nodes
     * 
     * @return collection of all node information
     */
    Collection<NodeInfo> getAllNodes();

    /**
     * Update node heartbeat timestamp
     * 
     * @param nodeId the node ID to update
     */
    void updateNodeHeartbeat(String nodeId);
}
