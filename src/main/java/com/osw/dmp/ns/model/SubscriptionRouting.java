package com.osw.dmp.ns.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Subscription Routing for Ignite Cache
 * Maps eventId -> nodeId for cross-node WebSocket push
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubscriptionRouting implements Serializable {

    private String eventId;
    private String nodeId;
    private String grpcAddress; // 節點 gRPC 地址（用於跨節點推送）
    private String sessionId;
    private long subscribedAtMillis; // Epoch millis for better Ignite serialization compatibility
}
