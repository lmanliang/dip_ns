package com.osw.dmp.ns.grpc;

import com.osw.dmp.ns.config.NodeProperties;
import com.osw.dmp.ns.ignite.IgniteCacheService;
import com.osw.dmp.ns.model.NodeInfo;
import com.osw.dmp.ns.model.NodeStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 節點註冊表服務
 * 
 * 管理叢集中所有節點的資訊，包含：
 * - 節點註冊/註銷
 * - 心跳更新
 * - 節點狀態管理
 * 
 * 透過 Ignite Cache (ns-nodes) 實現節點間的互相發現：
 * - 所有節點共享同一份註冊表
 * - 節點啟動時自動註冊到 Ignite
 * - 透過定期心跳維持存活狀態
 * - 其他節點透過查詢 Ignite 發現叢集成員
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NodeRegistry {

    private final NodeIdentity nodeIdentity;
    private final NodeProperties nodeProperties;
    private final IgniteCacheService igniteCache;

    /**
     * 心跳檢查排程器
     */
    private ScheduledExecutorService heartbeatScheduler;

    @PostConstruct
    public void init() {
        // 註冊當前節點到 Ignite
        registerCurrentNode();

        // 啟動心跳排程
        startHeartbeatScheduler();

        log.info("📋 NodeRegistry initialized: nodeId={}", nodeIdentity.getNodeId());
    }

    @PreDestroy
    public void shutdown() {
        // 從 Ignite 註銷當前節點
        unregisterNode(nodeIdentity.getNodeId());

        // 關閉排程器
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdown();
        }

        log.info("📋 NodeRegistry shutdown: nodeId={}", nodeIdentity.getNodeId());
    }

    /**
     * 註冊當前節點到 Ignite
     */
    private void registerCurrentNode() {
        NodeInfo nodeInfo = NodeInfo.builder()
                .nodeId(nodeIdentity.getNodeId())
                .grpcAddress(nodeIdentity.getGrpcAddress())
                .startedAtMillis(nodeIdentity.getStartedAtMillis())
                .lastHeartbeatMillis(System.currentTimeMillis())
                .status(NodeStatus.ONLINE)
                .activeConnections(0)
                .activeSubscriptions(0)
                .build();

        igniteCache.registerNode(nodeInfo);
        log.info("📋 Current node registered to Ignite: {}", nodeInfo);
    }

    /**
     * 啟動心跳排程
     */
    private void startHeartbeatScheduler() {
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "node-heartbeat");
            t.setDaemon(true);
            return t;
        });

        long intervalMs = nodeProperties.getHeartbeat().getIntervalMs();

        // 定期更新心跳到 Ignite
        heartbeatScheduler.scheduleAtFixedRate(
                this::updateHeartbeat,
                intervalMs,
                intervalMs,
                TimeUnit.MILLISECONDS);

        // 定期檢查其他節點狀態
        heartbeatScheduler.scheduleAtFixedRate(
                this::checkNodeStatus,
                intervalMs * 2,
                intervalMs,
                TimeUnit.MILLISECONDS);

        log.info("💓 Heartbeat scheduler started: interval={}ms", intervalMs);
    }

    /**
     * 更新當前節點心跳到 Ignite
     */
    private void updateHeartbeat() {
        try {
            igniteCache.updateNodeHeartbeat(nodeIdentity.getNodeId());
            log.debug("💓 Heartbeat updated to Ignite: nodeId={}", nodeIdentity.getNodeId());
        } catch (Exception e) {
            log.warn("Failed to update heartbeat: {}", e.getMessage());
        }
    }

    /**
     * 檢查所有節點狀態
     * 從 Ignite 讀取所有節點，檢查心跳超時
     */
    private void checkNodeStatus() {
        long now = System.currentTimeMillis();
        long timeoutMs = nodeProperties.getHeartbeat().getTimeoutMs();
        long offlineMs = nodeProperties.getHeartbeat().getOfflineMs();

        try {
            for (NodeInfo node : igniteCache.getAllNodes()) {
                // 跳過當前節點
                if (node.getNodeId().equals(nodeIdentity.getNodeId())) {
                    continue;
                }

                long lastHeartbeat = node.getLastHeartbeatMillis();
                long elapsed = now - lastHeartbeat;

                if (elapsed > offlineMs && node.getStatus() != NodeStatus.OFFLINE) {
                    // 標記為離線
                    node.setStatus(NodeStatus.OFFLINE);
                    igniteCache.registerNode(node); // 更新狀態
                    log.warn("❌ Node marked OFFLINE: nodeId={}, elapsed={}ms", node.getNodeId(), elapsed);

                    // 清除該節點的所有 SubscriptionRouting
                    igniteCache.removeRoutingsByNodeId(node.getNodeId());

                } else if (elapsed > timeoutMs && node.getStatus() == NodeStatus.ONLINE) {
                    // 標記為疑似離線
                    node.setStatus(NodeStatus.SUSPECTED);
                    igniteCache.registerNode(node); // 更新狀態
                    log.warn("⚠️ Node marked SUSPECTED: nodeId={}, elapsed={}ms", node.getNodeId(), elapsed);
                }
            }
        } catch (Exception e) {
            log.warn("Error checking node status: {}", e.getMessage());
        }
    }

    /**
     * 註冊節點
     */
    public void registerNode(NodeInfo nodeInfo) {
        igniteCache.registerNode(nodeInfo);
        log.info("📋 Node registered: nodeId={}, grpcAddress={}",
                nodeInfo.getNodeId(), nodeInfo.getGrpcAddress());
    }

    /**
     * 註銷節點
     */
    public void unregisterNode(String nodeId) {
        igniteCache.unregisterNode(nodeId);
        log.info("📋 Node unregistered: nodeId={}", nodeId);
    }

    /**
     * 取得節點資訊
     */
    public Optional<NodeInfo> getNode(String nodeId) {
        return igniteCache.getNode(nodeId);
    }

    /**
     * 取得所有節點
     */
    public Collection<NodeInfo> getAllNodes() {
        return igniteCache.getAllNodes();
    }

    /**
     * 取得所有線上節點
     */
    public Collection<NodeInfo> getOnlineNodes() {
        return igniteCache.getAllNodes().stream()
                .filter(NodeInfo::isOnline)
                .collect(Collectors.toList());
    }

    /**
     * 取得可用於推送的節點
     */
    public Collection<NodeInfo> getAvailableNodes() {
        return igniteCache.getAllNodes().stream()
                .filter(NodeInfo::isAvailableForPush)
                .collect(Collectors.toList());
    }

    /**
     * 更新節點心跳（外部節點）
     */
    public void updateNodeHeartbeat(String nodeId) {
        igniteCache.updateNodeHeartbeat(nodeId);
        log.debug("💓 External node heartbeat updated: nodeId={}", nodeId);
    }

    /**
     * 更新當前節點統計
     */
    public void updateCurrentNodeStats(int activeConnections, int activeSubscriptions) {
        igniteCache.getNode(nodeIdentity.getNodeId()).ifPresent(nodeInfo -> {
            nodeInfo.setActiveConnections(activeConnections);
            nodeInfo.setActiveSubscriptions(activeSubscriptions);
            igniteCache.registerNode(nodeInfo);
        });
    }

    /**
     * 取得各狀態節點數量
     */
    public Map<NodeStatus, Long> getNodeCountByStatus() {
        return igniteCache.getAllNodes().stream()
                .collect(Collectors.groupingBy(NodeInfo::getStatus, Collectors.counting()));
    }

    /**
     * 取得節點總數
     */
    public int getTotalNodeCount() {
        return igniteCache.getAllNodes().size();
    }
}
