package com.osw.dmp.ns.ignite;

import com.osw.dmp.ns.config.IgniteConfig;
import com.osw.dmp.ns.model.NotificationLog;
import com.osw.dmp.ns.model.NotificationLog.PushStatus;
import com.osw.dmp.ns.model.ResultPayload;
import com.osw.dmp.ns.model.StatusCode;
import com.osw.dmp.ns.model.SubscriptionRouting;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SimulatedIgniteCache 單元測試
 * 
 * 測試模擬 Ignite 快取服務:
 * - Subscription Routing 操作
 * - Notification Log 操作
 * - 冪等性保證
 * - 狀態轉換
 */
@DisplayName("SimulatedIgniteCache 單元測試")
class SimulatedIgniteCacheTest {

    private SimulatedIgniteCache igniteCache;

    private static final String TEST_EVENT_ID = "test-event-123";
    private static final String TEST_NODE_ID = "node-001";
    private static final String TEST_SESSION_ID = "session-001";
    private static final String KEY_PREFIX = "ns";

    @BeforeEach
    void setUp() {
        // 建立配置
        IgniteConfig config = new IgniteConfig();
        config.setKeyPrefix(KEY_PREFIX);

        // 設定 subscription routing cache
        IgniteConfig.CacheConfig subscriptionRoutingConfig = new IgniteConfig.CacheConfig();
        subscriptionRoutingConfig.setName("subscription-routing");
        subscriptionRoutingConfig.setBackups(1);
        config.setSubscriptionRouting(subscriptionRoutingConfig);

        // 設定 notification log cache
        IgniteConfig.CacheConfig notifLogConfig = new IgniteConfig.CacheConfig();
        notifLogConfig.setName("notification-logs");
        notifLogConfig.setBackups(1);
        config.setNotificationLog(notifLogConfig);

        igniteCache = new SimulatedIgniteCache(config);
        igniteCache.init();
    }

    // ==================== Subscription Routing 測試 ====================

    @Nested
    @DisplayName("Subscription Routing 操作")
    class SubscriptionRoutingTests {

        @Test
        @DisplayName("儲存與取得 Routing 應正確運作")
        void shouldPutAndGetRouting() {
            // Given
            SubscriptionRouting routing = createRouting(TEST_EVENT_ID, TEST_NODE_ID);

            // When
            igniteCache.putSubscriptionRouting(TEST_EVENT_ID, routing);
            Optional<SubscriptionRouting> result = igniteCache.getSubscriptionRouting(TEST_EVENT_ID);

            // Then
            assertThat(result).isPresent();
            assertThat(result.get().getEventId()).isEqualTo(TEST_EVENT_ID);
            assertThat(result.get().getNodeId()).isEqualTo(TEST_NODE_ID);
            assertThat(result.get().getSessionId()).isEqualTo(TEST_SESSION_ID);
        }

        @Test
        @DisplayName("取得不存在的 Routing 應返回空")
        void shouldReturnEmptyForNonExistentRouting() {
            // When
            Optional<SubscriptionRouting> result = igniteCache.getSubscriptionRouting("non-existent");

            // Then
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("移除 Routing 應正確運作")
        void shouldRemoveRouting() {
            // Given
            SubscriptionRouting routing = createRouting(TEST_EVENT_ID, TEST_NODE_ID);
            igniteCache.putSubscriptionRouting(TEST_EVENT_ID, routing);

            // When
            igniteCache.removeSubscriptionRouting(TEST_EVENT_ID);

            // Then
            assertThat(igniteCache.getSubscriptionRouting(TEST_EVENT_ID)).isEmpty();
        }

        @Test
        @DisplayName("根據 SessionId 移除所有 Routing")
        void shouldRemoveRoutingsBySessionId() {
            // Given
            igniteCache.putSubscriptionRouting("event-1", createRouting("event-1", TEST_NODE_ID, "session-A"));
            igniteCache.putSubscriptionRouting("event-2", createRouting("event-2", TEST_NODE_ID, "session-A"));
            igniteCache.putSubscriptionRouting("event-3", createRouting("event-3", TEST_NODE_ID, "session-B"));

            // When
            igniteCache.removeRoutingsBySessionId("session-A");

            // Then
            assertThat(igniteCache.getSubscriptionRouting("event-1")).isEmpty();
            assertThat(igniteCache.getSubscriptionRouting("event-2")).isEmpty();
            assertThat(igniteCache.getSubscriptionRouting("event-3")).isPresent();
        }

        @Test
        @DisplayName("根據 NodeId 移除所有 Routing")
        void shouldRemoveRoutingsByNodeId() {
            // Given
            igniteCache.putSubscriptionRouting("event-1", createRouting("event-1", "node-A"));
            igniteCache.putSubscriptionRouting("event-2", createRouting("event-2", "node-A"));
            igniteCache.putSubscriptionRouting("event-3", createRouting("event-3", "node-B"));

            // When
            igniteCache.removeRoutingsByNodeId("node-A");

            // Then
            assertThat(igniteCache.getSubscriptionRouting("event-1")).isEmpty();
            assertThat(igniteCache.getSubscriptionRouting("event-2")).isEmpty();
            assertThat(igniteCache.getSubscriptionRouting("event-3")).isPresent();
        }

        @Test
        @DisplayName("計算總訂閱數量應正確")
        void shouldCalculateTotalSubscriptionCount() {
            // Given
            igniteCache.putSubscriptionRouting("event-1", createRouting("event-1", TEST_NODE_ID));
            igniteCache.putSubscriptionRouting("event-2", createRouting("event-2", TEST_NODE_ID));
            igniteCache.putSubscriptionRouting("event-3", createRouting("event-3", TEST_NODE_ID));

            // When
            int count = igniteCache.getTotalSubscriptionCount();

            // Then
            assertThat(count).isEqualTo(3);
        }

        @Test
        @DisplayName("計算各節點訂閱數量應正確")
        void shouldCalculateSubscriptionCountByNode() {
            // Given
            igniteCache.putSubscriptionRouting("event-1", createRouting("event-1", "node-A"));
            igniteCache.putSubscriptionRouting("event-2", createRouting("event-2", "node-A"));
            igniteCache.putSubscriptionRouting("event-3", createRouting("event-3", "node-B"));

            // When
            Map<String, Long> countByNode = igniteCache.getSubscriptionCountByNode();

            // Then
            assertThat(countByNode).hasSize(2);
            assertThat(countByNode.get("node-A")).isEqualTo(2);
            assertThat(countByNode.get("node-B")).isEqualTo(1);
        }

        @Test
        @DisplayName("計算慢等待訂閱數量應正確")
        void shouldCalculateSlowWaitingCount() {
            // Given - 建立一個很舊的訂閱
            SubscriptionRouting oldRouting = SubscriptionRouting.builder()
                    .eventId("old-event")
                    .nodeId(TEST_NODE_ID)
                    .sessionId(TEST_SESSION_ID)
                    .subscribedAtMillis(System.currentTimeMillis() - Duration.ofMinutes(10).toMillis())
                    .build();
            igniteCache.putSubscriptionRouting("old-event", oldRouting);

            // 建立一個新的訂閱
            igniteCache.putSubscriptionRouting("new-event", createRouting("new-event", TEST_NODE_ID));

            // When
            int slowCount = igniteCache.getSlowWaitingCount(Duration.ofMinutes(5));

            // Then
            assertThat(slowCount).isEqualTo(1);
        }
    }

    // ==================== Notification Log 測試 ====================

    @Nested
    @DisplayName("Notification Log 操作")
    class NotificationLogTests {

        @Test
        @DisplayName("markPending 應建立 PENDING 狀態的 Log")
        void shouldCreatePendingLog() {
            // Given
            ResultPayload payload = createPayload(StatusCode.SUCCESS, "處理成功");

            // When
            igniteCache.markPending(TEST_EVENT_ID, payload);

            // Then
            Optional<NotificationLog> log = igniteCache.getNotificationLog(TEST_EVENT_ID);
            assertThat(log).isPresent();
            assertThat(log.get().getPushStatus()).isEqualTo(PushStatus.PENDING);
            assertThat(log.get().getResultPayload().getStatus()).isEqualTo(StatusCode.SUCCESS);
        }

        @Test
        @DisplayName("markSent 應更新為 SENT 狀態")
        void shouldMarkAsSent() {
            // Given
            igniteCache.markPending(TEST_EVENT_ID, createPayload(StatusCode.SUCCESS, ""));

            // When
            igniteCache.markSent(TEST_EVENT_ID);

            // Then
            Optional<NotificationLog> log = igniteCache.getNotificationLog(TEST_EVENT_ID);
            assertThat(log).isPresent();
            assertThat(log.get().getPushStatus()).isEqualTo(PushStatus.SENT);
        }

        @Test
        @DisplayName("markFailed 應更新為 FAILED 狀態")
        void shouldMarkAsFailed() {
            // Given
            igniteCache.markPending(TEST_EVENT_ID, createPayload(StatusCode.SUCCESS, ""));

            // When
            igniteCache.markFailed(TEST_EVENT_ID);

            // Then
            Optional<NotificationLog> log = igniteCache.getNotificationLog(TEST_EVENT_ID);
            assertThat(log).isPresent();
            assertThat(log.get().getPushStatus()).isEqualTo(PushStatus.FAILED);
        }

        @Test
        @DisplayName("isAlreadySent 應正確判斷")
        void shouldCheckIfAlreadySent() {
            // Given
            igniteCache.markPending(TEST_EVENT_ID, createPayload(StatusCode.SUCCESS, ""));

            // Then - PENDING 狀態
            assertThat(igniteCache.isAlreadySent(TEST_EVENT_ID)).isFalse();

            // When
            igniteCache.markSent(TEST_EVENT_ID);

            // Then - SENT 狀態
            assertThat(igniteCache.isAlreadySent(TEST_EVENT_ID)).isTrue();
        }

        @Test
        @DisplayName("不存在的 eventId 應視為未發送")
        void shouldReturnFalseForNonExistentEventId() {
            // When & Then
            assertThat(igniteCache.isAlreadySent("non-existent")).isFalse();
        }

        @Test
        @DisplayName("findPendingLog 應只返回 PENDING 狀態的 Log")
        void shouldFindOnlyPendingLog() {
            // Given
            igniteCache.markPending(TEST_EVENT_ID, createPayload(StatusCode.SUCCESS, ""));

            // When - PENDING 狀態
            Optional<NotificationLog> pendingLog = igniteCache.findPendingLog(TEST_EVENT_ID);
            assertThat(pendingLog).isPresent();

            // Given - 更新為 SENT
            igniteCache.markSent(TEST_EVENT_ID);

            // When - SENT 狀態
            Optional<NotificationLog> sentLog = igniteCache.findPendingLog(TEST_EVENT_ID);
            assertThat(sentLog).isEmpty();
        }
    }

    // ==================== 冪等性測試 ====================

    @Nested
    @DisplayName("冪等性保證")
    class IdempotencyTests {

        @Test
        @DisplayName("putIfAbsent 應保證冪等性")
        void shouldGuaranteeIdempotencyWithPutIfAbsent() {
            // Given
            NotificationLog log1 = NotificationLog.builder()
                    .eventId(TEST_EVENT_ID)
                    .pushStatus(PushStatus.PENDING)
                    .resultPayload(createPayload(StatusCode.SUCCESS, "第一次"))
                    .attempts(0)
                    .createdAt(System.currentTimeMillis())
                    .build();

            NotificationLog log2 = NotificationLog.builder()
                    .eventId(TEST_EVENT_ID)
                    .pushStatus(PushStatus.PENDING)
                    .resultPayload(createPayload(StatusCode.SUCCESS, "第二次"))
                    .attempts(0)
                    .createdAt(System.currentTimeMillis())
                    .build();

            // When
            Optional<NotificationLog> first = igniteCache.putIfAbsent(log1);
            Optional<NotificationLog> second = igniteCache.putIfAbsent(log2);

            // Then
            assertThat(first).isEmpty(); // 第一次插入成功，返回空
            assertThat(second).isPresent(); // 第二次插入失敗，返回已存在的
            assertThat(second.get().getResultPayload().getMessage()).isEqualTo("第一次");
        }

        @Test
        @DisplayName("多次 markSent 應安全")
        void shouldHandleMultipleMarkSent() {
            // Given
            igniteCache.markPending(TEST_EVENT_ID, createPayload(StatusCode.SUCCESS, ""));

            // When - 多次呼叫 markSent
            igniteCache.markSent(TEST_EVENT_ID);
            igniteCache.markSent(TEST_EVENT_ID);
            igniteCache.markSent(TEST_EVENT_ID);

            // Then
            assertThat(igniteCache.isAlreadySent(TEST_EVENT_ID)).isTrue();
        }
    }

    // ==================== 統計測試 ====================

    @Nested
    @DisplayName("統計功能")
    class StatisticsTests {

        @Test
        @DisplayName("getNotificationStats 應正確統計各狀態數量")
        void shouldCalculateNotificationStats() {
            // Given
            igniteCache.markPending("event-1", createPayload(StatusCode.SUCCESS, ""));
            igniteCache.markPending("event-2", createPayload(StatusCode.SUCCESS, ""));
            igniteCache.markPending("event-3", createPayload(StatusCode.SUCCESS, ""));

            igniteCache.markSent("event-1");
            igniteCache.markFailed("event-2");
            // event-3 保持 PENDING

            // When
            Map<PushStatus, Long> stats = igniteCache.getNotificationStats();

            // Then
            assertThat(stats.get(PushStatus.SENT)).isEqualTo(1);
            assertThat(stats.get(PushStatus.FAILED)).isEqualTo(1);
            assertThat(stats.get(PushStatus.PENDING)).isEqualTo(1);
        }
    }

    // ==================== 連線狀態測試 ====================

    @Nested
    @DisplayName("連線狀態")
    class ConnectionTests {

        @Test
        @DisplayName("初始狀態應為已連線")
        void shouldBeConnectedInitially() {
            assertThat(igniteCache.isConnected()).isTrue();
        }

        @Test
        @DisplayName("可以設定連線狀態")
        void shouldSetConnectionStatus() {
            // When
            igniteCache.setConnected(false);

            // Then
            assertThat(igniteCache.isConnected()).isFalse();

            // When
            igniteCache.setConnected(true);

            // Then
            assertThat(igniteCache.isConnected()).isTrue();
        }
    }

    // ==================== Node Registry 測試 ====================

    @Nested
    @DisplayName("Node Registry 操作")
    class NodeRegistryTests {

        @Test
        @DisplayName("註冊與取得節點應正確運作")
        void shouldRegisterAndGetNode() {
            // Given
            com.osw.dmp.ns.model.NodeInfo nodeInfo = com.osw.dmp.ns.model.NodeInfo.builder()
                    .nodeId("node-001")
                    .grpcAddress("localhost:9090")
                    .startedAtMillis(System.currentTimeMillis())
                    .lastHeartbeatMillis(System.currentTimeMillis())
                    .status(com.osw.dmp.ns.model.NodeStatus.ONLINE)
                    .activeConnections(10)
                    .activeSubscriptions(5)
                    .build();

            // When
            igniteCache.registerNode(nodeInfo);
            Optional<com.osw.dmp.ns.model.NodeInfo> result = igniteCache.getNode("node-001");

            // Then
            assertThat(result).isPresent();
            assertThat(result.get().getNodeId()).isEqualTo("node-001");
            assertThat(result.get().getGrpcAddress()).isEqualTo("localhost:9090");
            assertThat(result.get().getStatus()).isEqualTo(com.osw.dmp.ns.model.NodeStatus.ONLINE);
        }

        @Test
        @DisplayName("註銷節點應移除該節點")
        void shouldUnregisterNode() {
            // Given
            com.osw.dmp.ns.model.NodeInfo nodeInfo = com.osw.dmp.ns.model.NodeInfo.builder()
                    .nodeId("node-001")
                    .grpcAddress("localhost:9090")
                    .status(com.osw.dmp.ns.model.NodeStatus.ONLINE)
                    .build();
            igniteCache.registerNode(nodeInfo);

            // When
            igniteCache.unregisterNode("node-001");

            // Then
            assertThat(igniteCache.getNode("node-001")).isEmpty();
        }

        @Test
        @DisplayName("取得所有節點應回傳完整列表")
        void shouldGetAllNodes() {
            // Given
            com.osw.dmp.ns.model.NodeInfo node1 = com.osw.dmp.ns.model.NodeInfo.builder()
                    .nodeId("node-001")
                    .status(com.osw.dmp.ns.model.NodeStatus.ONLINE)
                    .build();
            com.osw.dmp.ns.model.NodeInfo node2 = com.osw.dmp.ns.model.NodeInfo.builder()
                    .nodeId("node-002")
                    .status(com.osw.dmp.ns.model.NodeStatus.SUSPECTED)
                    .build();

            igniteCache.registerNode(node1);
            igniteCache.registerNode(node2);

            // When
            java.util.Collection<com.osw.dmp.ns.model.NodeInfo> allNodes = igniteCache.getAllNodes();

            // Then
            assertThat(allNodes).hasSize(2);
        }

        @Test
        @DisplayName("更新節點心跳應更新時間戳和狀態")
        void shouldUpdateNodeHeartbeat() {
            // Given
            long oldTime = System.currentTimeMillis() - 10000;
            com.osw.dmp.ns.model.NodeInfo nodeInfo = com.osw.dmp.ns.model.NodeInfo.builder()
                    .nodeId("node-001")
                    .lastHeartbeatMillis(oldTime)
                    .status(com.osw.dmp.ns.model.NodeStatus.SUSPECTED)
                    .build();
            igniteCache.registerNode(nodeInfo);

            // When
            long beforeUpdate = System.currentTimeMillis();
            igniteCache.updateNodeHeartbeat("node-001");

            // Then
            com.osw.dmp.ns.model.NodeInfo updated = igniteCache.getNode("node-001").orElseThrow();
            assertThat(updated.getLastHeartbeatMillis()).isGreaterThanOrEqualTo(beforeUpdate);
            assertThat(updated.getStatus()).isEqualTo(com.osw.dmp.ns.model.NodeStatus.ONLINE);
        }

        @Test
        @DisplayName("更新不存在節點的心跳不應拋出異常")
        void shouldNotThrowWhenUpdatingHeartbeatForNonExistentNode() {
            // When & Then - 不應拋出異常
            igniteCache.updateNodeHeartbeat("non-existent-node");
        }

        @Test
        @DisplayName("取得不存在的節點應回傳 empty")
        void shouldReturnEmptyForNonExistentNode() {
            // When
            Optional<com.osw.dmp.ns.model.NodeInfo> result = igniteCache.getNode("non-existent");

            // Then
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("重新註冊應更新節點資訊")
        void shouldUpdateNodeInfoOnReRegister() {
            // Given
            com.osw.dmp.ns.model.NodeInfo original = com.osw.dmp.ns.model.NodeInfo.builder()
                    .nodeId("node-001")
                    .grpcAddress("localhost:9090")
                    .activeConnections(5)
                    .build();
            igniteCache.registerNode(original);

            // When
            com.osw.dmp.ns.model.NodeInfo updated = com.osw.dmp.ns.model.NodeInfo.builder()
                    .nodeId("node-001")
                    .grpcAddress("localhost:9091")
                    .activeConnections(10)
                    .build();
            igniteCache.registerNode(updated);

            // Then
            com.osw.dmp.ns.model.NodeInfo result = igniteCache.getNode("node-001").orElseThrow();
            assertThat(result.getGrpcAddress()).isEqualTo("localhost:9091");
            assertThat(result.getActiveConnections()).isEqualTo(10);
        }
    }

    // ==================== 輔助方法 ====================

    private SubscriptionRouting createRouting(String eventId, String nodeId) {
        return createRouting(eventId, nodeId, TEST_SESSION_ID);
    }

    private SubscriptionRouting createRouting(String eventId, String nodeId, String sessionId) {
        return SubscriptionRouting.builder()
                .eventId(eventId)
                .nodeId(nodeId)
                .sessionId(sessionId)
                .subscribedAtMillis(System.currentTimeMillis())
                .build();
    }

    private ResultPayload createPayload(int status, String message) {
        return ResultPayload.builder()
                .status(status)
                .message(message)
                .build();
    }
}
