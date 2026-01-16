package com.osw.dmp.ns.grpc;

import com.osw.dmp.ns.config.NodeProperties;
import com.osw.dmp.ns.ignite.IgniteCacheService;
import com.osw.dmp.ns.model.NodeInfo;
import com.osw.dmp.ns.model.NodeStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * NodeRegistry 單元測試
 * 
 * 測試節點註冊表服務
 */
@DisplayName("NodeRegistry 單元測試")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class NodeRegistryTest {

    @Mock
    private NodeIdentity nodeIdentity;

    @Mock
    private NodeProperties nodeProperties;

    @Mock
    private NodeProperties.HeartbeatConfig heartbeatProperties;

    @Mock
    private IgniteCacheService igniteCache;

    private NodeRegistry nodeRegistry;

    private static final String CURRENT_NODE_ID = "current-node-001";
    private static final String OTHER_NODE_ID = "other-node-002";
    private static final String GRPC_ADDRESS = "localhost:9090";

    @BeforeEach
    void setUp() {
        when(nodeIdentity.getNodeId()).thenReturn(CURRENT_NODE_ID);
        when(nodeIdentity.getGrpcAddress()).thenReturn(GRPC_ADDRESS);
        when(nodeIdentity.getStartedAtMillis()).thenReturn(System.currentTimeMillis());
        when(nodeProperties.getHeartbeat()).thenReturn(heartbeatProperties);
        when(heartbeatProperties.getIntervalMs()).thenReturn(5000L);
        when(heartbeatProperties.getTimeoutMs()).thenReturn(10000L);
        when(heartbeatProperties.getOfflineMs()).thenReturn(30000L);
    }

    @Nested
    @DisplayName("初始化測試")
    class InitTests {

        @Test
        @DisplayName("初始化時應註冊當前節點到 Ignite")
        void shouldRegisterCurrentNodeOnInit() {
            // Given
            nodeRegistry = new NodeRegistry(nodeIdentity, nodeProperties, igniteCache);

            // When
            nodeRegistry.init();

            // Then
            ArgumentCaptor<NodeInfo> captor = ArgumentCaptor.forClass(NodeInfo.class);
            verify(igniteCache).registerNode(captor.capture());

            NodeInfo registeredNode = captor.getValue();
            assertThat(registeredNode.getNodeId()).isEqualTo(CURRENT_NODE_ID);
            assertThat(registeredNode.getGrpcAddress()).isEqualTo(GRPC_ADDRESS);
            assertThat(registeredNode.getStatus()).isEqualTo(NodeStatus.ONLINE);
        }
    }

    @Nested
    @DisplayName("關閉測試")
    class ShutdownTests {

        @Test
        @DisplayName("關閉時應從 Ignite 註銷當前節點")
        void shouldUnregisterCurrentNodeOnShutdown() {
            // Given
            nodeRegistry = new NodeRegistry(nodeIdentity, nodeProperties, igniteCache);
            nodeRegistry.init();

            // When
            nodeRegistry.shutdown();

            // Then
            verify(igniteCache).unregisterNode(CURRENT_NODE_ID);
        }
    }

    @Nested
    @DisplayName("節點註冊測試")
    class RegisterNodeTests {

        @BeforeEach
        void initRegistry() {
            nodeRegistry = new NodeRegistry(nodeIdentity, nodeProperties, igniteCache);
            nodeRegistry.init();
            reset(igniteCache); // 重置 mock，清除 init 時的呼叫記錄
        }

        @Test
        @DisplayName("應能註冊外部節點")
        void shouldRegisterExternalNode() {
            // Given
            NodeInfo externalNode = NodeInfo.builder()
                    .nodeId(OTHER_NODE_ID)
                    .grpcAddress("other-host:9090")
                    .status(NodeStatus.ONLINE)
                    .build();

            // When
            nodeRegistry.registerNode(externalNode);

            // Then
            verify(igniteCache).registerNode(externalNode);
        }

        @Test
        @DisplayName("應能註銷外部節點")
        void shouldUnregisterExternalNode() {
            // When
            nodeRegistry.unregisterNode(OTHER_NODE_ID);

            // Then
            verify(igniteCache).unregisterNode(OTHER_NODE_ID);
        }
    }

    @Nested
    @DisplayName("節點查詢測試")
    class QueryNodeTests {

        @BeforeEach
        void initRegistry() {
            nodeRegistry = new NodeRegistry(nodeIdentity, nodeProperties, igniteCache);
            nodeRegistry.init();
        }

        @Test
        @DisplayName("getNode 應委託給 igniteCache")
        void shouldDelegateGetNodeToIgniteCache() {
            // Given
            NodeInfo expectedNode = NodeInfo.builder()
                    .nodeId(OTHER_NODE_ID)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(igniteCache.getNode(OTHER_NODE_ID)).thenReturn(Optional.of(expectedNode));

            // When
            Optional<NodeInfo> result = nodeRegistry.getNode(OTHER_NODE_ID);

            // Then
            assertThat(result).isPresent();
            assertThat(result.get().getNodeId()).isEqualTo(OTHER_NODE_ID);
        }

        @Test
        @DisplayName("getAllNodes 應委託給 igniteCache")
        void shouldDelegateGetAllNodesToIgniteCache() {
            // Given
            NodeInfo node1 = NodeInfo.builder().nodeId("node-1").status(NodeStatus.ONLINE).build();
            NodeInfo node2 = NodeInfo.builder().nodeId("node-2").status(NodeStatus.ONLINE).build();
            when(igniteCache.getAllNodes()).thenReturn(Arrays.asList(node1, node2));

            // When
            Collection<NodeInfo> result = nodeRegistry.getAllNodes();

            // Then
            assertThat(result).hasSize(2);
        }

        @Test
        @DisplayName("getOnlineNodes 應只回傳 ONLINE 狀態的節點")
        void shouldReturnOnlyOnlineNodes() {
            // Given
            NodeInfo onlineNode = NodeInfo.builder().nodeId("node-1").status(NodeStatus.ONLINE).build();
            NodeInfo suspectedNode = NodeInfo.builder().nodeId("node-2").status(NodeStatus.SUSPECTED).build();
            NodeInfo offlineNode = NodeInfo.builder().nodeId("node-3").status(NodeStatus.OFFLINE).build();
            when(igniteCache.getAllNodes()).thenReturn(Arrays.asList(onlineNode, suspectedNode, offlineNode));

            // When
            Collection<NodeInfo> result = nodeRegistry.getOnlineNodes();

            // Then
            assertThat(result).hasSize(1);
            assertThat(result.iterator().next().getNodeId()).isEqualTo("node-1");
        }

        @Test
        @DisplayName("getAvailableNodes 應回傳 ONLINE 和 SUSPECTED 狀態的節點")
        void shouldReturnAvailableNodes() {
            // Given
            NodeInfo onlineNode = NodeInfo.builder().nodeId("node-1").status(NodeStatus.ONLINE).build();
            NodeInfo suspectedNode = NodeInfo.builder().nodeId("node-2").status(NodeStatus.SUSPECTED).build();
            NodeInfo offlineNode = NodeInfo.builder().nodeId("node-3").status(NodeStatus.OFFLINE).build();
            when(igniteCache.getAllNodes()).thenReturn(Arrays.asList(onlineNode, suspectedNode, offlineNode));

            // When
            Collection<NodeInfo> result = nodeRegistry.getAvailableNodes();

            // Then
            assertThat(result).hasSize(2);
        }
    }

    @Nested
    @DisplayName("心跳更新測試")
    class HeartbeatUpdateTests {

        @BeforeEach
        void initRegistry() {
            nodeRegistry = new NodeRegistry(nodeIdentity, nodeProperties, igniteCache);
            nodeRegistry.init();
        }

        @Test
        @DisplayName("updateNodeHeartbeat 應委託給 igniteCache")
        void shouldDelegateUpdateHeartbeatToIgniteCache() {
            // When
            nodeRegistry.updateNodeHeartbeat(OTHER_NODE_ID);

            // Then
            verify(igniteCache).updateNodeHeartbeat(OTHER_NODE_ID);
        }
    }

    @Nested
    @DisplayName("統計查詢測試")
    class StatisticsTests {

        @BeforeEach
        void initRegistry() {
            nodeRegistry = new NodeRegistry(nodeIdentity, nodeProperties, igniteCache);
            nodeRegistry.init();
        }

        @Test
        @DisplayName("getNodeCountByStatus 應正確分組統計")
        void shouldGroupNodesByStatus() {
            // Given
            NodeInfo node1 = NodeInfo.builder().nodeId("node-1").status(NodeStatus.ONLINE).build();
            NodeInfo node2 = NodeInfo.builder().nodeId("node-2").status(NodeStatus.ONLINE).build();
            NodeInfo node3 = NodeInfo.builder().nodeId("node-3").status(NodeStatus.SUSPECTED).build();
            when(igniteCache.getAllNodes()).thenReturn(Arrays.asList(node1, node2, node3));

            // When
            Map<NodeStatus, Long> result = nodeRegistry.getNodeCountByStatus();

            // Then
            assertThat(result.get(NodeStatus.ONLINE)).isEqualTo(2L);
            assertThat(result.get(NodeStatus.SUSPECTED)).isEqualTo(1L);
        }

        @Test
        @DisplayName("getTotalNodeCount 應回傳正確數量")
        void shouldReturnCorrectTotalCount() {
            // Given
            NodeInfo node1 = NodeInfo.builder().nodeId("node-1").status(NodeStatus.ONLINE).build();
            NodeInfo node2 = NodeInfo.builder().nodeId("node-2").status(NodeStatus.OFFLINE).build();
            when(igniteCache.getAllNodes()).thenReturn(Arrays.asList(node1, node2));

            // When
            int result = nodeRegistry.getTotalNodeCount();

            // Then
            assertThat(result).isEqualTo(2);
        }

        @Test
        @DisplayName("空叢集時 getTotalNodeCount 應回傳 0")
        void shouldReturnZeroForEmptyCluster() {
            // Given
            when(igniteCache.getAllNodes()).thenReturn(Collections.emptyList());

            // When
            int result = nodeRegistry.getTotalNodeCount();

            // Then
            assertThat(result).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("當前節點統計更新測試")
    class UpdateCurrentNodeStatsTests {

        @BeforeEach
        void initRegistry() {
            nodeRegistry = new NodeRegistry(nodeIdentity, nodeProperties, igniteCache);
            nodeRegistry.init();
            reset(igniteCache);
        }

        @Test
        @DisplayName("updateCurrentNodeStats 應更新當前節點統計")
        void shouldUpdateCurrentNodeStats() {
            // Given
            NodeInfo currentNode = NodeInfo.builder()
                    .nodeId(CURRENT_NODE_ID)
                    .activeConnections(0)
                    .activeSubscriptions(0)
                    .build();
            when(igniteCache.getNode(CURRENT_NODE_ID)).thenReturn(Optional.of(currentNode));

            // When
            nodeRegistry.updateCurrentNodeStats(10, 5);

            // Then
            assertThat(currentNode.getActiveConnections()).isEqualTo(10);
            assertThat(currentNode.getActiveSubscriptions()).isEqualTo(5);
            verify(igniteCache).registerNode(currentNode);
        }

        @Test
        @DisplayName("當前節點不存在時不應拋出異常")
        void shouldNotThrowWhenCurrentNodeNotFound() {
            // Given
            when(igniteCache.getNode(CURRENT_NODE_ID)).thenReturn(Optional.empty());

            // When & Then - 不應拋出異常
            nodeRegistry.updateCurrentNodeStats(10, 5);
            verify(igniteCache, never()).registerNode(any());
        }
    }
}
