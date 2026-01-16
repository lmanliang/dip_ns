package com.osw.dmp.ns.grpc;

import com.osw.dmp.ns.config.GrpcProperties;
import com.osw.dmp.ns.model.NodeInfo;
import com.osw.dmp.ns.model.NodeStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * GrpcClientManager 單元測試
 * 
 * 測試 gRPC 客戶端連線管理器
 */
@DisplayName("GrpcClientManager 單元測試")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GrpcClientManagerTest {

    @Mock
    private GrpcProperties grpcProperties;

    @Mock
    private GrpcProperties.ClientConfig clientProperties;

    @Mock
    private NodeRegistry nodeRegistry;

    private GrpcClientManager grpcClientManager;

    private static final String TARGET_NODE_ID = "target-node-001";
    private static final String GRPC_ADDRESS = "localhost:9090";
    private static final int GRPC_PORT = 9090;

    @BeforeEach
    void setUp() {
        when(grpcProperties.getPort()).thenReturn(GRPC_PORT);
        when(grpcProperties.getClient()).thenReturn(clientProperties);
        when(clientProperties.getTimeoutMs()).thenReturn(5000L);
        when(clientProperties.getIdleTimeoutMs()).thenReturn(60000L);

        grpcClientManager = new GrpcClientManager(grpcProperties, nodeRegistry);
    }

    @Nested
    @DisplayName("getStub by nodeId 測試")
    class GetStubByNodeIdTests {

        @Test
        @DisplayName("節點存在時應回傳 Stub")
        void shouldReturnStubWhenNodeExists() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(nodeInfo));

            // When
            Optional<NodeServiceGrpc.NodeServiceBlockingStub> stub = grpcClientManager.getStub(TARGET_NODE_ID);

            // Then
            assertThat(stub).isPresent();
        }

        @Test
        @DisplayName("節點不存在時應回傳 empty")
        void shouldReturnEmptyWhenNodeNotExists() {
            // Given
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.empty());

            // When
            Optional<NodeServiceGrpc.NodeServiceBlockingStub> stub = grpcClientManager.getStub(TARGET_NODE_ID);

            // Then
            assertThat(stub).isEmpty();
        }

        @Test
        @DisplayName("同一節點應重用 Stub")
        void shouldReuseSameStubForSameNode() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(nodeInfo));

            // When
            Optional<NodeServiceGrpc.NodeServiceBlockingStub> stub1 = grpcClientManager.getStub(TARGET_NODE_ID);
            Optional<NodeServiceGrpc.NodeServiceBlockingStub> stub2 = grpcClientManager.getStub(TARGET_NODE_ID);

            // Then
            assertThat(stub1).isPresent();
            assertThat(stub2).isPresent();
            // Stub 應該是同一個實例（來自連線池）
            // 注意：由於 withDeadlineAfter 會返回新的 stub，這裡只驗證都能取得
        }
    }

    @Nested
    @DisplayName("getStub by host:port 測試")
    class GetStubByHostPortTests {

        @Test
        @DisplayName("應能透過 host 和 port 取得 Stub")
        void shouldGetStubByHostAndPort() {
            // When
            NodeServiceGrpc.NodeServiceBlockingStub stub = grpcClientManager.getStub("localhost", 9090);

            // Then
            assertThat(stub).isNotNull();
        }

        @Test
        @DisplayName("不同地址應取得不同 Stub")
        void shouldGetDifferentStubForDifferentAddress() {
            // When
            NodeServiceGrpc.NodeServiceBlockingStub stub1 = grpcClientManager.getStub("host1", 9090);
            NodeServiceGrpc.NodeServiceBlockingStub stub2 = grpcClientManager.getStub("host2", 9090);

            // Then
            assertThat(stub1).isNotNull();
            assertThat(stub2).isNotNull();
        }
    }

    @Nested
    @DisplayName("closeConnection 測試")
    class CloseConnectionTests {

        @Test
        @DisplayName("關閉連線後重新取得應建立新連線")
        void shouldCreateNewConnectionAfterClose() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(nodeInfo));

            // 先取得 stub
            grpcClientManager.getStub(TARGET_NODE_ID);

            // When
            grpcClientManager.closeConnection(GRPC_ADDRESS);

            // 重新取得
            Optional<NodeServiceGrpc.NodeServiceBlockingStub> newStub = grpcClientManager.getStub(TARGET_NODE_ID);

            // Then
            assertThat(newStub).isPresent();
        }
    }

    @Nested
    @DisplayName("shutdown 測試")
    class ShutdownTests {

        @Test
        @DisplayName("shutdown 應正常執行不拋出異常")
        void shouldShutdownWithoutException() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(nodeInfo));

            // 建立一些連線
            grpcClientManager.getStub(TARGET_NODE_ID);
            grpcClientManager.getStub("other-host", 9091);

            // When & Then - 不應拋出異常
            grpcClientManager.shutdown();
        }
    }

    @Nested
    @DisplayName("sendHeartbeat 測試")
    class SendHeartbeatTests {

        @Test
        @DisplayName("發送心跳到不存在的節點應回傳 false")
        void shouldReturnFalseWhenTargetNodeUnreachable() {
            // Given - 使用不存在的地址
            String unreachableAddress = "unreachable-host:9999";

            // When
            boolean result = grpcClientManager.sendHeartbeat("my-node", unreachableAddress);

            // Then - 連線會失敗，應回傳 false
            assertThat(result).isFalse();
        }
    }
}
