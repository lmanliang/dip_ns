package com.osw.dmp.ns.grpc;

import com.osw.dmp.ns.config.GrpcProperties;
import com.osw.dmp.ns.model.NodeInfo;
import com.osw.dmp.ns.model.NodeStatus;
import com.osw.dmp.ns.model.SubscriptionRouting;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * CrossNodePushService 單元測試
 * 
 * 測試跨節點推送服務
 */
@DisplayName("CrossNodePushService 單元測試")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CrossNodePushServiceTest {

    @Mock
    private NodeIdentity nodeIdentity;

    @Mock
    private NodeRegistry nodeRegistry;

    @Mock
    private GrpcClientManager grpcClientManager;

    @Mock
    private GrpcProperties grpcProperties;

    @Mock
    private GrpcProperties.ClientConfig clientProperties;

    @Mock
    private NodeServiceGrpc.NodeServiceBlockingStub mockStub;

    private MeterRegistry meterRegistry;
    private CrossNodePushService crossNodePushService;

    private static final String CURRENT_NODE_ID = "current-node-001";
    private static final String TARGET_NODE_ID = "target-node-002";
    private static final String TARGET_GRPC_ADDRESS = "target-host:9090";
    private static final String TEST_EVENT_ID = "test-event-123";
    private static final String TEST_SESSION_ID = "session-001";

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();

        when(nodeIdentity.getNodeId()).thenReturn(CURRENT_NODE_ID);
        when(grpcProperties.getClient()).thenReturn(clientProperties);
        when(clientProperties.getRetryCount()).thenReturn(2);
        when(clientProperties.getRetryDelayMs()).thenReturn(100L);

        crossNodePushService = new CrossNodePushService(
                nodeIdentity,
                nodeRegistry,
                grpcClientManager,
                grpcProperties,
                meterRegistry);
    }

    private SubscriptionRouting createRouting(String nodeId, String grpcAddress) {
        return SubscriptionRouting.builder()
                .eventId(TEST_EVENT_ID)
                .nodeId(nodeId)
                .grpcAddress(grpcAddress)
                .sessionId(TEST_SESSION_ID)
                .subscribedAtMillis(System.currentTimeMillis())
                .build();
    }

    @Nested
    @DisplayName("LOCAL_NODE 結果測試")
    class LocalNodeTests {

        @Test
        @DisplayName("目標為當前節點時應回傳 LOCAL_NODE")
        void shouldReturnLocalNodeWhenTargetIsCurrent() {
            // Given
            SubscriptionRouting routing = createRouting(CURRENT_NODE_ID, TARGET_GRPC_ADDRESS);
            when(nodeIdentity.isCurrentNode(CURRENT_NODE_ID)).thenReturn(true);

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.LOCAL_NODE);
            verifyNoInteractions(grpcClientManager);
        }
    }

    @Nested
    @DisplayName("NODE_UNREACHABLE 結果測試")
    class NodeUnreachableTests {

        @Test
        @DisplayName("目標節點為 OFFLINE 時應回傳 NODE_UNREACHABLE")
        void shouldReturnNodeUnreachableWhenTargetOffline() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, TARGET_GRPC_ADDRESS);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);

            NodeInfo offlineNode = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(TARGET_GRPC_ADDRESS)
                    .status(NodeStatus.OFFLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(offlineNode));

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.NODE_UNREACHABLE);
        }

        @Test
        @DisplayName("無 gRPC 地址時應回傳 NODE_UNREACHABLE")
        void shouldReturnNodeUnreachableWhenNoGrpcAddress() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, null);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.empty());

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.NODE_UNREACHABLE);
        }

        @Test
        @DisplayName("空白 gRPC 地址且無法從 NodeRegistry 取得時應回傳 NODE_UNREACHABLE")
        void shouldReturnNodeUnreachableWhenBlankGrpcAddressAndNoNodeInfo() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, "   ");
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.empty());

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.NODE_UNREACHABLE);
        }
    }

    @Nested
    @DisplayName("SUCCESS 結果測試")
    class SuccessTests {

        @Test
        @DisplayName("成功推送時應回傳 SUCCESS")
        void shouldReturnSuccessOnSuccessfulPush() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, TARGET_GRPC_ADDRESS);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);

            NodeInfo onlineNode = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(TARGET_GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(onlineNode));

            when(grpcClientManager.getStub(TARGET_NODE_ID)).thenReturn(Optional.of(mockStub));

            PushResponse successResponse = PushResponse.newBuilder()
                    .setSuccess(true)
                    .setCode(PushResultCode.PUSH_SUCCESS)
                    .build();
            when(mockStub.pushNotification(any(PushRequest.class))).thenReturn(successResponse);

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.SUCCESS);
        }

        @Test
        @DisplayName("SUSPECTED 狀態節點推送成功時應回傳 SUCCESS")
        void shouldReturnSuccessForSuspectedNodeOnSuccessfulPush() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, TARGET_GRPC_ADDRESS);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);

            NodeInfo suspectedNode = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(TARGET_GRPC_ADDRESS)
                    .status(NodeStatus.SUSPECTED)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(suspectedNode));

            when(grpcClientManager.getStub(TARGET_NODE_ID)).thenReturn(Optional.of(mockStub));

            PushResponse successResponse = PushResponse.newBuilder()
                    .setSuccess(true)
                    .setCode(PushResultCode.PUSH_SUCCESS)
                    .build();
            when(mockStub.pushNotification(any(PushRequest.class))).thenReturn(successResponse);

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.SUCCESS);
        }
    }

    @Nested
    @DisplayName("SESSION_NOT_FOUND 結果測試")
    class SessionNotFoundTests {

        @Test
        @DisplayName("Session 不存在時應回傳 SESSION_NOT_FOUND")
        void shouldReturnSessionNotFoundWhenSessionDoesNotExist() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, TARGET_GRPC_ADDRESS);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);

            NodeInfo onlineNode = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(TARGET_GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(onlineNode));

            when(grpcClientManager.getStub(TARGET_NODE_ID)).thenReturn(Optional.of(mockStub));

            PushResponse sessionNotFoundResponse = PushResponse.newBuilder()
                    .setSuccess(false)
                    .setCode(PushResultCode.SESSION_NOT_FOUND)
                    .setMessage("Session not found")
                    .build();
            when(mockStub.pushNotification(any(PushRequest.class))).thenReturn(sessionNotFoundResponse);

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.SESSION_NOT_FOUND);
        }

        @Test
        @DisplayName("Session 已關閉時應回傳 SESSION_NOT_FOUND")
        void shouldReturnSessionNotFoundWhenSessionClosed() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, TARGET_GRPC_ADDRESS);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);

            NodeInfo onlineNode = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(TARGET_GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(onlineNode));

            when(grpcClientManager.getStub(TARGET_NODE_ID)).thenReturn(Optional.of(mockStub));

            PushResponse sessionClosedResponse = PushResponse.newBuilder()
                    .setSuccess(false)
                    .setCode(PushResultCode.SESSION_CLOSED)
                    .setMessage("Session closed")
                    .build();
            when(mockStub.pushNotification(any(PushRequest.class))).thenReturn(sessionClosedResponse);

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.SESSION_NOT_FOUND);
        }
    }

    @Nested
    @DisplayName("重試機制測試")
    class RetryTests {

        @Test
        @DisplayName("推送失敗後應重試直到成功")
        void shouldRetryUntilSuccess() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, TARGET_GRPC_ADDRESS);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);

            NodeInfo onlineNode = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(TARGET_GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(onlineNode));

            when(grpcClientManager.getStub(TARGET_NODE_ID)).thenReturn(Optional.of(mockStub));

            PushResponse failResponse = PushResponse.newBuilder()
                    .setSuccess(false)
                    .setCode(PushResultCode.SEND_FAILED)
                    .build();
            PushResponse successResponse = PushResponse.newBuilder()
                    .setSuccess(true)
                    .setCode(PushResultCode.PUSH_SUCCESS)
                    .build();

            // 第一次失敗，第二次成功
            when(mockStub.pushNotification(any(PushRequest.class)))
                    .thenReturn(failResponse)
                    .thenReturn(successResponse);

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.SUCCESS);
            verify(mockStub, times(2)).pushNotification(any(PushRequest.class));
        }

        @Test
        @DisplayName("SESSION_NOT_FOUND 不應重試")
        void shouldNotRetryOnSessionNotFound() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, TARGET_GRPC_ADDRESS);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);

            NodeInfo onlineNode = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(TARGET_GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(onlineNode));

            when(grpcClientManager.getStub(TARGET_NODE_ID)).thenReturn(Optional.of(mockStub));

            PushResponse sessionNotFoundResponse = PushResponse.newBuilder()
                    .setSuccess(false)
                    .setCode(PushResultCode.SESSION_NOT_FOUND)
                    .build();
            when(mockStub.pushNotification(any(PushRequest.class))).thenReturn(sessionNotFoundResponse);

            // When
            crossNodePushService.push(routing, 1000, "test message");

            // Then - 只呼叫一次，不重試
            verify(mockStub, times(1)).pushNotification(any(PushRequest.class));
        }
    }

    @Nested
    @DisplayName("gRPC 錯誤處理測試")
    class GrpcErrorTests {

        @Test
        @DisplayName("gRPC 連線錯誤後經重試最終回傳 FAILED")
        void shouldReturnFailedAfterGrpcConnectionErrorWithRetry() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, TARGET_GRPC_ADDRESS);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);

            NodeInfo onlineNode = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(TARGET_GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(onlineNode));

            when(grpcClientManager.getStub(TARGET_NODE_ID)).thenReturn(Optional.of(mockStub));
            // gRPC UNAVAILABLE 錯誤會導致重試，重試後仍失敗最終回傳 FAILED
            when(mockStub.pushNotification(any(PushRequest.class)))
                    .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then - 因為有重試機制 (retryCount=3)，連續失敗後最終會回傳 FAILED
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.FAILED);
        }

        @Test
        @DisplayName("無可用 Stub 經重試後應回傳 FAILED")
        void shouldReturnFailedWhenNoStubAvailable() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, TARGET_GRPC_ADDRESS);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);

            NodeInfo onlineNode = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(TARGET_GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(onlineNode));

            when(grpcClientManager.getStub(TARGET_NODE_ID)).thenReturn(Optional.empty());

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then - 無可用 Stub 會拋出 RuntimeException，經重試後最終回傳 FAILED
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.FAILED);
        }
    }

    @Nested
    @DisplayName("Metrics 測試")
    class MetricsTests {

        @Test
        @DisplayName("成功推送應增加成功計數器")
        void shouldIncrementSuccessCounterOnSuccess() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, TARGET_GRPC_ADDRESS);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);

            NodeInfo onlineNode = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(TARGET_GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(onlineNode));

            when(grpcClientManager.getStub(TARGET_NODE_ID)).thenReturn(Optional.of(mockStub));

            PushResponse successResponse = PushResponse.newBuilder()
                    .setSuccess(true)
                    .setCode(PushResultCode.PUSH_SUCCESS)
                    .build();
            when(mockStub.pushNotification(any(PushRequest.class))).thenReturn(successResponse);

            // When
            crossNodePushService.push(routing, 1000, "test message");

            // Then
            Counter successCounter = meterRegistry.find("ns_cross_node_push_total")
                    .tag("result", "success")
                    .counter();
            assertThat(successCounter).isNotNull();
            assertThat(successCounter.count()).isEqualTo(1.0);
        }
    }

    @Nested
    @DisplayName("gRPC 地址回退測試")
    class GrpcAddressFallbackTests {

        @Test
        @DisplayName("routing 無 gRPC 地址時應從 NodeRegistry 取得")
        void shouldFallbackToNodeRegistryGrpcAddress() {
            // Given
            SubscriptionRouting routing = createRouting(TARGET_NODE_ID, null);
            when(nodeIdentity.isCurrentNode(TARGET_NODE_ID)).thenReturn(false);

            NodeInfo onlineNode = NodeInfo.builder()
                    .nodeId(TARGET_NODE_ID)
                    .grpcAddress(TARGET_GRPC_ADDRESS)
                    .status(NodeStatus.ONLINE)
                    .build();
            when(nodeRegistry.getNode(TARGET_NODE_ID)).thenReturn(Optional.of(onlineNode));

            when(grpcClientManager.getStub(TARGET_NODE_ID)).thenReturn(Optional.of(mockStub));

            PushResponse successResponse = PushResponse.newBuilder()
                    .setSuccess(true)
                    .setCode(PushResultCode.PUSH_SUCCESS)
                    .build();
            when(mockStub.pushNotification(any(PushRequest.class))).thenReturn(successResponse);

            // When
            CrossNodePushService.PushResult result = crossNodePushService.push(routing, 1000, "test message");

            // Then
            assertThat(result).isEqualTo(CrossNodePushService.PushResult.SUCCESS);
        }
    }
}
