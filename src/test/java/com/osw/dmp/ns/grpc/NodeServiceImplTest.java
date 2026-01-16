package com.osw.dmp.ns.grpc;

import com.osw.dmp.ns.model.NodeInfo;
import com.osw.dmp.ns.websocket.NotificationWebSocketHandler;
import com.osw.dmp.ns.websocket.WebSocketMessages.NotificationMessage;
import io.grpc.stub.StreamObserver;
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

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * NodeServiceImpl 單元測試
 * 
 * 測試 gRPC NodeService 實作
 */
@DisplayName("NodeServiceImpl 單元測試")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class NodeServiceImplTest {

    @Mock
    private NodeIdentity nodeIdentity;

    @Mock
    private NodeRegistry nodeRegistry;

    @Mock
    private NotificationWebSocketHandler webSocketHandler;

    @Mock
    private StreamObserver<PushResponse> pushResponseObserver;

    @Mock
    private StreamObserver<HeartbeatResponse> heartbeatResponseObserver;

    @Mock
    private StreamObserver<NodeStatusResponse> nodeStatusResponseObserver;

    private NodeServiceImpl nodeServiceImpl;

    private static final String CURRENT_NODE_ID = "current-node-001";
    private static final String SOURCE_NODE_ID = "source-node-002";
    private static final String TEST_EVENT_ID = "test-event-123";
    private static final String TEST_SESSION_ID = "session-001";
    private static final String GRPC_ADDRESS = "localhost:9090";

    @BeforeEach
    void setUp() {
        when(nodeIdentity.getNodeId()).thenReturn(CURRENT_NODE_ID);
        nodeServiceImpl = new NodeServiceImpl(nodeIdentity, nodeRegistry, webSocketHandler);
    }

    @Nested
    @DisplayName("pushNotification 測試")
    class PushNotificationTests {

        @Test
        @DisplayName("成功推送時應回傳 PUSH_SUCCESS")
        void shouldReturnPushSuccessOnSuccessfulPush() {
            // Given
            PushRequest request = PushRequest.newBuilder()
                    .setEventId(TEST_EVENT_ID)
                    .setSessionId(TEST_SESSION_ID)
                    .setStatus(1000)
                    .setMessage("Test notification")
                    .setTimestamp(System.currentTimeMillis())
                    .setSourceNodeId(SOURCE_NODE_ID)
                    .setTraceId("trace-123")
                    .build();

            when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), any(NotificationMessage.class)))
                    .thenReturn(true);

            // When
            nodeServiceImpl.pushNotification(request, pushResponseObserver);

            // Then
            ArgumentCaptor<PushResponse> captor = ArgumentCaptor.forClass(PushResponse.class);
            verify(pushResponseObserver).onNext(captor.capture());
            verify(pushResponseObserver).onCompleted();

            PushResponse response = captor.getValue();
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getCode()).isEqualTo(PushResultCode.PUSH_SUCCESS);
        }

        @Test
        @DisplayName("Session 不存在時應回傳 SESSION_NOT_FOUND")
        void shouldReturnSessionNotFoundWhenSessionDoesNotExist() {
            // Given
            PushRequest request = PushRequest.newBuilder()
                    .setEventId(TEST_EVENT_ID)
                    .setSessionId(TEST_SESSION_ID)
                    .setStatus(1000)
                    .setMessage("Test notification")
                    .setTimestamp(System.currentTimeMillis())
                    .setSourceNodeId(SOURCE_NODE_ID)
                    .setTraceId("trace-123")
                    .build();

            when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), any(NotificationMessage.class)))
                    .thenReturn(false);

            // When
            nodeServiceImpl.pushNotification(request, pushResponseObserver);

            // Then
            ArgumentCaptor<PushResponse> captor = ArgumentCaptor.forClass(PushResponse.class);
            verify(pushResponseObserver).onNext(captor.capture());
            verify(pushResponseObserver).onCompleted();

            PushResponse response = captor.getValue();
            assertThat(response.getSuccess()).isFalse();
            assertThat(response.getCode()).isEqualTo(PushResultCode.SESSION_NOT_FOUND);
        }

        @Test
        @DisplayName("推送發生異常時應回傳 INTERNAL_ERROR")
        void shouldReturnInternalErrorOnException() {
            // Given
            PushRequest request = PushRequest.newBuilder()
                    .setEventId(TEST_EVENT_ID)
                    .setSessionId(TEST_SESSION_ID)
                    .setStatus(1000)
                    .setMessage("Test notification")
                    .setTimestamp(System.currentTimeMillis())
                    .setSourceNodeId(SOURCE_NODE_ID)
                    .setTraceId("trace-123")
                    .build();

            when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), any(NotificationMessage.class)))
                    .thenThrow(new RuntimeException("Test exception"));

            // When
            nodeServiceImpl.pushNotification(request, pushResponseObserver);

            // Then
            ArgumentCaptor<PushResponse> captor = ArgumentCaptor.forClass(PushResponse.class);
            verify(pushResponseObserver).onNext(captor.capture());
            verify(pushResponseObserver).onCompleted();

            PushResponse response = captor.getValue();
            assertThat(response.getSuccess()).isFalse();
            assertThat(response.getCode()).isEqualTo(PushResultCode.INTERNAL_ERROR);
            assertThat(response.getMessage()).contains("Test exception");
        }

        @Test
        @DisplayName("應正確建構 NotificationMessage")
        void shouldBuildCorrectNotificationMessage() {
            // Given
            long timestamp = System.currentTimeMillis();
            PushRequest request = PushRequest.newBuilder()
                    .setEventId(TEST_EVENT_ID)
                    .setSessionId(TEST_SESSION_ID)
                    .setStatus(1000)
                    .setMessage("Custom message")
                    .setTimestamp(timestamp)
                    .setSourceNodeId(SOURCE_NODE_ID)
                    .setTraceId("trace-123")
                    .build();

            when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), any(NotificationMessage.class)))
                    .thenReturn(true);

            // When
            nodeServiceImpl.pushNotification(request, pushResponseObserver);

            // Then
            ArgumentCaptor<NotificationMessage> messageCaptor = ArgumentCaptor.forClass(NotificationMessage.class);
            verify(webSocketHandler).pushNotification(eq(TEST_EVENT_ID), messageCaptor.capture());

            NotificationMessage notification = messageCaptor.getValue();
            assertThat(notification.getEventId()).isEqualTo(TEST_EVENT_ID);
            assertThat(notification.getStatus()).isEqualTo(1000);
            assertThat(notification.getMessage()).isEqualTo("Custom message");
            assertThat(notification.getTimestamp()).isEqualTo(timestamp);
        }
    }

    @Nested
    @DisplayName("heartbeat 測試")
    class HeartbeatTests {

        @Test
        @DisplayName("收到心跳時應更新發送方節點的心跳時間")
        void shouldUpdateSenderNodeHeartbeat() {
            // Given
            long timestamp = System.currentTimeMillis();
            HeartbeatRequest request = HeartbeatRequest.newBuilder()
                    .setNodeId(SOURCE_NODE_ID)
                    .setTimestamp(timestamp)
                    .build();

            // When
            nodeServiceImpl.heartbeat(request, heartbeatResponseObserver);

            // Then
            verify(nodeRegistry).updateNodeHeartbeat(SOURCE_NODE_ID);
            verify(heartbeatResponseObserver).onNext(any(HeartbeatResponse.class));
            verify(heartbeatResponseObserver).onCompleted();
        }

        @Test
        @DisplayName("心跳回應應設定 alive 為 true")
        void shouldRespondWithAliveTrue() {
            // Given
            HeartbeatRequest request = HeartbeatRequest.newBuilder()
                    .setNodeId(SOURCE_NODE_ID)
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            // When
            nodeServiceImpl.heartbeat(request, heartbeatResponseObserver);

            // Then
            ArgumentCaptor<HeartbeatResponse> captor = ArgumentCaptor.forClass(HeartbeatResponse.class);
            verify(heartbeatResponseObserver).onNext(captor.capture());

            HeartbeatResponse response = captor.getValue();
            assertThat(response.getAlive()).isTrue();
            assertThat(response.getTimestamp()).isGreaterThan(0);
        }
    }

    @Nested
    @DisplayName("getNodeStatus 測試")
    class GetNodeStatusTests {

        @Test
        @DisplayName("查詢空 nodeId 時應查詢當前節點")
        void shouldQueryCurrentNodeWhenNodeIdIsEmpty() {
            // Given
            NodeStatusRequest request = NodeStatusRequest.newBuilder()
                    .setNodeId("")
                    .build();

            NodeInfo currentNodeInfo = NodeInfo.builder()
                    .nodeId(CURRENT_NODE_ID)
                    .grpcAddress(GRPC_ADDRESS)
                    .startedAtMillis(1000L)
                    .activeConnections(10)
                    .activeSubscriptions(5)
                    .build();
            when(nodeRegistry.getNode(CURRENT_NODE_ID)).thenReturn(Optional.of(currentNodeInfo));

            // When
            nodeServiceImpl.getNodeStatus(request, nodeStatusResponseObserver);

            // Then
            verify(nodeRegistry).getNode(CURRENT_NODE_ID);
            ArgumentCaptor<NodeStatusResponse> captor = ArgumentCaptor.forClass(NodeStatusResponse.class);
            verify(nodeStatusResponseObserver).onNext(captor.capture());

            NodeStatusResponse response = captor.getValue();
            assertThat(response.getNodeId()).isEqualTo(CURRENT_NODE_ID);
            assertThat(response.getGrpcAddress()).isEqualTo(GRPC_ADDRESS);
            assertThat(response.getActiveConnections()).isEqualTo(10);
            assertThat(response.getActiveSubscriptions()).isEqualTo(5);
        }

        @Test
        @DisplayName("查詢指定節點時應回傳該節點資訊")
        void shouldReturnSpecifiedNodeInfo() {
            // Given
            NodeStatusRequest request = NodeStatusRequest.newBuilder()
                    .setNodeId(SOURCE_NODE_ID)
                    .build();

            NodeInfo sourceNodeInfo = NodeInfo.builder()
                    .nodeId(SOURCE_NODE_ID)
                    .grpcAddress("source-host:9090")
                    .startedAtMillis(2000L)
                    .activeConnections(20)
                    .activeSubscriptions(15)
                    .build();
            when(nodeRegistry.getNode(SOURCE_NODE_ID)).thenReturn(Optional.of(sourceNodeInfo));

            // When
            nodeServiceImpl.getNodeStatus(request, nodeStatusResponseObserver);

            // Then
            verify(nodeRegistry).getNode(SOURCE_NODE_ID);
            ArgumentCaptor<NodeStatusResponse> captor = ArgumentCaptor.forClass(NodeStatusResponse.class);
            verify(nodeStatusResponseObserver).onNext(captor.capture());

            NodeStatusResponse response = captor.getValue();
            assertThat(response.getNodeId()).isEqualTo(SOURCE_NODE_ID);
            assertThat(response.getGrpcAddress()).isEqualTo("source-host:9090");
        }

        @Test
        @DisplayName("節點不存在時應回傳空資料")
        void shouldReturnEmptyDataWhenNodeNotFound() {
            // Given
            String unknownNodeId = "unknown-node";
            NodeStatusRequest request = NodeStatusRequest.newBuilder()
                    .setNodeId(unknownNodeId)
                    .build();

            when(nodeRegistry.getNode(unknownNodeId)).thenReturn(Optional.empty());

            // When
            nodeServiceImpl.getNodeStatus(request, nodeStatusResponseObserver);

            // Then
            ArgumentCaptor<NodeStatusResponse> captor = ArgumentCaptor.forClass(NodeStatusResponse.class);
            verify(nodeStatusResponseObserver).onNext(captor.capture());

            NodeStatusResponse response = captor.getValue();
            assertThat(response.getNodeId()).isEqualTo(unknownNodeId);
            assertThat(response.getGrpcAddress()).isEmpty();
            assertThat(response.getActiveConnections()).isEqualTo(0);
        }
    }
}
