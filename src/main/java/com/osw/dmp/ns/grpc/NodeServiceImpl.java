package com.osw.dmp.ns.grpc;

import com.osw.dmp.ns.model.NodeInfo;
import com.osw.dmp.ns.websocket.NotificationWebSocketHandler;
import com.osw.dmp.ns.websocket.WebSocketMessages.NotificationMessage;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * gRPC NodeService 實作
 * 
 * 處理節點間通訊請求：
 * - PushNotification: 跨節點推送通知
 * - Heartbeat: 心跳檢測
 * - GetNodeStatus: 查詢節點狀態
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NodeServiceImpl extends NodeServiceGrpc.NodeServiceImplBase {

    private final NodeIdentity nodeIdentity;
    private final NodeRegistry nodeRegistry;
    private final NotificationWebSocketHandler webSocketHandler;

    /**
     * 處理跨節點推送請求
     */
    @Override
    public void pushNotification(PushRequest request, StreamObserver<PushResponse> responseObserver) {
        String eventId = request.getEventId();
        String sessionId = request.getSessionId();
        String sourceNodeId = request.getSourceNodeId();
        String traceId = request.getTraceId();

        log.info("📥 Received PushNotification: eventId={}, sessionId={}, from={}, traceId={}",
                eventId, sessionId, sourceNodeId, traceId);

        try {
            // 建構通知訊息
            NotificationMessage notification = NotificationMessage.builder()
                    .eventId(eventId)
                    .status(request.getStatus())
                    .message(request.getMessage())
                    .timestamp(request.getTimestamp())
                    .build();

            // 嘗試本地推送
            boolean pushed = webSocketHandler.pushNotification(eventId, notification);

            if (pushed) {
                log.info("✅ Cross-node push success: eventId={}, from={}", eventId, sourceNodeId);
                responseObserver.onNext(PushResponse.newBuilder()
                        .setSuccess(true)
                        .setCode(PushResultCode.PUSH_SUCCESS)
                        .setMessage("Notification pushed successfully")
                        .build());
            } else {
                // Session 不存在
                log.warn("⚠️ Session not found for cross-node push: eventId={}, sessionId={}", eventId, sessionId);
                responseObserver.onNext(PushResponse.newBuilder()
                        .setSuccess(false)
                        .setCode(PushResultCode.SESSION_NOT_FOUND)
                        .setMessage("Session not found: " + sessionId)
                        .build());
            }

        } catch (Exception e) {
            log.error("❌ Error processing cross-node push: eventId={}, error={}", eventId, e.getMessage(), e);
            responseObserver.onNext(PushResponse.newBuilder()
                    .setSuccess(false)
                    .setCode(PushResultCode.INTERNAL_ERROR)
                    .setMessage("Internal error: " + e.getMessage())
                    .build());
        }

        responseObserver.onCompleted();
    }

    /**
     * 處理心跳請求
     */
    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        String requestNodeId = request.getNodeId();
        long requestTimestamp = request.getTimestamp();

        log.debug("💓 Received heartbeat: nodeId={}, timestamp={}", requestNodeId, requestTimestamp);

        // 更新發送方節點的心跳時間
        nodeRegistry.updateNodeHeartbeat(requestNodeId);

        responseObserver.onNext(HeartbeatResponse.newBuilder()
                .setAlive(true)
                .setTimestamp(System.currentTimeMillis())
                .build());

        responseObserver.onCompleted();
    }

    /**
     * 查詢節點狀態
     */
    @Override
    public void getNodeStatus(NodeStatusRequest request, StreamObserver<NodeStatusResponse> responseObserver) {
        String targetNodeId = request.getNodeId();

        // 如果目標節點 ID 為空，則查詢當前節點
        if (targetNodeId == null || targetNodeId.isBlank()) {
            targetNodeId = nodeIdentity.getNodeId();
        }

        log.debug("📊 GetNodeStatus: targetNodeId={}", targetNodeId);

        NodeInfo nodeInfo = nodeRegistry.getNode(targetNodeId).orElse(null);

        if (nodeInfo != null) {
            responseObserver.onNext(NodeStatusResponse.newBuilder()
                    .setNodeId(nodeInfo.getNodeId())
                    .setGrpcAddress(nodeInfo.getGrpcAddress())
                    .setStartedAt(nodeInfo.getStartedAtMillis())
                    .setActiveConnections(nodeInfo.getActiveConnections())
                    .setActiveSubscriptions(nodeInfo.getActiveSubscriptions())
                    .build());
        } else {
            // 節點不存在時回傳空資料
            responseObserver.onNext(NodeStatusResponse.newBuilder()
                    .setNodeId(targetNodeId)
                    .build());
        }

        responseObserver.onCompleted();
    }
}
