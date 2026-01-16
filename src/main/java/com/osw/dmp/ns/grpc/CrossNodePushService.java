package com.osw.dmp.ns.grpc;

import com.osw.dmp.ns.config.GrpcProperties;
import com.osw.dmp.ns.model.NodeInfo;
import com.osw.dmp.ns.model.SubscriptionRouting;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 跨節點推送服務
 * 
 * 負責將通知推送到其他 NS 節點：
 * - 根據 SubscriptionRouting 找到目標節點
 * - 透過 gRPC 發送推送請求
 * - 處理重試與降級邏輯
 */
@Slf4j
@Service
public class CrossNodePushService {

    private final NodeIdentity nodeIdentity;
    private final NodeRegistry nodeRegistry;
    private final GrpcClientManager grpcClientManager;
    private final GrpcProperties grpcProperties;

    // Metrics
    private final Counter pushSuccessCounter;
    private final Counter pushFailureCounter;
    private final Timer pushDurationTimer;

    public CrossNodePushService(
            NodeIdentity nodeIdentity,
            NodeRegistry nodeRegistry,
            GrpcClientManager grpcClientManager,
            GrpcProperties grpcProperties,
            MeterRegistry meterRegistry) {
        this.nodeIdentity = nodeIdentity;
        this.nodeRegistry = nodeRegistry;
        this.grpcClientManager = grpcClientManager;
        this.grpcProperties = grpcProperties;

        // Initialize metrics
        this.pushSuccessCounter = Counter.builder("ns_cross_node_push_total")
                .tag("result", "success")
                .description("Total successful cross-node pushes")
                .register(meterRegistry);

        this.pushFailureCounter = Counter.builder("ns_cross_node_push_total")
                .tag("result", "failed")
                .description("Total failed cross-node pushes")
                .register(meterRegistry);

        this.pushDurationTimer = Timer.builder("ns_cross_node_push_duration_seconds")
                .description("Cross-node push duration")
                .register(meterRegistry);
    }

    /**
     * 跨節點推送結果
     */
    public enum PushResult {
        /** 推送成功 */
        SUCCESS,
        /** 目標是當前節點（應本地處理） */
        LOCAL_NODE,
        /** Session 不存在 */
        SESSION_NOT_FOUND,
        /** 目標節點不可達 */
        NODE_UNREACHABLE,
        /** 推送失敗 */
        FAILED,
        /** 內部錯誤 */
        ERROR
    }

    /**
     * 執行跨節點推送
     * 
     * @param routing 訂閱路由資訊
     * @param status  業務狀態碼
     * @param message 訊息內容
     * @return 推送結果
     */
    public PushResult push(SubscriptionRouting routing, int status, String message) {
        String eventId = routing.getEventId();
        String targetNodeId = routing.getNodeId();
        String targetGrpcAddress = routing.getGrpcAddress();
        String sessionId = routing.getSessionId();

        // 檢查是否為當前節點
        if (nodeIdentity.isCurrentNode(targetNodeId)) {
            log.debug("🏠 Target is current node, should handle locally: eventId={}", eventId);
            return PushResult.LOCAL_NODE;
        }

        // 檢查目標節點是否可用
        Optional<NodeInfo> targetNodeOpt = nodeRegistry.getNode(targetNodeId);
        if (targetNodeOpt.isPresent() && !targetNodeOpt.get().isAvailableForPush()) {
            log.warn("⚠️ Target node not available: nodeId={}, status={}",
                    targetNodeId, targetNodeOpt.get().getStatus());
            return PushResult.NODE_UNREACHABLE;
        }

        // 如果沒有 gRPC 地址，嘗試從 NodeRegistry 取得
        if (targetGrpcAddress == null || targetGrpcAddress.isBlank()) {
            targetGrpcAddress = targetNodeOpt.map(NodeInfo::getGrpcAddress).orElse(null);
        }

        if (targetGrpcAddress == null || targetGrpcAddress.isBlank()) {
            log.error("❌ No gRPC address for target node: nodeId={}", targetNodeId);
            return PushResult.NODE_UNREACHABLE;
        }

        // 執行推送（含重試）
        return pushWithRetry(eventId, sessionId, targetNodeId, targetGrpcAddress, status, message);
    }

    /**
     * 帶重試的推送邏輯
     */
    private PushResult pushWithRetry(
            String eventId,
            String sessionId,
            String targetNodeId,
            String targetGrpcAddress,
            int status,
            String message) {

        int maxRetries = grpcProperties.getClient().getRetryCount();
        long retryDelayMs = grpcProperties.getClient().getRetryDelayMs();

        String traceId = UUID.randomUUID().toString();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                log.info("🔄 Retrying cross-node push: eventId={}, attempt={}/{}",
                        eventId, attempt, maxRetries);
                try {
                    TimeUnit.MILLISECONDS.sleep(retryDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return PushResult.ERROR;
                }
            }

            PushResult result = doPush(eventId, sessionId, targetNodeId, targetGrpcAddress, status, message, traceId);

            if (result == PushResult.SUCCESS || result == PushResult.SESSION_NOT_FOUND) {
                // 成功或 Session 不存在（不需重試）
                return result;
            }

            // 其他錯誤繼續重試
        }

        log.error("❌ Cross-node push failed after {} retries: eventId={}, targetNode={}",
                maxRetries, eventId, targetNodeId);
        return PushResult.FAILED;
    }

    /**
     * 執行單次推送
     */
    private PushResult doPush(
            String eventId,
            String sessionId,
            String targetNodeId,
            String targetGrpcAddress,
            int status,
            String message,
            String traceId) {

        long startTime = System.nanoTime();

        try {
            log.info("📤 Sending cross-node push: eventId={}, target={}, address={}",
                    eventId, targetNodeId, targetGrpcAddress);

            // 建立請求
            PushRequest request = PushRequest.newBuilder()
                    .setEventId(eventId)
                    .setSessionId(sessionId != null ? sessionId : "")
                    .setStatus(status)
                    .setMessage(message != null ? message : "")
                    .setTimestamp(System.currentTimeMillis())
                    .setSourceNodeId(nodeIdentity.getNodeId())
                    .setTraceId(traceId)
                    .build();

            // 取得 Stub 並發送請求
            NodeServiceGrpc.NodeServiceBlockingStub stub = grpcClientManager.getStub(targetNodeId)
                    .orElseThrow(() -> new RuntimeException("No stub available for node: " + targetNodeId));

            PushResponse response = stub.pushNotification(request);

            // 記錄延遲
            long duration = System.nanoTime() - startTime;
            pushDurationTimer.record(duration, TimeUnit.NANOSECONDS);

            // 處理回應
            if (response.getSuccess()) {
                pushSuccessCounter.increment();
                log.info("✅ Cross-node push success: eventId={}, target={}, duration={}ms",
                        eventId, targetNodeId, TimeUnit.NANOSECONDS.toMillis(duration));
                return PushResult.SUCCESS;
            } else {
                PushResultCode code = response.getCode();
                log.warn("⚠️ Cross-node push returned failure: eventId={}, code={}, message={}",
                        eventId, code, response.getMessage());

                if (code == PushResultCode.SESSION_NOT_FOUND || code == PushResultCode.SESSION_CLOSED) {
                    pushFailureCounter.increment();
                    return PushResult.SESSION_NOT_FOUND;
                }

                return PushResult.FAILED;
            }

        } catch (StatusRuntimeException e) {
            pushFailureCounter.increment();
            log.error("❌ gRPC error during cross-node push: eventId={}, target={}, status={}",
                    eventId, targetNodeId, e.getStatus(), e);

            // 標記節點為 SUSPECTED
            nodeRegistry.getNode(targetNodeId).ifPresent(node -> {
                if (node.isOnline()) {
                    node.setStatus(com.osw.dmp.ns.model.NodeStatus.SUSPECTED);
                    log.warn("⚠️ Marked node as SUSPECTED due to gRPC failure: {}", targetNodeId);
                }
            });

            return PushResult.NODE_UNREACHABLE;

        } catch (Exception e) {
            pushFailureCounter.increment();
            log.error("❌ Unexpected error during cross-node push: eventId={}, target={}",
                    eventId, targetNodeId, e);
            return PushResult.ERROR;
        }
    }
}
