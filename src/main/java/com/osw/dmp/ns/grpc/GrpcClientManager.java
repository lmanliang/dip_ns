package com.osw.dmp.ns.grpc;

import com.osw.dmp.ns.config.GrpcProperties;
import com.osw.dmp.ns.model.NodeInfo;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * gRPC 客戶端連線管理器
 * 
 * 負責管理與其他 NS 節點的 gRPC 連線：
 * - 連線池管理
 * - 連線建立與關閉
 * - Stub 取得
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GrpcClientManager {

    private final GrpcProperties grpcProperties;
    private final NodeRegistry nodeRegistry;

    /**
     * 連線池 (grpcAddress -> ManagedChannel)
     */
    private final Map<String, ManagedChannel> channelPool = new ConcurrentHashMap<>();

    /**
     * Stub 池 (grpcAddress -> Stub)
     */
    private final Map<String, NodeServiceGrpc.NodeServiceBlockingStub> stubPool = new ConcurrentHashMap<>();

    @PreDestroy
    public void shutdown() {
        log.info("🛑 Shutting down gRPC client connections...");

        for (Map.Entry<String, ManagedChannel> entry : channelPool.entrySet()) {
            try {
                entry.getValue().shutdown().awaitTermination(5, TimeUnit.SECONDS);
                log.debug("Channel closed: {}", entry.getKey());
            } catch (InterruptedException e) {
                log.warn("Error closing channel: {}", entry.getKey(), e);
                Thread.currentThread().interrupt();
            }
        }

        channelPool.clear();
        stubPool.clear();

        log.info("🛑 gRPC client connections shutdown complete");
    }

    /**
     * 取得指定節點的 Blocking Stub
     * 
     * @param nodeId 目標節點 ID
     * @return Stub 或 empty
     */
    public Optional<NodeServiceGrpc.NodeServiceBlockingStub> getStub(String nodeId) {
        return nodeRegistry.getNode(nodeId)
                .map(this::getStubForNode);
    }

    /**
     * 取得指定 gRPC 地址的 Blocking Stub
     * 
     * @param grpcAddress gRPC 地址 (host:port)
     * @return Stub
     */
    public NodeServiceGrpc.NodeServiceBlockingStub getStub(String host, int port) {
        String grpcAddress = host + ":" + port;
        return getOrCreateStub(grpcAddress);
    }

    /**
     * 取得指定節點資訊的 Stub
     */
    private NodeServiceGrpc.NodeServiceBlockingStub getStubForNode(NodeInfo nodeInfo) {
        return getOrCreateStub(nodeInfo.getGrpcAddress());
    }

    /**
     * 取得或建立 Stub
     */
    private NodeServiceGrpc.NodeServiceBlockingStub getOrCreateStub(String grpcAddress) {
        return stubPool.computeIfAbsent(grpcAddress, addr -> {
            ManagedChannel channel = getOrCreateChannel(addr);
            return NodeServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(grpcProperties.getClient().getTimeoutMs(), TimeUnit.MILLISECONDS);
        });
    }

    /**
     * 取得或建立 Channel
     */
    private ManagedChannel getOrCreateChannel(String grpcAddress) {
        return channelPool.computeIfAbsent(grpcAddress, addr -> {
            String[] parts = addr.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : grpcProperties.getPort();

            log.info("📡 Creating gRPC channel: {}", addr);

            return ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext() // 開發環境不使用 TLS，生產環境應啟用
                    .idleTimeout(grpcProperties.getClient().getIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                    .build();
        });
    }

    /**
     * 關閉指定節點的連線
     */
    public void closeConnection(String grpcAddress) {
        ManagedChannel channel = channelPool.remove(grpcAddress);
        stubPool.remove(grpcAddress);

        if (channel != null) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                log.info("📡 gRPC channel closed: {}", grpcAddress);
            } catch (InterruptedException e) {
                log.warn("Error closing channel: {}", grpcAddress, e);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 發送心跳到指定節點
     */
    public boolean sendHeartbeat(String nodeId, String targetGrpcAddress) {
        try {
            NodeServiceGrpc.NodeServiceBlockingStub stub = getOrCreateStub(targetGrpcAddress);

            HeartbeatRequest request = HeartbeatRequest.newBuilder()
                    .setNodeId(nodeId)
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            HeartbeatResponse response = stub.heartbeat(request);
            return response.getAlive();

        } catch (StatusRuntimeException e) {
            log.warn("💔 Heartbeat failed to {}: {}", targetGrpcAddress, e.getStatus());
            return false;
        }
    }

    /**
     * 取得連線池大小
     */
    public int getPoolSize() {
        return channelPool.size();
    }

    /**
     * 檢查連線是否存在
     */
    public boolean hasConnection(String grpcAddress) {
        ManagedChannel channel = channelPool.get(grpcAddress);
        return channel != null && !channel.isShutdown() && !channel.isTerminated();
    }
}
