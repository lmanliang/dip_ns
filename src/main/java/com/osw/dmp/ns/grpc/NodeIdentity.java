package com.osw.dmp.ns.grpc;

import com.osw.dmp.ns.config.GrpcProperties;
import com.osw.dmp.ns.config.NodeProperties;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * 節點身份管理服務
 * 
 * 負責生成和管理當前節點的唯一識別資訊
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NodeIdentity {

    private final NodeProperties nodeProperties;
    private final GrpcProperties grpcProperties;

    /**
     * 節點唯一 ID
     */
    @Getter
    private String nodeId;

    /**
     * gRPC 服務地址 (host:port)
     */
    @Getter
    private String grpcAddress;

    /**
     * 節點啟動時間 (epoch millis)
     */
    @Getter
    private long startedAtMillis;

    @PostConstruct
    public void init() {
        this.startedAtMillis = System.currentTimeMillis();
        this.nodeId = generateNodeId();
        this.grpcAddress = generateGrpcAddress();

        log.info("🆔 Node Identity initialized: nodeId={}, grpcAddress={}", nodeId, grpcAddress);
    }

    /**
     * 生成節點 ID
     * 
     * 優先順序:
     * 1. 配置中的 app.node.id
     * 2. 環境變數 HOSTNAME（Kubernetes POD_NAME）
     * 3. 自動生成: hostname-port-uuid
     */
    private String generateNodeId() {
        // 1. Check configured node id
        if (nodeProperties.getId() != null && !nodeProperties.getId().isBlank()) {
            return nodeProperties.getId();
        }

        // 2. Check HOSTNAME environment variable (Kubernetes)
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isBlank()) {
            return hostname;
        }

        // 3. Auto-generate
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "unknown";
        }

        String shortUuid = UUID.randomUUID().toString().substring(0, 8);
        return String.format("%s-%d-%s", hostname, grpcProperties.getPort(), shortUuid);
    }

    /**
     * 生成 gRPC 地址
     * 
     * 優先順序:
     * 1. 環境變數 POD_IP (Kubernetes)
     * 2. 本地 IP 地址
     */
    private String generateGrpcAddress() {
        String host;

        // 1. Check POD_IP environment variable (Kubernetes)
        String podIp = System.getenv("POD_IP");
        if (podIp != null && !podIp.isBlank()) {
            host = podIp;
        } else {
            // 2. Use local address
            try {
                host = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                host = "localhost";
            }
        }

        return String.format("%s:%d", host, grpcProperties.getPort());
    }

    /**
     * 檢查是否為當前節點
     */
    public boolean isCurrentNode(String targetNodeId) {
        return nodeId.equals(targetNodeId);
    }
}
