package com.osw.dmp.ns.grpc;

import com.osw.dmp.ns.config.GrpcProperties;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * gRPC Server 配置與生命週期管理
 * 
 * 負責啟動和關閉 gRPC Server
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.grpc.enabled", havingValue = "true", matchIfMissing = true)
public class GrpcServerConfig {

    private final GrpcProperties grpcProperties;
    private final NodeServiceImpl nodeService;

    private Server server;

    @PostConstruct
    public void start() throws IOException {
        int port = grpcProperties.getPort();

        server = ServerBuilder.forPort(port)
                .addService(nodeService)
                .addService(ProtoReflectionServiceV1.newInstance()) // 支援 gRPC reflection
                .build()
                .start();

        log.info("gRPC Server started on port {}", port);

        // 註冊 shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down gRPC server...");
            try {
                stop();
            } catch (InterruptedException e) {
                log.error("Error shutting down gRPC server", e);
                Thread.currentThread().interrupt();
            }
        }));
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            log.info("gRPC Server stopped");
        }
    }

    /**
     * 取得 Server 實例
     */
    public Server getServer() {
        return server;
    }

    /**
     * 檢查 Server 是否運行中
     */
    public boolean isRunning() {
        return server != null && !server.isShutdown() && !server.isTerminated();
    }
}
