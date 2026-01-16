package com.osw.dmp.ns;

import com.osw.dmp.ns.kafka.SimulatedKafkaConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Notification Service Application
 * 
 * 中台結果通知服務
 * - WebSocket 連線管理
 * - Kafka 消費處理
 * - Ignite 分散式狀態儲存
 */
@Slf4j
@SpringBootApplication
public class NotificationServiceApplication {

    @Autowired(required = false)
    private SimulatedKafkaConsumer simulatedKafkaConsumer;

    @Value("${app.kafka.enabled:false}")
    private boolean kafkaEnabled;

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }

    @Bean
    public CommandLineRunner startKafkaConsumer() {
        return args -> {
            log.info("═══════════════════════════════════════════════════════════════");
            log.info("   🚀 Notification Service Started!");
            log.info("═══════════════════════════════════════════════════════════════");
            log.info("");
            log.info("   📡 WebSocket Endpoint: ws://localhost:8080/ws/notifications");
            log.info("   📡 Kafka Enabled: {}", kafkaEnabled);
            log.info("");
            if (!kafkaEnabled) {
                log.info("   🔧 Simulation APIs:");
                log.info("      POST /api/simulate/success/{eventId}  - Simulate success");
                log.info("      POST /api/simulate/failed/{eventId}   - Simulate failure");
                log.info("      POST /api/simulate/delayed/{eventId}  - Simulate delayed");
                log.info("");
            }
            log.info("   📊 Status APIs:");
            log.info("      GET  /api/status                      - Service status");
            log.info("      GET  /api/health                      - Health check");
            log.info("      GET  /api/ready                       - Readiness check");
            log.info("      GET  /api/metrics/custom              - Custom metrics");
            log.info("");
            log.info("   📈 Actuator:");
            log.info("      GET  /actuator/health                 - Spring Health");
            log.info("      GET  /actuator/prometheus             - Prometheus metrics");
            log.info("");
            log.info("═══════════════════════════════════════════════════════════════");

            // Start simulated Kafka consumer only if real Kafka is disabled
            if (simulatedKafkaConsumer != null) {
                simulatedKafkaConsumer.start();
            }
        };
    }
}
