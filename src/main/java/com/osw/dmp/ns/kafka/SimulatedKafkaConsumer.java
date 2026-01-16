package com.osw.dmp.ns.kafka;

import com.osw.dmp.ns.model.ResultEvent;
import com.osw.dmp.ns.model.StatusCode;
import com.osw.dmp.ns.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.UUID;

/**
 * Simulated Kafka Consumer for development/testing
 * Enabled when app.kafka.enabled=false (default)
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "false", matchIfMissing = true)
public class SimulatedKafkaConsumer implements KafkaConsumerStatus {

    private final NotificationService notificationService;

    // Sink to simulate Kafka messages
    private final Sinks.Many<ResultEvent> eventSink = Sinks.many().multicast().onBackpressureBuffer();

    private volatile boolean running = false;

    /**
     * Start consuming simulated events
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        log.info("📡 Simulated Kafka Consumer started");

        eventSink.asFlux()
                .doOnNext(event -> {
                    StatusCode.logByStatus(log, event.getStatus(), event.getEventId(),
                            String.format("Received ResultEvent from Kafka: status=%s", event.getStatus()));
                })
                .flatMap(event -> notificationService.processResultEvent(event)
                        .doOnError(e -> log.error("❌ Error processing event: {}", e.getMessage()))
                        .onErrorComplete())
                .subscribe();
    }

    /**
     * Stop consumer
     */
    public void stop() {
        running = false;
        log.info("📡 Simulated Kafka Consumer stopped");
    }

    /**
     * Simulate sending a result event (for testing)
     */
    public void simulateResultEvent(ResultEvent event) {
        log.info("🔧 Simulating ResultEvent: eventId={}, status={}", event.getEventId(), event.getStatus());
        eventSink.tryEmitNext(event);
    }

    /**
     * Simulate a successful result for an eventId
     */
    public void simulateSuccess(String eventId) {
        simulateSuccess(eventId, "處理完成");
    }

    /**
     * Simulate a successful result for an eventId with message
     */
    public void simulateSuccess(String eventId, String message) {
        ResultEvent event = ResultEvent.builder()
                .schemaVersion("1.0")
                .eventId(eventId)
                .status(StatusCode.SUCCESS)
                .message(message)
                .completedAt(System.currentTimeMillis())
                .traceId(UUID.randomUUID().toString())
                .build();
        simulateResultEvent(event);
    }

    /**
     * Simulate a failed result for an eventId
     */
    public void simulateFailed(String eventId, int status, String message) {
        ResultEvent event = ResultEvent.builder()
                .schemaVersion("1.0")
                .eventId(eventId)
                .status(status)
                .message(message)
                .completedAt(System.currentTimeMillis())
                .traceId(UUID.randomUUID().toString())
                .build();
        simulateResultEvent(event);
    }

    /**
     * Simulate delayed result (for testing async behavior)
     */
    public void simulateDelayedSuccess(String eventId, Duration delay) {
        Flux.interval(delay)
                .take(1)
                .doOnNext(i -> simulateSuccess(eventId))
                .subscribe();
    }

    public boolean isRunning() {
        return running;
    }
}
