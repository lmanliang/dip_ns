package com.osw.dmp.ns.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.osw.dmp.ns.model.ResultEvent;
import com.osw.dmp.ns.model.StatusCode;
import com.osw.dmp.ns.service.NotificationService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

/**
 * Real Kafka Consumer using Reactor Kafka
 * Enabled when app.kafka.enabled=true
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = false)
public class ReactorKafkaConsumer implements KafkaConsumerStatus {

    private final KafkaReceiver<String, String> kafkaReceiver;
    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic.notification-result}")
    private String topic;

    private Disposable subscription;
    private volatile boolean running = false;

    @PostConstruct
    public void start() {
        log.info("📡 Starting Reactor Kafka Consumer for topic: {}", topic);
        running = true;

        subscription = kafkaReceiver.receive()
                .doOnNext(record -> {
                    log.debug("📨 Received Kafka message: partition={}, offset={}, key={}",
                            record.partition(), record.offset(), record.key());
                })
                .flatMap(this::processRecord)
                .subscribe(
                        result -> {
                        },
                        error -> log.error("❌ Kafka consumer error: {}", error.getMessage(), error),
                        () -> log.info("📡 Kafka consumer completed"));

        log.info("✅ Reactor Kafka Consumer started");
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
        log.info("📡 Reactor Kafka Consumer stopped");
    }

    private Mono<Void> processRecord(ReceiverRecord<String, String> record) {
        return Mono.fromCallable(() -> {
            String json = record.value();
            log.debug("📄 Parsing message: {}", json);
            return objectMapper.readValue(json, ResultEvent.class);
        })
                .doOnNext(event -> {
                    StatusCode.logByStatus(log, event.getStatus(), event.getEventId(),
                            String.format("Received ResultEvent from Kafka: status=%s", event.getStatus()));
                })
                .flatMap(event -> notificationService.processResultEvent(event))
                .doOnSuccess(v -> record.receiverOffset().acknowledge())
                .doOnError(e -> {
                    log.error("❌ Error processing message: key={}, error={}",
                            record.key(), e.getMessage());
                    // Still acknowledge to avoid infinite retry loop
                    // In production, you might want to send to DLQ
                    record.receiverOffset().acknowledge();
                })
                .onErrorComplete()
                .then();
    }

    public boolean isRunning() {
        return running && subscription != null && !subscription.isDisposed();
    }
}
