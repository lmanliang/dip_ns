package com.osw.dmp.ns.kafka;

/**
 * Interface for checking Kafka consumer status
 */
public interface KafkaConsumerStatus {

    /**
     * Check if the Kafka consumer is running
     */
    boolean isRunning();
}
