package io.debezium.server.api;

import io.debezium.server.BaseChangeConsumer;

import java.util.Optional;

/**
 * Core interface for sink-specific consumers that process change events from Debezium.
 * <p>
 * Implementations of this interface are responsible for delivering captured database change events
 * to a specific destination (sink) such as Kafka, Kinesis, HTTP endpoints, or other messaging platforms.
 * Each consumer implementation must be annotated with {@code @Named} using the sink type identifier
 * (e.g., "kinesis", "kafka", "http") to enable CDI-based discovery via {@link ChangeConsumerHandler}.
 * <p>
 * Typical implementations extend {@link BaseChangeConsumer} to leverage common utilities for
 * stream name mapping, header conversion, and configuration extraction.
 *
 * @param <T> the type of events to handle, typically {@code CapturingEvents<BatchEvent>}
 *            containing batches of change events with their metadata
 *
 * @see ChangeConsumerHandler
 * @see BaseChangeConsumer
 */
public interface DebeziumServerConsumer<T> {

    void handle(T events) throws InterruptedException;

    default Optional<Boolean> tombstoneSupport() {
        return Optional.empty();
    }
}
