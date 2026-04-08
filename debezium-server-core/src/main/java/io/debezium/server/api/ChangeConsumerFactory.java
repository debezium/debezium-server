package io.debezium.server.api;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;

import java.util.Optional;

/**
 * Factory interface for obtaining the configured sink consumer and its capabilities.
 * <p>
 * This factory provides access to the sink-specific {@link DebeziumServerConsumer} instance
 * selected based on the {@code debezium.sink.type} configuration property. The actual consumer
 * is discovered via CDI by matching the configured sink type against {@code @Named} beans.
 * <p>
 * The factory also exposes whether the selected consumer supports tombstone events, allowing
 * other components to adapt behavior accordingly.
 */
public interface ChangeConsumerFactory {
    DebeziumServerConsumer<CapturingEvents<BatchEvent>> get();

    Optional<Boolean> tombstoneSupport();
}
