package io.debezium.server.producers;

import io.debezium.engine.DebeziumEngine;
import io.debezium.server.api.ChangeConsumerHandler;
import io.quarkus.arc.Unremovable;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.quarkus.debezium.engine.capture.CapturingTombstoneEvents;

/**
 * CDI producer that creates the {@link CapturingTombstoneEvents} configuration for tombstone event handling.
 * <p>
 * This producer queries the configured {@link ChangeConsumerHandler} to determine whether the selected
 * sink consumer supports tombstone events (delete events with null payloads). The tombstone support
 * capability is used by the Debezium embedded engine to decide whether to capture and deliver these
 * events to the consumer.
 * <p>
 * If the consumer explicitly declares tombstone support via {@link ChangeConsumerHandler#tombstoneSupport()},
 * that value is used. Otherwise, it falls back to the default behavior defined by
 * {@link DebeziumEngine.ChangeConsumer#supportsTombstoneEvents()}.
 *
 * @see CapturingTombstoneEvents
 * @see ChangeConsumerHandler#tombstoneSupport()
 * @see DebeziumEngine.ChangeConsumer#supportsTombstoneEvents()
 */
@ApplicationScoped
public class TombstoneSupportProducer {

    @Startup
    @Produces
    @Unremovable
    @ApplicationScoped
    public CapturingTombstoneEvents produces(ChangeConsumerHandler changeConsumerHandler) {
        return changeConsumerHandler
                .tombstoneSupport()
                .map(isSupported -> (CapturingTombstoneEvents) () -> isSupported)
                .orElse(() -> ((DebeziumEngine.ChangeConsumer<Object>) (records, committer) -> {
                    /* ignore */
                }).supportsTombstoneEvents());
    }
}
