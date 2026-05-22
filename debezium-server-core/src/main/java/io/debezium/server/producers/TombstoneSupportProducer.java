/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.producers;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.debezium.engine.DebeziumEngine;
import io.debezium.server.api.ChangeConsumerHolder;
import io.quarkus.arc.Unremovable;
import io.quarkus.debezium.engine.capture.CapturingTombstoneEvents;
import io.quarkus.runtime.Startup;

/**
 * CDI producer that creates the {@link CapturingTombstoneEvents} configuration for tombstone event handling.
 * <p>
 * This producer queries the configured {@link ChangeConsumerHolder} to determine whether the selected
 * sink consumer supports tombstone events (delete events with null payloads). The tombstone support
 * capability is used by the Debezium embedded engine to decide whether to capture and deliver these
 * events to the consumer.
 * <p>
 * If the consumer explicitly declares tombstone support via {@link ChangeConsumerHolder#tombstoneSupport()},
 * that value is used. Otherwise, it falls back to the default behavior defined by
 * {@link DebeziumEngine.ChangeConsumer#supportsTombstoneEvents()}.
 *
 * @see CapturingTombstoneEvents
 * @see ChangeConsumerHolder#tombstoneSupport()
 * @see DebeziumEngine.ChangeConsumer#supportsTombstoneEvents()
 */
@ApplicationScoped
public class TombstoneSupportProducer {

    @Startup
    @Produces
    @Unremovable
    @ApplicationScoped
    public CapturingTombstoneEvents produces(ChangeConsumerHolder changeConsumerHolder) {
        return changeConsumerHolder
                .tombstoneSupport()
                .map(isSupported -> (CapturingTombstoneEvents) () -> isSupported)
                .orElse(() -> ((DebeziumEngine.ChangeConsumer<Object>) (records, committer) -> {
                    /* ignore */
                }).supportsTombstoneEvents());
    }
}
