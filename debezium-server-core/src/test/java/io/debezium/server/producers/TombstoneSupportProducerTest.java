/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.producers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.ChangeConsumerHolder;
import io.debezium.server.api.DebeziumServerConsumer;

/**
 * Unit tests for the tombstone support that {@link TombstoneSupportProducer} reports to the engine.
 */
public class TombstoneSupportProducerTest {

    private final TombstoneSupportProducer producer = new TombstoneSupportProducer();

    @Test
    @FixFor("debezium/dbz#3133")
    public void shouldDeliverTombstonesWhenSinkDoesNotDeclareSupport() {
        assertThat(producer.produces(sinkWithoutDeclaredTombstoneSupport()).isSupported()).isTrue();
    }

    @Test
    public void shouldDeliverTombstonesWhenSinkDeclaresSupport() {
        assertThat(producer.produces(sinkWithTombstoneSupport(true)).isSupported()).isTrue();
    }

    @Test
    public void shouldNotDeliverTombstonesWhenSinkDeclaresNoSupport() {
        assertThat(producer.produces(sinkWithTombstoneSupport(false)).isSupported()).isFalse();
    }

    private static ChangeConsumerHolder<CapturingEvents<BatchEvent>> sinkWithoutDeclaredTombstoneSupport() {
        return sinkWithTombstoneSupport(null);
    }

    // The holder is how the server hands the selected sink to other components.
    // A null argument stands for a sink that declares nothing about tombstones.
    private static ChangeConsumerHolder<CapturingEvents<BatchEvent>> sinkWithTombstoneSupport(Boolean declaredSupport) {
        return new ChangeConsumerHolder<>() {
            @Override
            public DebeziumServerConsumer<CapturingEvents<BatchEvent>> get() {
                throw new UnsupportedOperationException("The producer only reads the tombstone support");
            }

            @Override
            public Optional<Boolean> tombstoneSupport() {
                return Optional.ofNullable(declaredSupport);
            }
        };
    }
}
