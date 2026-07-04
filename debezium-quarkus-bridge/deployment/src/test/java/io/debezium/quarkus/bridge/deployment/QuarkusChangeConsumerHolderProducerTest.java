/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.bridge.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Stream;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.quarkus.bridge.QuarkusChangeConsumerHolderProducer;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.ChangeConsumerHolder;
import io.debezium.server.api.DebeziumServerConsumer;

class QuarkusChangeConsumerHolderProducerTest {

    @SuppressWarnings("unchecked")
    @Test
    void producesHolderWithSingleConsumer() {
        DebeziumServerConsumer<CapturingEvents<BatchEvent>> consumer = mock(DebeziumServerConsumer.class);
        Instance<DebeziumServerConsumer<CapturingEvents<BatchEvent>>> instance = mock(Instance.class);
        when(instance.stream()).thenReturn(Stream.of(consumer));

        QuarkusChangeConsumerHolderProducer producer = new QuarkusChangeConsumerHolderProducer(instance);
        ChangeConsumerHolder holder = producer.produces();

        assertThat(holder.get()).isSameAs(consumer);
    }

    @SuppressWarnings("unchecked")
    @Test
    void throwsWhenMultipleConsumersFound() {
        DebeziumServerConsumer<CapturingEvents<BatchEvent>> consumer1 = mock(DebeziumServerConsumer.class);
        DebeziumServerConsumer<CapturingEvents<BatchEvent>> consumer2 = mock(DebeziumServerConsumer.class);
        Instance<DebeziumServerConsumer<CapturingEvents<BatchEvent>>> instance = mock(Instance.class);
        when(instance.stream()).thenReturn(Stream.of(consumer1, consumer2));

        QuarkusChangeConsumerHolderProducer producer = new QuarkusChangeConsumerHolderProducer(instance);

        assertThatThrownBy(producer::produces)
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("multiple sink");
    }

    @SuppressWarnings("unchecked")
    @Test
    void delegatesTombstoneSupport() {
        DebeziumServerConsumer<CapturingEvents<BatchEvent>> consumer = mock(DebeziumServerConsumer.class);
        when(consumer.tombstoneSupport()).thenReturn(Optional.of(true));
        Instance<DebeziumServerConsumer<CapturingEvents<BatchEvent>>> instance = mock(Instance.class);
        when(instance.stream()).thenReturn(Stream.of(consumer));

        QuarkusChangeConsumerHolderProducer producer = new QuarkusChangeConsumerHolderProducer(instance);
        ChangeConsumerHolder holder = producer.produces();

        assertThat(holder.tombstoneSupport()).contains(true);
    }

    @SuppressWarnings("unchecked")
    @Test
    void tombstoneSupportDefaultsToEmpty() {
        DebeziumServerConsumer<CapturingEvents<BatchEvent>> consumer = mock(DebeziumServerConsumer.class);
        Instance<DebeziumServerConsumer<CapturingEvents<BatchEvent>>> instance = mock(Instance.class);
        when(instance.stream()).thenReturn(Stream.of(consumer));

        QuarkusChangeConsumerHolderProducer producer = new QuarkusChangeConsumerHolderProducer(instance);
        ChangeConsumerHolder holder = producer.produces();

        assertThat(holder.tombstoneSupport()).isEmpty();
    }
}
