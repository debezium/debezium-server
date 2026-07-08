/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.bridge.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.quarkus.bridge.QuarkusChangeConsumer;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.ChangeConsumerHolder;
import io.debezium.server.api.DebeziumServerConsumer;

class QuarkusChangeConsumerTest {

    @SuppressWarnings("unchecked")
    @Test
    void delegatesToChangeConsumerHolder() throws InterruptedException {
        DebeziumServerConsumer<CapturingEvents<BatchEvent>> consumer = mock(DebeziumServerConsumer.class);
        ChangeConsumerHolder holder = mock(ChangeConsumerHolder.class);
        when(holder.get()).thenReturn(consumer);

        QuarkusChangeConsumer changeConsumer = new QuarkusChangeConsumer(holder);
        CapturingEvents<BatchEvent> events = testEvents();

        changeConsumer.handleBatch(events);

        verify(consumer).handle(events);
    }

    @SuppressWarnings("unchecked")
    @Test
    void wrapsInterruptedExceptionAndRestoresInterruptFlag() throws InterruptedException {
        DebeziumServerConsumer<CapturingEvents<BatchEvent>> consumer = mock(DebeziumServerConsumer.class);
        ChangeConsumerHolder holder = mock(ChangeConsumerHolder.class);
        when(holder.get()).thenReturn(consumer);
        doThrow(new InterruptedException("test interrupt")).when(consumer).handle(any());

        QuarkusChangeConsumer changeConsumer = new QuarkusChangeConsumer(holder);

        assertThatThrownBy(() -> changeConsumer.handleBatch(testEvents()))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(InterruptedException.class);

        assertThat(Thread.currentThread().isInterrupted()).isTrue();

        Thread.interrupted();
    }

    private CapturingEvents<BatchEvent> testEvents() {
        return new CapturingEvents<>() {
            @Override
            public List<BatchEvent> records() {
                return List.of();
            }

            @Override
            public String destination() {
                return "test-destination";
            }

            @Override
            public String source() {
                return "test-source";
            }

            @Override
            public String engine() {
                return "default";
            }
        };
    }
}
