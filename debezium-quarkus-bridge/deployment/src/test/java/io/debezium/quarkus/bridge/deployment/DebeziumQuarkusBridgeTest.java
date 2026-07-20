/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.bridge.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.engine.format.Json;
import io.debezium.quarkus.bridge.QuarkusChangeConsumer;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.runtime.DebeziumSerialization;
import io.debezium.server.api.ChangeConsumerHolder;
import io.quarkus.test.QuarkusUnitTest;

class DebeziumQuarkusBridgeTest {

    @RegisterExtension
    static final QuarkusUnitTest unitTest = new QuarkusUnitTest()
            .withApplicationRoot(jar -> jar.addClasses(TestSinkConsumer.class));

    @Inject
    QuarkusChangeConsumer changeConsumer;

    @Inject
    ChangeConsumerHolder changeConsumerHolder;

    @Inject
    DebeziumSerialization<String, String, String> serialization;

    @Inject
    TestSinkConsumer testSink;

    @Test
    void changeConsumerBeanIsAvailable() {
        assertThat(changeConsumer).isNotNull();
    }

    @Test
    void changeConsumerHolderIsAvailable() {
        assertThat(changeConsumerHolder).isNotNull();
        assertThat(changeConsumerHolder.get()).isNotNull();
    }

    @Test
    void serializationDefaultsToJson() {
        assertThat(serialization).isNotNull();
        assertThat(serialization.getKeyFormat()).isEqualTo(Json.class);
        assertThat(serialization.getValueFormat()).isEqualTo(Json.class);
        assertThat(serialization.getHeaderFormat()).isEqualTo(Json.class);
        assertThat(serialization.getEngineId()).isEqualTo("default");
    }

    @Test
    void changeConsumerDelegatesToSink() {
        CapturingEvents<BatchEvent> events = new CapturingEvents<>() {
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

        changeConsumer.handleBatch(events);

        assertThat(testSink.getReceived()).hasSize(1);
        assertThat(testSink.getReceived().getFirst()).isSameAs(events);
    }

    @Test
    void holderResolvesToTestSink() {
        assertThat(changeConsumerHolder.get()).isInstanceOf(TestSinkConsumer.class);
    }
}
