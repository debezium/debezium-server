/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Unit tests for {@link KafkaChangeConsumerConfig}.
 */
public class KafkaChangeConsumerConfigTest {

    @Test
    public void testWaitMessageDeliveryTimeoutDefaultsToZero() {
        KafkaChangeConsumerConfig config = new KafkaChangeConsumerConfig(Configuration.from(Map.of()));
        assertEquals(0, config.getWaitMessageDeliveryTimeout());
    }

    @Test
    public void testWaitMessageDeliveryTimeoutExplicitValueResolved() {
        KafkaChangeConsumerConfig config = new KafkaChangeConsumerConfig(Configuration.from(Map.of(
                "wait.message.delivery.timeout.ms", "5000")));
        assertEquals(5000, config.getWaitMessageDeliveryTimeout());
    }

    @Test
    public void testWaitMessageDeliveryTimeoutNegativeValueFailsValidation() {
        Configuration configuration = Configuration.from(Map.of(
                "wait.message.delivery.timeout.ms", "-1"));

        List<String> problems = new ArrayList<>();
        boolean valid = configuration.validateAndRecord(
                Field.setOf(KafkaChangeConsumerConfig.WAIT_MESSAGE_DELIVERY_TIMEOUT_MS), problems::add);

        assertFalse(valid);
        assertTrue(problems.stream().anyMatch(problem -> problem.contains("wait.message.delivery.timeout.ms")));
    }

    @Test
    public void testWaitMessageDeliveryTimeoutZeroPassesValidation() {
        Configuration configuration = Configuration.from(Map.of(
                "wait.message.delivery.timeout.ms", "0"));

        List<String> problems = new ArrayList<>();
        boolean valid = configuration.validateAndRecord(
                Field.setOf(KafkaChangeConsumerConfig.WAIT_MESSAGE_DELIVERY_TIMEOUT_MS), problems::add);

        assertTrue(valid);
        assertTrue(problems.isEmpty());
    }
}
