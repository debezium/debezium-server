/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link KafkaChangeConsumer}.
 */
public class KafkaChangeConsumerConfig {

    public static final Field WAIT_MESSAGE_DELIVERY_TIMEOUT_MS = Field.create("wait.message.delivery.timeout.ms")
            .withDisplayName("Wait message delivery timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(0)
            .withValidation(Field::isNonNegativeInteger)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Timeout in milliseconds to wait for message delivery confirmation. "
                    + "A value of 0 (the default) waits indefinitely, deferring entirely to the Kafka producer's own "
                    + "'delivery.timeout.ms'. If set to a positive value, it should be greater than or equal to the "
                    + "effective 'delivery.timeout.ms' of the underlying producer (explicit override, or its default) "
                    + "otherwise this outer wait may time out and report failure while the producer is still "
                    + "legitimately retrying in the background.");

    // Instance field
    private int waitMessageDeliveryTimeout;

    public KafkaChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        waitMessageDeliveryTimeout = config.getInteger(WAIT_MESSAGE_DELIVERY_TIMEOUT_MS);
    }

    public int getWaitMessageDeliveryTimeout() {
        return waitMessageDeliveryTimeout;
    }
}
