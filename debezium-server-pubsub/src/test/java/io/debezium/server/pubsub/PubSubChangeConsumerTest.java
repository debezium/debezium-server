/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import com.google.pubsub.v1.PubsubMessage;

import io.debezium.config.Configuration;
import io.debezium.runtime.BatchEvent;

/**
 * Unit tests for the {@link PubSubChangeConsumer} header to attribute mapping.
 */
class PubSubChangeConsumerTest extends AbstractPubSubChangeConsumerTest {

    private PubSubChangeConsumer consumer;

    @Override
    void createConsumer() {
        consumer = new PubSubChangeConsumer();
        consumer.config = new PubSubChangeConsumerConfig(Configuration.create()
                .with(PubSubChangeConsumerConfig.ORDERING_ENABLED, false)
                .build());
    }

    @Override
    void setHeaderSchemasEnabled(boolean enabled) {
        consumer.headerSchemasEnabled = enabled;
    }

    @Override
    PubsubMessage buildPubSubMessage(BatchEvent event) {
        return consumer.buildPubSubMessage(event);
    }
}
