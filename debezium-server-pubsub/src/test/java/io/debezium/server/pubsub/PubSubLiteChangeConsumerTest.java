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
 * Unit tests for the {@link PubSubLiteChangeConsumer} header to attribute mapping.
 */
class PubSubLiteChangeConsumerTest extends AbstractPubSubChangeConsumerTest {

    private PubSubLiteChangeConsumer consumer;

    @Override
    void createConsumer() {
        consumer = new PubSubLiteChangeConsumer();
        consumer.config = new PubSubLiteChangeConsumerConfig(Configuration.create()
                .with(PubSubLiteChangeConsumerConfig.ORDERING_ENABLED, false)
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
