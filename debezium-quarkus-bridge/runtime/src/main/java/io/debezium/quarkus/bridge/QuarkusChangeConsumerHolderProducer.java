/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.bridge;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import io.debezium.DebeziumException;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.ChangeConsumerHolder;
import io.debezium.server.api.DebeziumServerConsumer;

@ApplicationScoped
public class QuarkusChangeConsumerHolderProducer {

    private final Instance<DebeziumServerConsumer<CapturingEvents<BatchEvent>>> instance;

    @Inject
    public QuarkusChangeConsumerHolderProducer(Instance<DebeziumServerConsumer<CapturingEvents<BatchEvent>>> instance) {
        this.instance = instance;
    }

    @Produces
    public ChangeConsumerHolder produces() {
        List<DebeziumServerConsumer<CapturingEvents<BatchEvent>>> consumers = instance.stream().toList();

        if (consumers.size() > 1) {
            throw new DebeziumException("Found multiple sink. In bridge mode, you can have only one sink");
        }

        return new ChangeConsumerHolder() {
            @Override
            public DebeziumServerConsumer<CapturingEvents<BatchEvent>> get() {
                return consumers.get(0);
            }

            @Override
            public Optional<Boolean> tombstoneSupport() {
                return consumers.get(0).tombstoneSupport();
            }
        };
    }
}
