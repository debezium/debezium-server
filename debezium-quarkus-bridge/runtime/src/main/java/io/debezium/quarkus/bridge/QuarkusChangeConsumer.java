/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.bridge;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.ChangeConsumerHolder;
import io.quarkus.arc.Unremovable;

@ApplicationScoped
@Unremovable
public class QuarkusChangeConsumer {

    private final ChangeConsumerHolder changeConsumerHolder;

    @Inject
    public QuarkusChangeConsumer(ChangeConsumerHolder changeConsumerHolder) {
        this.changeConsumerHolder = changeConsumerHolder;
    }

    @Capturing
    public void handleBatch(CapturingEvents<BatchEvent> events) {
        try {
            this.changeConsumerHolder.get().handle(events);
        }
        catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();

            throw new RuntimeException(interruptedException);
        }
    }
}
