/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.bridge.deployment;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.DebeziumServerConsumer;

@ApplicationScoped
public class TestSinkConsumer implements DebeziumServerConsumer<CapturingEvents<BatchEvent>> {

    private final List<CapturingEvents<BatchEvent>> received = new ArrayList<>();

    @Override
    public void handle(CapturingEvents<BatchEvent> events) {
        received.add(events);
    }

    public List<CapturingEvents<BatchEvent>> getReceived() {
        return received;
    }
}
