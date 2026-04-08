/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.debezium.server.api.DebeziumServerConsumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.util.Testing;
import jakarta.inject.Singleton;

@Singleton
@Named("test")
public class TestConsumer implements DebeziumServerConsumer<CapturingEvents<BatchEvent>> {

    final List<Object> values = Collections.synchronizedList(new ArrayList<>());

    @PostConstruct
    void init() {
        Testing.print("Test consumer constructed");
    }

    @PreDestroy
    void close() {
        Testing.print("Test consumer destroyed");
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events) throws InterruptedException {
        events.records().forEach(record -> {
            Testing.print(record);
            values.add(record.value());
            record.commit();
        });
    }

    public List<Object> getValues() {
        return values;
    }
}
