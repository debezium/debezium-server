/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.util.Testing;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Dependent
@Named("test")
public class TestConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

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
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        records.forEach(record -> {
            Testing.print(record);
            values.add(record.value());
            try {
                committer.markProcessed(record);
            }
            catch (InterruptedException e) {
                throw new DebeziumException(e);
            }
        });
    }

    public List<Object> getValues() {
        return values;
    }
}
