/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;

/**
 * Converts Debezium Server's ChangeEvent (EmbeddedEngineChangeEvent) to Kafka Connect's SinkRecord
 * for use with JdbcChangeEventSink.
 * <p>
 * This is a simple conversion because EmbeddedEngineChangeEvent internally wraps a SourceRecord,
 * and SinkRecord shares the same structure (topic, partition, key, value, schemas, headers, etc.).
 *
 * @author Mario Fiore Vitale
 */
public class ChangeEventToSinkRecordConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventToSinkRecordConverter.class);

    /**
     * Converts a ChangeEvent to a SinkRecord.
     * <p>
     * The conversion extracts the underlying SourceRecord from EmbeddedEngineChangeEvent
     * and maps its fields to a SinkRecord.
     *
     * @param event the change event from Debezium Server
     * @return the equivalent SinkRecord for JDBC connector
     * @throws IllegalArgumentException if the event is not an EmbeddedEngineChangeEvent
     */
    public SinkRecord convert(ChangeEvent<Object, Object> event) {
        if (!(event instanceof EmbeddedEngineChangeEvent)) {
            throw new IllegalArgumentException(
                    "Expected EmbeddedEngineChangeEvent but got: " + event.getClass().getName() +
                            ". Ensure Debezium Server is using EmbeddedEngine.");
        }

        @SuppressWarnings("unchecked")
        EmbeddedEngineChangeEvent<Object, Object, Object> embeddedEvent = (EmbeddedEngineChangeEvent<Object, Object, Object>) event;

        SourceRecord sourceRecord = embeddedEvent.sourceRecord();

        if (sourceRecord == null) {
            throw new IllegalArgumentException("SourceRecord is null in EmbeddedEngineChangeEvent");
        }

        LOGGER.trace("Converting SourceRecord to SinkRecord: topic={}",
                sourceRecord.topic());

        return new SinkRecord(
                sourceRecord.topic(),
                0, // partition - not used by JDBC sink
                sourceRecord.keySchema(),
                sourceRecord.key(),
                sourceRecord.valueSchema(),
                sourceRecord.value(),
                0L, // offset - not used by JDBC sink
                sourceRecord.timestamp(),
                null, // timestampType - SourceRecord doesn't have this
                sourceRecord.headers());
    }
}
