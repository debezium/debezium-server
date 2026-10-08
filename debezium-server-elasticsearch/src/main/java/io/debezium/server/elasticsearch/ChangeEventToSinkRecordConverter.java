/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.elasticsearch;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.BatchEvent;

/**
 * Converts a Debezium Server {@link BatchEvent} to a Kafka Connect {@link SinkRecord} for use with
 * the Elasticsearch sink connector task.
 * <p>
 * The batch event wraps the engine's {@link SourceRecord}, and a sink record shares the same
 * structure (topic, key, value, schemas, and headers), so the conversion is a field-by-field copy.
 * The partition and offset are fixed because Debezium Server has no Kafka partitions; the task
 * tracks them only to report offsets back to Kafka Connect.
 *
 * @author Chris Cranford
 */
public class ChangeEventToSinkRecordConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventToSinkRecordConverter.class);

    /**
     * Converts a batch event to a sink record.
     *
     * @param event the change event from Debezium Server
     * @return the equivalent sink record for the Elasticsearch connector task
     * @throws IllegalArgumentException if the event carries no source record
     */
    public SinkRecord convert(BatchEvent event) {
        final SourceRecord sourceRecord = event.record();

        if (sourceRecord == null) {
            throw new IllegalArgumentException("SourceRecord is null in BatchEvent");
        }

        LOGGER.trace("Converting SourceRecord to SinkRecord: topic={}", sourceRecord.topic());

        return new SinkRecord(
                sourceRecord.topic(),
                0, // partition - Debezium Server has no Kafka partitions
                sourceRecord.keySchema(),
                sourceRecord.key(),
                sourceRecord.valueSchema(),
                sourceRecord.value(),
                0L, // offset - not used by the Elasticsearch sink under Debezium Server
                sourceRecord.timestamp(),
                null, // timestampType - SourceRecord doesn't have this
                sourceRecord.headers());
    }
}
