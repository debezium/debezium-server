/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.BatchEvent;

/**
 * Unit tests for {@link ChangeEventToSinkRecordConverter}.
 *
 * @author Chris Cranford
 */
public class ChangeEventToSinkRecordConverterTest {

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("key").field("id", Schema.INT32_SCHEMA).build();
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("value").field("name", Schema.STRING_SCHEMA).build();

    private final ChangeEventToSinkRecordConverter converter = new ChangeEventToSinkRecordConverter();

    @Test
    public void shouldCopyTopicKeyValueSchemasTimestampAndHeaders() {
        final Struct key = new Struct(KEY_SCHEMA).put("id", 1001);
        final Struct value = new Struct(VALUE_SCHEMA).put("name", "Sally");
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addString("h1", "v1");
        final SourceRecord sourceRecord = new SourceRecord(Map.of("server", "testserver"), Map.of("lsn", 1L),
                "testserver.inventory.customers", null, KEY_SCHEMA, key, VALUE_SCHEMA, value, 1234L, headers);

        final SinkRecord sinkRecord = converter.convert(new TestBatchEvent(sourceRecord));

        assertThat(sinkRecord.topic()).isEqualTo("testserver.inventory.customers");
        assertThat(sinkRecord.kafkaPartition()).isEqualTo(0);
        assertThat(sinkRecord.kafkaOffset()).isEqualTo(0L);
        assertThat(sinkRecord.keySchema()).isSameAs(KEY_SCHEMA);
        assertThat(sinkRecord.key()).isSameAs(key);
        assertThat(sinkRecord.valueSchema()).isSameAs(VALUE_SCHEMA);
        assertThat(sinkRecord.value()).isSameAs(value);
        assertThat(sinkRecord.timestamp()).isEqualTo(1234L);
        assertThat(sinkRecord.headers().lastWithName("h1").value()).isEqualTo("v1");
    }

    @Test
    public void shouldConvertTombstoneWithNullValue() {
        final Struct key = new Struct(KEY_SCHEMA).put("id", 1001);
        final SourceRecord sourceRecord = new SourceRecord(Map.of(), Map.of(), "testserver.inventory.customers",
                KEY_SCHEMA, key, null, null);

        final SinkRecord sinkRecord = converter.convert(new TestBatchEvent(sourceRecord));

        assertThat(sinkRecord.key()).isSameAs(key);
        assertThat(sinkRecord.valueSchema()).isNull();
        assertThat(sinkRecord.value()).isNull();
    }

    @Test
    public void shouldRejectEventWithoutSourceRecord() {
        assertThatThrownBy(() -> converter.convert(new TestBatchEvent(null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SourceRecord is null");
    }

    private static final class TestBatchEvent implements BatchEvent {

        private final SourceRecord record;

        TestBatchEvent(SourceRecord record) {
            this.record = record;
        }

        @Override
        public Object key() {
            return record != null ? record.key() : null;
        }

        @Override
        public Object value() {
            return record != null ? record.value() : null;
        }

        @Override
        public Integer partition() {
            return null;
        }

        @Override
        public SourceRecord record() {
            return record;
        }

        @Override
        public String destination() {
            return record != null ? record.topic() : null;
        }

        @Override
        public void commit() {
        }
    }
}
