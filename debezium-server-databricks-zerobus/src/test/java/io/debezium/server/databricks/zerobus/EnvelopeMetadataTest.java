/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.BatchEvent;

/**
 * Unit tests for the envelope-metadata extractors {@link ZerobusChangeConsumer#operationOf} and
 * {@link ZerobusChangeConsumer#sourceTsMsOf}, which feed the sink metrics (per-operation counters and
 * the freshness / lag-behind-source gauge). They cover the well-formed Debezium envelope and the
 * degenerate shapes that must degrade gracefully to {@code null} / {@code -1}.
 */
class EnvelopeMetadataTest {

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().name("source")
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build();

    private static final Schema ENVELOPE_SCHEMA = SchemaBuilder.struct().name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("source", SOURCE_SCHEMA)
            .build();

    @Test
    void extractsOperationAndSourceTimestampFromEnvelope() {
        Struct source = new Struct(SOURCE_SCHEMA).put("ts_ms", 1_700_000_000_000L);
        Struct envelope = new Struct(ENVELOPE_SCHEMA).put("op", "u").put("source", source);
        BatchEvent event = eventWith(envelope, ENVELOPE_SCHEMA);

        assertThat(ZerobusChangeConsumer.operationOf(event)).isEqualTo("u");
        assertThat(ZerobusChangeConsumer.sourceTsMsOf(event)).isEqualTo(1_700_000_000_000L);
    }

    @Test
    void nullRecordYieldsNullOpAndMinusOne() {
        BatchEvent event = eventWith(null, null);
        assertThat(ZerobusChangeConsumer.operationOf(event)).isNull();
        assertThat(ZerobusChangeConsumer.sourceTsMsOf(event)).isEqualTo(-1L);
    }

    @Test
    void structWithoutOpOrSourceDegradesGracefully() {
        Schema plain = SchemaBuilder.struct().name("Value").field("id", Schema.INT32_SCHEMA).build();
        Struct value = new Struct(plain).put("id", 7);
        BatchEvent event = eventWith(value, plain);

        assertThat(ZerobusChangeConsumer.operationOf(event)).isNull();
        assertThat(ZerobusChangeConsumer.sourceTsMsOf(event)).isEqualTo(-1L);
    }

    @Test
    void extractsFromUnwrappedRecordWithFlattenedFields() {
        // The recommended ExtractNewRecordState setup (add.fields=op,source.ts_ms, default __ prefix)
        // exposes the metadata as top-level __op / __source_ts_ms rather than a nested envelope.
        Schema unwrapped = SchemaBuilder.struct().name("Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("__op", Schema.STRING_SCHEMA)
                .field("__source_ts_ms", Schema.INT64_SCHEMA)
                .build();
        Struct value = new Struct(unwrapped).put("id", 1).put("__op", "d").put("__source_ts_ms", 1_700_000_000_500L);
        BatchEvent event = eventWith(value, unwrapped);

        assertThat(ZerobusChangeConsumer.operationOf(event)).isEqualTo("d");
        assertThat(ZerobusChangeConsumer.sourceTsMsOf(event)).isEqualTo(1_700_000_000_500L);
    }

    @Test
    void envelopeWithSourceButNoTsMsYieldsMinusOne() {
        Schema sourceNoTs = SchemaBuilder.struct().name("source").field("db", Schema.STRING_SCHEMA).build();
        Schema env = SchemaBuilder.struct().name("Envelope")
                .field("op", Schema.STRING_SCHEMA)
                .field("source", sourceNoTs)
                .build();
        Struct value = new Struct(env).put("op", "c").put("source", new Struct(sourceNoTs).put("db", "x"));
        BatchEvent event = eventWith(value, env);

        assertThat(ZerobusChangeConsumer.operationOf(event)).isEqualTo("c");
        assertThat(ZerobusChangeConsumer.sourceTsMsOf(event)).isEqualTo(-1L);
    }

    private static BatchEvent eventWith(Object value, Schema valueSchema) {
        SourceRecord record = value == null ? null
                : new SourceRecord(null, null, "topic", 0, null, null, valueSchema, value);
        return new BatchEvent() {
            public Object key() {
                return null;
            }

            public Object value() {
                return value;
            }

            public Integer partition() {
                return 0;
            }

            public SourceRecord record() {
                return record;
            }

            public String destination() {
                return "topic";
            }

            public void commit() {
            }
        };
    }
}
