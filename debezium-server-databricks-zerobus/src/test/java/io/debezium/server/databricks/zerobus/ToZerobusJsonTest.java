/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.BatchEvent;

/**
 * End-to-end tests of {@link ZerobusChangeConsumer#toZerobusJson}, the shared serialization entry
 * point used by both the gRPC and REST routes after integrating {@link ZerobusTypeSystem}. Exercises
 * the TYPED path (a {@link SourceRecord} carrying a {@link Struct}) across a wide range of data types
 * — the mapping that must stay correct once the connector is in production / accepted upstream — and
 * the verbatim fallback path.
 */
class ToZerobusJsonTest {

    @Test
    void typedPathNormalizesWideRangeOfDataTypes() throws Exception {
        // A rich schema spanning the type families that historically caused Zerobus 4044 errors.
        Schema schema = SchemaBuilder.struct().name("Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("c_smallint", Schema.INT16_SCHEMA)
                .field("c_bigint", Schema.INT64_SCHEMA)
                .field("c_double", Schema.FLOAT64_SCHEMA)
                .field("c_bool", Schema.BOOLEAN_SCHEMA)
                .field("c_text", Schema.STRING_SCHEMA)
                .field("c_decimal", Decimal.schema(4))
                .field("c_bytes", Schema.BYTES_SCHEMA)
                .field("c_json", SchemaBuilder.string().name("io.debezium.data.Json").build())
                .field("c_uuid", SchemaBuilder.string().name("io.debezium.data.Uuid").build())
                .field("c_date", SchemaBuilder.int32().name("io.debezium.time.Date").build())
                .field("c_micro_ts", SchemaBuilder.int64().name("io.debezium.time.MicroTimestamp").build())
                .field("c_zoned_ts", SchemaBuilder.string().name("io.debezium.time.ZonedTimestamp").build())
                .field("c_array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("c_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
                .build();

        Struct value = new Struct(schema)
                .put("id", 1)
                .put("c_smallint", (short) 32000)
                .put("c_bigint", 9_000_000_000L)
                .put("c_double", 3.14159)
                .put("c_bool", true)
                .put("c_text", "texto rico")
                .put("c_decimal", new BigDecimal("12345.6789"))
                .put("c_bytes", new byte[]{ (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF })
                .put("c_json", "{\"k\":\"v\"}")
                .put("c_uuid", "11111111-1111-1111-1111-111111111111")
                .put("c_date", 20148)
                .put("c_micro_ts", 1740839400000000L)
                .put("c_zoned_ts", "2025-03-01T14:30:00.000000-03:00")
                .put("c_array", List.of("a", "b", "c"))
                .put("c_map", Map.of("x", 10));

        String json = ZerobusChangeConsumer.toZerobusJson(typedEvent(schema, value), Object::toString);

        // Parse back and assert each type landed in the shape Zerobus/Delta expects.
        @SuppressWarnings("unchecked")
        Map<String, Object> m = new com.fasterxml.jackson.databind.ObjectMapper().readValue(json, Map.class);

        assertThat(m.get("id")).isEqualTo(1);
        assertThat(m.get("c_smallint")).isEqualTo(32000);
        assertThat(((Number) m.get("c_bigint")).longValue()).isEqualTo(9_000_000_000L);
        assertThat(m.get("c_double")).isEqualTo(3.14159);
        assertThat(m.get("c_bool")).isEqualTo(true);
        assertThat(m.get("c_text")).isEqualTo("texto rico");
        assertThat(m.get("c_decimal")).isEqualTo("12345.6789"); // decimal → string (precision kept)
        assertThat(m.get("c_bytes")).isEqualTo("3q2+7w=="); // bytes → base64
        assertThat(m.get("c_json")).isEqualTo("{\"k\":\"v\"}"); // Debezium JSON → string (VARIANT)
        assertThat(m.get("c_uuid")).isEqualTo("11111111-1111-1111-1111-111111111111");
        assertThat(m.get("c_date")).isEqualTo(20148); // io.debezium.time.Date → int days
        assertThat(((Number) m.get("c_micro_ts")).longValue()).isEqualTo(1740839400000000L); // micros
        assertThat(m.get("c_zoned_ts")).isEqualTo("2025-03-01T14:30:00.000000-03:00");
        assertThat(m.get("c_array")).isEqualTo(List.of("a", "b", "c")); // homogeneous ARRAY<STRING>
        assertThat(m.get("c_map")).isEqualTo(Map.of("x", 10));
    }

    @Test
    void fallsBackToVerbatimWhenNoSchema() {
        // No typed SourceRecord → verbatim path (backwards-compatible with format.value=json).
        String json = ZerobusChangeConsumer.toZerobusJson(untypedEvent("{\"id\":9}"), v -> (String) v);
        assertThat(json).isEqualTo("{\"id\":9}");
    }

    @Test
    void returnsNullForTombstone() {
        assertThat(ZerobusChangeConsumer.toZerobusJson(untypedEvent(null), v -> (String) v)).isNull();
    }

    // --- helpers -------------------------------------------------------------

    private static BatchEvent typedEvent(Schema schema, Struct value) {
        SourceRecord sr = new SourceRecord(null, null, "topic", 0, null, null, schema, value);
        return baseEvent(value, sr);
    }

    private static BatchEvent untypedEvent(String jsonValue) {
        return baseEvent(jsonValue, null);
    }

    private static BatchEvent baseEvent(Object value, SourceRecord record) {
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
