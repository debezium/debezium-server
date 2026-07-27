/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for the {@link ZerobusTypeSystem} prototype — the destination-aware type mapping that
 * turns a Connect {@link Struct}/{@link Schema} into the JSON value shape Zerobus/Delta expects.
 * Each case pins a mapping that otherwise produced a Zerobus "invalid type" (4044) error.
 */
class ZerobusTypeSystemTest {

    @Test
    void decimalPreciseBecomesStringPreservingPrecision() {
        Schema schema = Decimal.schema(4); // org.apache.kafka.connect.data.Decimal
        Object out = ZerobusTypeSystem.normalize(schema, new BigDecimal("12345.6789"));
        assertThat(out).isEqualTo("12345.6789");
    }

    @Test
    void decimalFromUnscaledBytesRebuildsExactValue() {
        // decimal.handling.mode default delivers the unscaled two's-complement bytes, not a BigDecimal.
        // The scale lives in the schema parameter, so the exact value must be rebuilt (regression guard:
        // previously these were emitted as a base64 blob, which a native Delta DECIMAL column rejects).
        Schema schema = Decimal.schema(2); // scale=2
        byte[] unscaled = new BigDecimal("259.80").unscaledValue().toByteArray(); // 25980 → 0x657C
        Object out = ZerobusTypeSystem.normalize(schema, unscaled);
        assertThat(out).isEqualTo("259.80");
    }

    @ParameterizedTest
    @ValueSource(strings = { "de-DE", "pt-BR", "fr-FR", "en-US" })
    void decimalIsLocaleIndependent(String languageTag) {
        // A deploy in Brazil/Germany boots the JVM in a comma-decimal, dot-thousands locale. The
        // Delta DECIMAL contract (and Zerobus's parser) is fixed on '.' as the decimal point with NO
        // thousands grouping. BigDecimal.toString() is locale-independent by spec, but this pins it
        // so a future refactor to String.format/NumberFormat (which WOULD localize) fails loudly.
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag(languageTag));
            Schema schema = Decimal.schema(2);
            // Value with thousands (1.234.567,89 in de-DE) and a fractional part — the worst case for grouping/comma.
            byte[] unscaled = new BigDecimal("1234567.89").unscaledValue().toByteArray();
            assertThat(ZerobusTypeSystem.normalize(schema, unscaled)).isEqualTo("1234567.89");
            // Precise BigDecimal path, high precision + negative.
            assertThat(ZerobusTypeSystem.normalize(Decimal.schema(6), new BigDecimal("-0.000001"))).isEqualTo("-0.000001");
            assertThat(ZerobusTypeSystem.normalize(Decimal.schema(4), new BigDecimal("9876543.2100"))).isEqualTo("9876543.2100");
        }
        finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void bytesBecomeBase64() {
        Object out = ZerobusTypeSystem.normalize(Schema.BYTES_SCHEMA, new byte[]{ (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF });
        assertThat(out).isEqualTo("3q2+7w==");
    }

    @Test
    void debeziumJsonKeptVerbatimForVariant() {
        Schema json = SchemaBuilder.string().name("io.debezium.data.Json").build();
        Object out = ZerobusTypeSystem.normalize(json, "{\"k\":\"v\"}");
        assertThat(out).isEqualTo("{\"k\":\"v\"}");
    }

    @Test
    void debeziumNumericTemporalsPassThrough() {
        Schema microTs = SchemaBuilder.int64().name("io.debezium.time.MicroTimestamp").build();
        assertThat(ZerobusTypeSystem.normalize(microTs, 1740839400000000L)).isEqualTo(1740839400000000L);

        Schema date = SchemaBuilder.int32().name("io.debezium.time.Date").build();
        assertThat(ZerobusTypeSystem.normalize(date, 20148)).isEqualTo(20148);
    }

    @Test
    void zonedTimestampKeptAsIsoString() {
        Schema zoned = SchemaBuilder.string().name("io.debezium.time.ZonedTimestamp").build();
        Object out = ZerobusTypeSystem.normalize(zoned, "2025-03-01T14:30:00.000000-03:00");
        assertThat(out).isEqualTo("2025-03-01T14:30:00.000000-03:00");
    }

    @Test
    void normalizesNestedStructWithMixedTypes() {
        Schema schema = SchemaBuilder.struct().name("Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("price", Decimal.schema(2))
                .field("photo", Schema.BYTES_SCHEMA)
                .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .build();
        Struct value = new Struct(schema)
                .put("id", 7)
                .put("price", new BigDecimal("9.90"))
                .put("photo", new byte[]{ 1, 2, 3 })
                .put("tags", List.of("a", "b"));

        @SuppressWarnings("unchecked")
        Map<String, Object> out = (Map<String, Object>) ZerobusTypeSystem.normalize(schema, value);

        assertThat(out.get("id")).isEqualTo(7);
        assertThat(out.get("price")).isEqualTo("9.90"); // decimal → string
        assertThat(out.get("photo")).isEqualTo("AQID"); // bytes → base64
        assertThat(out.get("tags")).isEqualTo(List.of("a", "b")); // homogeneous ARRAY<STRING>
    }

    @Test
    void mapKeysAreStringified() {
        Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
        Object out = ZerobusTypeSystem.normalize(schema, Map.of("x", 10));
        assertThat(out).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) out).get("x")).isEqualTo(10);
    }

    @Test
    void connectDateBecomesDaysNotMillis() {
        // time.precision.mode=connect delivers org.apache.kafka.connect.data.Date as a java.util.Date
        // at UTC midnight; the Delta INT column expects DAYS since epoch, not millis (regression guard).
        Schema connectDate = org.apache.kafka.connect.data.Date.SCHEMA;
        java.util.Date utcMidnight = new java.util.Date(20148L * 86_400_000L); // 2025-03-01
        Object out = ZerobusTypeSystem.normalize(connectDate, utcMidnight);
        assertThat(out).isEqualTo(20148);
    }

    @Test
    void connectTimestampBecomesEpochMicros() {
        // Connect Timestamp is millis; a native Delta TIMESTAMP column expects micros → ×1000.
        Schema connectTs = org.apache.kafka.connect.data.Timestamp.SCHEMA;
        java.util.Date d = new java.util.Date(1740839400000L);
        assertThat(ZerobusTypeSystem.normalize(connectTs, d)).isEqualTo(1740839400000000L);
    }

    @Test
    void debeziumTimestampMillisBecomesMicros() {
        // io.debezium.time.Timestamp is millis → micros for a TIMESTAMP column.
        Schema ts = SchemaBuilder.int64().name("io.debezium.time.Timestamp").build();
        assertThat(ZerobusTypeSystem.normalize(ts, 1740839400000L)).isEqualTo(1740839400000000L);
    }

    @Test
    void debeziumNanoTimestampBecomesMicros() {
        // io.debezium.time.NanoTimestamp is nanos → micros (÷1000).
        Schema ts = SchemaBuilder.int64().name("io.debezium.time.NanoTimestamp").build();
        assertThat(ZerobusTypeSystem.normalize(ts, 1740839400000000000L)).isEqualTo(1740839400000000L);
    }

    @Test
    void connectTimeBecomesMillisSinceMidnight() {
        Schema connectTime = org.apache.kafka.connect.data.Time.SCHEMA;
        java.util.Date t = new java.util.Date(52_200_000L); // 14:30:00
        assertThat(ZerobusTypeSystem.normalize(connectTime, t)).isEqualTo(52_200_000L);
    }

    @Test
    void nullValueReturnsNull() {
        assertThat(ZerobusTypeSystem.normalize(Schema.OPTIONAL_STRING_SCHEMA, null)).isNull();
    }
}
