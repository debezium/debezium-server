/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;

/**
 * Destination-aware type mapping between the Debezium/Kafka Connect {@link Schema}/{@link Struct}
 * representation of a change event and the JSON value shape that Databricks Zerobus (and the target
 * managed Delta table) expects. This is the equivalent, for the Zerobus sink, of the per-dialect
 * {@code type system} in {@code debezium-connector-jdbc}: it is the layer that owns the mapping to
 * the destination's contract, which is intentionally NOT the job of SMTs.
 * <p>
 * It normalizes by the schema's <em>logical type name</em> (e.g. {@code org.apache.kafka.connect
 * .data.Decimal}, {@code io.debezium.time.MicroTimestamp}, {@code io.debezium.data.Json}) rather
 * than by the already-degraded JSON, because Zerobus validates strictly and does no coercion. The
 * result is a plain {@link Map}/{@link List}/scalar tree that a JSON serializer turns into the body
 * Zerobus accepts, avoiding the "invalid type: X, expected Y" (error 4044) class of failures.
 * <p>
 * Scope is deliberately narrow: type coercion only. Reshaping (rename/route/flatten) stays in SMTs.
 */
final class ZerobusTypeSystem {

    // Kafka Connect logical types
    private static final String CONNECT_DECIMAL = "org.apache.kafka.connect.data.Decimal";
    private static final String CONNECT_DATE = "org.apache.kafka.connect.data.Date";
    private static final String CONNECT_TIME = "org.apache.kafka.connect.data.Time";
    private static final String CONNECT_TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";

    // Debezium logical types (io.debezium.time.* / io.debezium.data.*)
    private static final String DBZ_JSON = "io.debezium.data.Json";
    private static final String DBZ_ENUM = "io.debezium.data.Enum";
    private static final String DBZ_UUID = "io.debezium.data.Uuid";
    private static final String DBZ_BITS = "io.debezium.data.Bits";
    private static final String DBZ_ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";
    private static final String DBZ_ZONED_TIME = "io.debezium.time.ZonedTime";

    // Debezium temporal logical types. A Delta DATE column expects epoch-days; a Delta TIMESTAMP
    // column expects epoch-MICROS (verified against Zerobus). Debezium emits each at a source-native
    // precision, tagged by the logical type name — we normalize every timestamp to micros and every
    // date to days so a native DATE/TIMESTAMP target column is fed correctly (bronze faithful to
    // source: a temporal in the source becomes a temporal in Delta, never a raw number).
    private static final String DBZ_DATE = "io.debezium.time.Date"; // days
    private static final String DBZ_TIMESTAMP = "io.debezium.time.Timestamp"; // millis
    private static final String DBZ_MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp"; // micros
    private static final String DBZ_NANO_TIMESTAMP = "io.debezium.time.NanoTimestamp"; // nanos

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ZerobusTypeSystem() {
    }

    /**
     * Normalizes a top-level record value (expected to be a {@link Struct}) into a JSON-ready map.
     * Returns {@code null} for a null/tombstone value.
     */
    static Object normalize(Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        return convert(schema, value);
    }

    /**
     * Normalizes a record value and serializes it to the JSON string Zerobus ingests. Returns
     * {@code null} for a null/tombstone value (the caller skips it).
     */
    static String normalizeToJson(Schema schema, Object value) {
        Object normalized = normalize(schema, value);
        if (normalized == null) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(normalized);
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Failed to serialize normalized record to JSON", e);
        }
    }

    private static Object convert(Schema schema, Object value) {
        if (value == null) {
            return null;
        }

        // Logical types are keyed by schema name and take precedence over the primitive kind.
        final String logical = schema != null ? schema.name() : null;
        if (logical != null) {
            Object mapped = convertLogical(schema, logical, value);
            if (mapped != SENTINEL) {
                return mapped;
            }
        }

        if (schema == null) {
            return value; // schemaless: pass through
        }

        switch (schema.type()) {
            case STRUCT:
                return convertStruct(schema, (Struct) value);
            case ARRAY:
                return convertArray(schema, (List<?>) value);
            case MAP:
                return convertMap(schema, (Map<?, ?>) value);
            case BYTES:
                return base64(value);
            default:
                return value; // primitives (int/long/float/double/boolean/string) pass through
        }
    }

    private static final Object SENTINEL = new Object();

    /** Returns the mapped value for a known logical type, or {@link #SENTINEL} if not handled here. */
    private static Object convertLogical(Schema schema, String logical, Object value) {
        switch (logical) {
            case CONNECT_DECIMAL:
                // A Delta DECIMAL(p,s) column is fed by a JSON string that preserves precision.
                // precise mode delivers a java.math.BigDecimal directly.
                if (value instanceof BigDecimal) {
                    return value.toString();
                }
                // Some paths (notably the JSON/binary decimal.handling.mode default) deliver the
                // unscaled two's-complement bytes; the scale lives in the schema parameter, so we can
                // rebuild the exact BigDecimal instead of emitting a base64 blob that a native
                // DECIMAL column would reject.
                if (value instanceof byte[] || value instanceof ByteBuffer) {
                    return Decimal.toLogical(schema, toBytes(value)).toString();
                }
                return value.toString();
            case DBZ_JSON:
                // already a JSON string; keep verbatim so a VARIANT column parses it.
                return value;
            case DBZ_ENUM:
            case DBZ_UUID:
            case DBZ_ZONED_TIMESTAMP:
            case DBZ_ZONED_TIME:
                // string logical types: keep as-is (ISO string / enum literal / uuid text).
                return value;
            case DBZ_BITS:
                return base64(value);

            // --- Temporals: normalize to what a native Delta DATE (epoch-days) / TIMESTAMP
            // (epoch-MICROS) column expects, using the logical type's unit. ---
            case DBZ_DATE:
                // io.debezium.time.Date: already days since epoch → feeds a DATE column directly.
                return value;
            case DBZ_MICRO_TIMESTAMP:
                // already micros → TIMESTAMP column directly.
                return value;
            case DBZ_TIMESTAMP:
                // io.debezium.time.Timestamp is millis → micros for a TIMESTAMP column.
                return asLong(value) * 1000L;
            case DBZ_NANO_TIMESTAMP:
                // nanos → micros.
                return asLong(value) / 1000L;
            case CONNECT_DATE:
                // time.precision.mode=connect: java.util.Date at UTC midnight → days since epoch.
                if (value instanceof java.util.Date) {
                    return (int) (((java.util.Date) value).getTime() / 86_400_000L);
                }
                return value;
            case CONNECT_TIMESTAMP:
                // Connect Timestamp: java.util.Date (millis) → micros for a TIMESTAMP column.
                if (value instanceof java.util.Date) {
                    return ((java.util.Date) value).getTime() * 1000L;
                }
                return value;
            case CONNECT_TIME:
                // Connect Time: java.util.Date whose epoch millis are millis-since-midnight; a Delta
                // TIME type does not exist, so leave the value for a numeric (BIGINT) column.
                if (value instanceof java.util.Date) {
                    return ((java.util.Date) value).getTime();
                }
                return value;
            default:
                // Other io.debezium.time.* (Time/MicroTime/NanoTime, Year, …): no native Delta type,
                // pass the numeric value through as-is for a numeric target column.
                return SENTINEL;
        }
    }

    private static long asLong(Object value) {
        return ((Number) value).longValue();
    }

    private static Map<String, Object> convertStruct(Schema schema, Struct struct) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Field field : schema.fields()) {
            out.put(field.name(), convert(field.schema(), struct.get(field)));
        }
        return out;
    }

    private static List<Object> convertArray(Schema schema, List<?> list) {
        Schema valueSchema = schema.valueSchema();
        List<Object> out = new java.util.ArrayList<>(list.size());
        for (Object el : list) {
            out.add(convert(valueSchema, el));
        }
        return out;
    }

    private static Map<String, Object> convertMap(Schema schema, Map<?, ?> map) {
        // Zerobus/Delta MAP keys must be strings in JSON; stringify keys, convert values by valueSchema.
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : map.entrySet()) {
            out.put(String.valueOf(e.getKey()), convert(schema.valueSchema(), e.getValue()));
        }
        return out;
    }

    private static String base64(Object value) {
        byte[] bytes = toBytes(value);
        return bytes != null ? Base64.getEncoder().encodeToString(bytes) : String.valueOf(value);
    }

    /** Extracts the raw bytes from a {@code byte[]} or {@link ByteBuffer}; {@code null} otherwise. */
    private static byte[] toBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer bb = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            return bytes;
        }
        return null;
    }
}
