/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.data.Envelope;
import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;
import io.debezium.server.BaseChangeConsumer;

/** Maps a Debezium event to the stable envelope contract written by the Zerobus sink. */
final class ZerobusEnvelopeMapper extends BaseChangeConsumer {

    private final ZerobusChangeConsumerConfig config;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    ZerobusEnvelopeMapper(ZerobusChangeConsumerConfig config) {
        this.config = config;
    }

    ZerobusEnvelope map(BatchEvent record, String targetTable) {
        Map<String, String> sourcePosition = sourcePosition(record);
        Object key = serializedValue(record.key());
        Object value = serializedValue(record.value());
        Map<String, String> headers = sortedHeaders(record);
        ZerobusOperation operation = operation(record);

        return new ZerobusEnvelope(
                targetTable,
                record.destination(),
                record.partition(),
                key,
                value,
                sourcePosition,
                headers,
                operation,
                idempotencyKey(record, key, operation, sourcePosition, record.headers()),
                sourceTimestampMillis(record, sourcePosition));
    }

    ZerobusOperation operation(BatchEvent record) {
        if (record.value() == null) {
            return ZerobusOperation.TOMBSTONE;
        }
        return ZerobusOperation.fromDebeziumCode(debeziumOperationCode(record));
    }

    private String debeziumOperationCode(BatchEvent record) {
        SourceRecord sourceRecord = record.record();
        if (sourceRecord != null && sourceRecord.value() instanceof Struct valueStruct
                && sourceRecord.valueSchema() != null && Envelope.isEnvelopeSchema(sourceRecord.valueSchema())) {
            return valueStruct.getString(Envelope.FieldName.OPERATION);
        }
        return jsonOperation(record.value());
    }

    private Map<String, String> sourcePosition(BatchEvent record) {
        SourceRecord sourceRecord = record.record();
        if (sourceRecord == null) {
            return Map.of();
        }

        Map<String, String> position = new LinkedHashMap<>();
        copyPosition("partition.", sourceRecord.sourcePartition(), position);
        copyPosition("offset.", sourceRecord.sourceOffset(), position);
        return java.util.Collections.unmodifiableMap(position);
    }

    private Map<String, String> sortedHeaders(BatchEvent record) {
        Map<String, String> converted = convertHeaders(record);
        Map<String, String> headers = new LinkedHashMap<>();
        converted.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
        return java.util.Collections.unmodifiableMap(headers);
    }

    private String jsonOperation(Object value) {
        String json = serializedValue(value);
        if (json == null) {
            return null;
        }
        try {
            JsonNode root = jsonMapper.readTree(json);
            JsonNode operation = root.get("op");
            if (operation == null || operation.isNull()) {
                operation = root.path("payload").get("op");
            }
            return operation == null || operation.isNull() ? null : operation.asText();
        }
        catch (IOException e) {
            return null;
        }
    }

    private void copyPosition(String prefix, Map<String, ?> source, Map<String, String> target) {
        if (source == null || source.isEmpty()) {
            return;
        }
        source.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> target.put(prefix + entry.getKey(), String.valueOf(entry.getValue())));
    }

    private String idempotencyKey(BatchEvent record, Object key, ZerobusOperation operation,
                                  Map<String, String> sourcePosition, List<Header<Object>> headers) {
        if (ZerobusChangeConsumerConfig.IDEMPOTENCY_NONE.equals(config.getIdempotencyMode())) {
            return null;
        }

        String identity;
        if (!sourcePosition.isEmpty()) {
            identity = sourcePosition.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> encode(entry.getKey()) + "=" + encode(entry.getValue()))
                    .collect(java.util.stream.Collectors.joining("&"));
            identity = "source_position=" + identity;
        }
        else {
            List<Header<Object>> safeHeaders = headers == null ? List.of() : new ArrayList<>(headers);
            identity = "headers=" + safeHeaders.stream()
                    .sorted(Comparator.comparing(Header::getKey))
                    .map(header -> encode(header.getKey()) + "=" + encode(String.valueOf(header.getValue())))
                    .collect(java.util.stream.Collectors.joining("&"));
        }

        return "destination=" + encode(String.valueOf(record.destination()))
                + "|partition=" + encode(String.valueOf(record.partition()))
                + "|key=" + encode(canonicalValue(key))
                + "|operation=" + encode(operation.name().toLowerCase(java.util.Locale.ROOT))
                + "|" + identity;
    }

    private String canonicalValue(Object value) {
        if (value == null) {
            return "null";
        }
        String text = String.valueOf(value);
        try {
            return jsonMapper.writeValueAsString(jsonMapper.readTree(text));
        }
        catch (IOException e) {
            return text;
        }
    }

    private String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private String serializedValue(Object value) {
        return value == null ? null : getString(value);
    }

    private Long sourceTimestampMillis(BatchEvent record, Map<String, String> sourcePosition) {
        SourceRecord sourceRecord = record.record();
        if (sourceRecord != null && sourceRecord.value() instanceof Struct envelope
                && envelope.schema().field(Envelope.FieldName.SOURCE) != null) {
            Object source = envelope.get(Envelope.FieldName.SOURCE);
            if (source instanceof Struct sourceStruct && sourceStruct.schema().field("ts_ms") != null) {
                Object timestamp = sourceStruct.get("ts_ms");
                if (timestamp instanceof Number number) {
                    return number.longValue();
                }
            }
        }

        Long jsonTimestamp = jsonSourceTimestamp(record.value());
        if (jsonTimestamp != null) {
            return jsonTimestamp;
        }
        Long millis = parseLong(sourcePosition.get("offset.ts_ms"));
        if (millis != null) {
            return millis;
        }
        Long micros = parseLong(sourcePosition.get("offset.ts_usec"));
        return micros == null ? null : micros / 1_000L;
    }

    private Long jsonSourceTimestamp(Object value) {
        String json = serializedValue(value);
        if (json == null) {
            return null;
        }
        try {
            JsonNode root = jsonMapper.readTree(json);
            JsonNode source = root.path("source");
            if (source.isMissingNode()) {
                source = root.path("payload").path("source");
            }
            JsonNode timestamp = source.path("ts_ms");
            return timestamp.isNumber() ? timestamp.longValue() : null;
        }
        catch (IOException e) {
            return null;
        }
    }

    private Long parseLong(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        }
        catch (NumberFormatException e) {
            return null;
        }
    }
}
