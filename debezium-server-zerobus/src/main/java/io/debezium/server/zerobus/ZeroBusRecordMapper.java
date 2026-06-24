/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.Header;
import io.debezium.server.BaseChangeConsumer;

/**
 * Converts Debezium change events into the native request shape expected by the ZeroBus client boundary.
 */
public class ZeroBusRecordMapper extends BaseChangeConsumer {

    private final ZeroBusTableRouter tableRouter;
    private final ZeroBusSinkConfig config;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public ZeroBusRecordMapper(ZeroBusTableRouter tableRouter, ZeroBusSinkConfig config) {
        this.tableRouter = tableRouter;
        this.config = config;
    }

    public ZeroBusRecord map(ChangeEvent<Object, Object> record) {
        List<Header<Object>> eventHeaders = record.headers();
        Map<String, String> sourcePosition = sourcePosition(record);
        Map<String, String> headers = convertHeaders(record).entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (left, right) -> right,
                        LinkedHashMap::new));

        ZeroBusOperation operation = operation(record);
        return new ZeroBusRecord(
                tableRouter.route(record),
                record.destination(),
                record.partition(),
                record.key(),
                record.value(),
                sourcePosition,
                Map.copyOf(headers),
                operation,
                idempotencyKey(record, sourcePosition, eventHeaders));
    }

    private ZeroBusOperation operation(ChangeEvent<Object, Object> record) {
        if (record.value() == null) {
            return ZeroBusOperation.TOMBSTONE;
        }
        return ZeroBusOperation.fromDebeziumCode(debeziumOperationCode(record));
    }

    private String debeziumOperationCode(ChangeEvent<Object, Object> record) {
        String sourceRecordOperation = sourceRecordOperation(record);
        if (sourceRecordOperation != null) {
            return sourceRecordOperation;
        }
        return jsonOperation(record.value());
    }

    private String sourceRecordOperation(ChangeEvent<Object, Object> record) {
        SourceRecord sourceRecord = sourceRecord(record);
        if (sourceRecord == null || sourceRecord.value() == null || sourceRecord.valueSchema() == null) {
            return null;
        }
        if (Envelope.isEnvelopeSchema(sourceRecord.valueSchema()) && sourceRecord.value() instanceof Struct valueStruct) {
            return valueStruct.getString(Envelope.FieldName.OPERATION);
        }
        return null;
    }

    private Map<String, String> sourcePosition(ChangeEvent<Object, Object> record) {
        SourceRecord sourceRecord = sourceRecord(record);
        if (sourceRecord == null) {
            return Map.of();
        }

        Map<String, String> position = new LinkedHashMap<>();
        copyPosition("partition.", sourceRecord.sourcePartition(), position);
        copyPosition("offset.", sourceRecord.sourceOffset(), position);
        return Map.copyOf(position);
    }

    private SourceRecord sourceRecord(ChangeEvent<Object, Object> record) {
        if (!(record instanceof EmbeddedEngineChangeEvent<?, ?, ?> embeddedEvent)) {
            return null;
        }
        return embeddedEvent.sourceRecord();
    }

    private String jsonOperation(Object value) {
        String json = jsonString(value);
        if (json == null) {
            return null;
        }
        try {
            JsonNode root = jsonMapper.readTree(json);
            JsonNode op = root.get("op");
            if (op == null || op.isNull()) {
                op = root.path("payload").get("op");
            }
            return op == null || op.isNull() ? null : op.asText();
        }
        catch (IOException e) {
            return null;
        }
    }

    private String jsonString(Object value) {
        if (value instanceof String string) {
            return string;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return null;
    }

    private void copyPosition(String prefix, Map<String, ?> source, Map<String, String> target) {
        if (source == null || source.isEmpty()) {
            return;
        }
        source.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> target.put(prefix + entry.getKey(), asString(entry.getValue())));
    }

    private String idempotencyKey(ChangeEvent<Object, Object> record, Map<String, String> sourcePosition, List<Header<Object>> headers) {
        if (ZeroBusSinkConfig.IDEMPOTENCY_NONE.equals(config.getIdempotencyMode())) {
            return null;
        }
        if (!sourcePosition.isEmpty()) {
            String sourceFingerprint = sourcePosition.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.joining("&"));
            return asString(record.destination())
                    + "|partition=" + asString(record.partition())
                    + "|key=" + asString(record.key())
                    + "|source_position=" + sourceFingerprint;
        }
        String headerFingerprint = headers.stream()
                .sorted(Comparator.comparing(Header::getKey))
                .map(header -> header.getKey() + "=" + asString(header.getValue()))
                .collect(Collectors.joining("&"));
        return asString(record.destination())
                + "|partition=" + asString(record.partition())
                + "|key=" + asString(record.key())
                + "|headers=" + headerFingerprint;
    }
}
