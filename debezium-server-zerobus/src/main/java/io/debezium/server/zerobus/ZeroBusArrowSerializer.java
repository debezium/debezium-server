/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;

/**
 * Serializes mapped Debezium change events into the Arrow envelope sent to ZeroBus.
 */
public class ZeroBusArrowSerializer implements AutoCloseable {

    private static final String TARGET_TABLE = "target_table";
    private static final String DESTINATION = "destination";
    private static final String PARTITION = "partition";
    private static final String OPERATION = "operation";
    private static final String IDEMPOTENCY_KEY = "idempotency_key";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String SOURCE_POSITION = "source_position";
    private static final String HEADERS = "headers";

    private static final Schema SCHEMA = new Schema(List.of(
            stringField(TARGET_TABLE),
            stringField(DESTINATION),
            new Field(PARTITION, FieldType.nullable(new ArrowType.Int(32, true)), null),
            stringField(OPERATION),
            stringField(IDEMPOTENCY_KEY),
            stringField(KEY),
            stringField(VALUE),
            stringField(SOURCE_POSITION),
            stringField(HEADERS)));

    private final ObjectMapper mapper = new ObjectMapper();
    private final BufferAllocator allocator;
    private final boolean closeAllocator;

    public ZeroBusArrowSerializer() {
        this(new RootAllocator(), true);
    }

    ZeroBusArrowSerializer(BufferAllocator allocator) {
        this(allocator, false);
    }

    private ZeroBusArrowSerializer(BufferAllocator allocator, boolean closeAllocator) {
        this.allocator = allocator;
        this.closeAllocator = closeAllocator;
    }

    public Schema schema() {
        return SCHEMA;
    }

    public VectorSchemaRoot serialize(List<ZeroBusRecord> records) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        boolean success = false;
        try {
            root.allocateNew();

            VarCharVector targetTable = (VarCharVector) root.getVector(TARGET_TABLE);
            VarCharVector destination = (VarCharVector) root.getVector(DESTINATION);
            IntVector partition = (IntVector) root.getVector(PARTITION);
            VarCharVector operation = (VarCharVector) root.getVector(OPERATION);
            VarCharVector idempotencyKey = (VarCharVector) root.getVector(IDEMPOTENCY_KEY);
            VarCharVector key = (VarCharVector) root.getVector(KEY);
            VarCharVector value = (VarCharVector) root.getVector(VALUE);
            VarCharVector sourcePosition = (VarCharVector) root.getVector(SOURCE_POSITION);
            VarCharVector headers = (VarCharVector) root.getVector(HEADERS);

            for (int row = 0; row < records.size(); row++) {
                ZeroBusRecord record = records.get(row);
                set(targetTable, row, record.targetTable());
                set(destination, row, record.destination());
                if (record.partition() == null) {
                    partition.setNull(row);
                }
                else {
                    partition.setSafe(row, record.partition());
                }
                set(operation, row, record.operation().name().toLowerCase(Locale.ROOT));
                set(idempotencyKey, row, record.idempotencyKey());
                set(key, row, valueAsText(record.key()));
                set(value, row, valueAsText(record.value()));
                set(sourcePosition, row, mapAsJson(record.sourcePosition()));
                set(headers, row, mapAsJson(record.headers()));
            }

            root.setRowCount(records.size());
            success = true;
            return root;
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to serialize ZeroBus Arrow record", e);
        }
        finally {
            if (!success) {
                root.close();
            }
        }
    }

    private static Field stringField(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
    }

    private static void set(VarCharVector vector, int row, String value) {
        if (value == null) {
            vector.setNull(row);
        }
        else {
            vector.setSafe(row, new Text(value));
        }
    }

    private String valueAsText(Object value) throws IOException {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return stringAsCanonicalJsonOrText(new String(bytes, StandardCharsets.UTF_8));
        }
        if (value instanceof String string) {
            return stringAsCanonicalJsonOrText(string);
        }
        return mapper.writeValueAsString(value);
    }

    private String mapAsJson(Map<String, String> value) throws IOException {
        if (value == null || value.isEmpty()) {
            return null;
        }
        return mapper.writeValueAsString(value);
    }

    private String stringAsCanonicalJsonOrText(String value) throws IOException {
        try {
            JsonNode json = mapper.readTree(value);
            return mapper.writeValueAsString(json);
        }
        catch (IOException ignored) {
            return value;
        }
    }

    @Override
    public void close() {
        if (closeAllocator) {
            allocator.close();
        }
    }
}
