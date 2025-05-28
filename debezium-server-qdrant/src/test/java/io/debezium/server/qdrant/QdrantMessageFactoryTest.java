/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.qdrant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.data.Uuid;
import io.debezium.data.vector.FloatVector;

/**
 * Tests for {@link QdrantMessageFactory}.
 *
 * @author Jiri Pechanec
 */
public class QdrantMessageFactoryTest {

    private QdrantMessageFactory defaultMessageFactory() {
        return new QdrantMessageFactory(Optional.empty(), Collections.emptyMap());
    }

    private QdrantMessageFactory vectorFieldMessageFactory(String config) {
        return new QdrantMessageFactory(Optional.of(config), Collections.emptyMap());
    }

    private QdrantMessageFactory includeFieldMessageFactory(String config) {
        return new QdrantMessageFactory(Optional.empty(), Map.of("testcol", config, "othercol", "metadat1,meatadata2"));
    }

    @Test
    public void keySchemaInt64() throws Exception {
        final var messageFactory = defaultMessageFactory();

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Schema.INT64_SCHEMA)
                .build();

        messageFactory.validateKey("test", keySchema);
    }

    @Test
    public void keySchemaUuid() throws Exception {
        final var messageFactory = defaultMessageFactory();

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Uuid.builder().build())
                .build();

        messageFactory.validateKey("test", keySchema);
    }

    @Test
    public void keySchemaNotStruct() {
        final var messageFactory = defaultMessageFactory();

        final var error = assertThrows(DebeziumException.class, () -> {
            messageFactory.validateKey("test", Schema.INT64_SCHEMA);
        });
        assertTrue(error.getMessage().contains("Only structs are supported as the key"));
    }

    @Test
    public void keySchemaInvalidFieldType() throws Exception {
        final var messageFactory = defaultMessageFactory();

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Schema.STRING_SCHEMA)
                .build();

        final var error = assertThrows(DebeziumException.class, () -> {
            messageFactory.validateKey("test", keySchema);
        });
        assertTrue(error.getMessage().contains("Only UUID and INT64 type can be used as key but got '(STRING)(null)' for collection 'test'"));
    }

    @Test
    public void implicitMapping() throws Exception {
        final var messageFactory = defaultMessageFactory();

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Schema.INT64_SCHEMA)
                .build();

        final var valueSchema = SchemaBuilder.struct()
                .field("pk", Schema.INT64_SCHEMA)
                .field("metadata1", Schema.STRING_SCHEMA)
                .field("metadata2", Schema.INT32_SCHEMA)
                .field("vector", FloatVector.schema())
                .build();

        final var key = new Struct(keySchema)
                .put("pk", 13L);

        final var value = new Struct(valueSchema)
                .put("pk", 13L)
                .put("metadata1", "mymeta")
                .put("metadata2", 13)
                .put("vector", FloatVector.fromLogical(FloatVector.schema(), new float[]{ 1.0f, 2.0f, 3.0f }));

        final var pointId = messageFactory.toPointId(key);
        assertEquals(13L, pointId.getNum());

        final var vectors = messageFactory.toVectors("testcol", value);
        assertEquals(List.of(1.0f, 2.0f, 3.0f), vectors.getVector().getDataList());

        final var payload = messageFactory.toPayloadMap("testcol", key, value);
        assertEquals(2, payload.size());
        assertEquals("mymeta", payload.get("metadata1").getStringValue());
        assertEquals(13L, payload.get("metadata2").getIntegerValue());
    }

    @Test
    public void implicitMappingTwoVectors() throws Exception {
        final var messageFactory = defaultMessageFactory();

        final var valueSchema = SchemaBuilder.struct()
                .field("pk", Schema.INT64_SCHEMA)
                .field("metadata1", Schema.STRING_SCHEMA)
                .field("metadata2", Schema.INT32_SCHEMA)
                .field("vector1", FloatVector.schema())
                .field("vector2", FloatVector.schema())
                .build();

        final var value = new Struct(valueSchema)
                .put("pk", 13L)
                .put("metadata1", "mymeta")
                .put("metadata2", 13)
                .put("vector1", FloatVector.fromLogical(FloatVector.schema(), new float[]{ 1.0f, 2.0f, 3.0f }))
                .put("vector2", FloatVector.fromLogical(FloatVector.schema(), new float[]{ 10.0f, 20.0f, 30.0f }));

        final var error = assertThrows(DebeziumException.class, () -> {
            final var vectors = messageFactory.toVectors("testcol", value);
        });
        assertTrue(error.getMessage().contains("Multiple fields with logical type 'io.debezium.data.FloatVector' found in collection 'testcol'"));
    }

    @Test
    public void invalidVectorFieldConfiguration() throws Exception {
        var error = assertThrows(DebeziumException.class, () -> {
            final var messageFactory = vectorFieldMessageFactory("");
        });
        assertTrue(error.getMessage().contains("Vector field names cannot be empty"));

        error = assertThrows(DebeziumException.class, () -> {
            final var messageFactory = vectorFieldMessageFactory("x");
        });
        assertTrue(error.getMessage().contains("Invalid vector field format: 'x'"));

        error = assertThrows(DebeziumException.class, () -> {
            final var messageFactory = vectorFieldMessageFactory(":x");
        });
        assertTrue(error.getMessage().contains("Invalid vector field format: ':x'"));

        error = assertThrows(DebeziumException.class, () -> {
            final var messageFactory = vectorFieldMessageFactory("x: ,y:z");
        });
        assertTrue(error.getMessage().contains("Invalid vector field format: 'x: '"));

        error = assertThrows(DebeziumException.class, () -> {
            final var messageFactory = vectorFieldMessageFactory("x:y, : ,y:z");
        });
        assertTrue(error.getMessage().contains("Invalid vector field format: ' : '"));

        error = assertThrows(DebeziumException.class, () -> {
            final var messageFactory = vectorFieldMessageFactory("x:y,x:z");
        });
        assertTrue(error.getMessage().contains("Multiple vector fields requested for collection 'x': 'y' and 'z'"));
    }

    @Test
    public void explicitMappingTwoVectors() throws Exception {
        final var messageFactory = vectorFieldMessageFactory("ignore:vector1,testcol:vector2");

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Schema.INT64_SCHEMA)
                .build();

        final var valueSchema = SchemaBuilder.struct()
                .field("pk", Schema.INT64_SCHEMA)
                .field("metadata1", Schema.STRING_SCHEMA)
                .field("metadata2", Schema.INT32_SCHEMA)
                .field("vector1", FloatVector.schema())
                .field("vector2", FloatVector.schema())
                .build();

        final var key = new Struct(keySchema)
                .put("pk", 13L);

        final var value = new Struct(valueSchema)
                .put("pk", 13L)
                .put("metadata1", "mymeta")
                .put("metadata2", 13)
                .put("vector1", FloatVector.fromLogical(FloatVector.schema(), new float[]{ 1.0f, 2.0f, 3.0f }))
                .put("vector2", FloatVector.fromLogical(FloatVector.schema(), new float[]{ 10.0f, 20.0f, 30.0f }));

        final var vectors = messageFactory.toVectors("testcol", value);
        assertEquals(List.of(10.0f, 20.0f, 30.0f), vectors.getVector().getDataList());

        final var payload = messageFactory.toPayloadMap("testcol", key, value);
        assertEquals(2, payload.size());
        assertEquals("mymeta", payload.get("metadata1").getStringValue());
        assertEquals(13L, payload.get("metadata2").getIntegerValue());
    }

    @Test
    public void explicitMappingMappingForFields() throws Exception {
        final var messageFactory = includeFieldMessageFactory("metadata2");

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Schema.INT64_SCHEMA)
                .build();

        final var valueSchema = SchemaBuilder.struct()
                .field("pk", Schema.INT64_SCHEMA)
                .field("metadata1", Schema.STRING_SCHEMA)
                .field("metadata2", Schema.INT32_SCHEMA)
                .field("vector", FloatVector.schema())
                .build();

        final var key = new Struct(keySchema)
                .put("pk", 13L);

        final var value = new Struct(valueSchema)
                .put("pk", 13L)
                .put("metadata1", "mymeta")
                .put("metadata2", 13)
                .put("vector", FloatVector.fromLogical(FloatVector.schema(), new float[]{ 1.0f, 2.0f, 3.0f }));

        final var pointId = messageFactory.toPointId(key);
        assertEquals(13L, pointId.getNum());

        final var vectors = messageFactory.toVectors("testcol", value);
        assertEquals(List.of(1.0f, 2.0f, 3.0f), vectors.getVector().getDataList());

        final var payload = messageFactory.toPayloadMap("testcol", key, value);
        assertEquals(1, payload.size());
        assertEquals(13L, payload.get("metadata2").getIntegerValue());
    }

    @Test
    public void invalidIncludeFieldsConfiguration() throws Exception {
        var error = assertThrows(DebeziumException.class, () -> {
            final var messageFactory = includeFieldMessageFactory("");
        });
        assertTrue(error.getMessage().contains("Field names for collection 'testcol' cannot be empty"));

        error = assertThrows(DebeziumException.class, () -> {
            final var messageFactory = includeFieldMessageFactory(", ,");
        });
        assertTrue(error.getMessage().contains("Field names for collection 'testcol' cannot be empty"));

        error = assertThrows(DebeziumException.class, () -> {
            final var messageFactory = includeFieldMessageFactory(",,");
        });
        assertTrue(error.getMessage().contains("Field names for collection 'testcol' cannot be empty"));
    }
}
