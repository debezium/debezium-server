/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.qdrant;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.data.Uuid;

public class QdrantMessageFactoryTest {

    @Test
    public void keySchemaInt64() throws Exception {
        final var schema = new QdrantMessageFactory();

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Schema.INT64_SCHEMA)
                .build();

        schema.validateKey("test", keySchema);
    }

    @Test
    public void keySchemaUuid() throws Exception {
        final var schema = new QdrantMessageFactory();

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Uuid.builder().build())
                .build();

        schema.validateKey("test", keySchema);
    }

    @Test
    public void keySchemaNotStruct() {
        final var schema = new QdrantMessageFactory();

        final var error = assertThrows(DebeziumException.class, () -> {
            schema.validateKey("test", Schema.INT64_SCHEMA);
        });
        assertTrue(error.getMessage().contains("Only structs are supported as the key"));
    }

    @Test
    public void keySchemaInvalidFieldType() throws Exception {
        final var schema = new QdrantMessageFactory();

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Schema.STRING_SCHEMA)
                .build();

        final var error = assertThrows(DebeziumException.class, () -> {
            schema.validateKey("test", keySchema);
        });
        assertTrue(error.getMessage().contains("Only UUID and INT64 type can be used as key but got '(STRING)(null)' for collection 'test'"));
    }
}
