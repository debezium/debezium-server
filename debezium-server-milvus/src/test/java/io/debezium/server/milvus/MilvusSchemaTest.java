/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.milvus;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
public class MilvusSchemaTest {

    @Test
    public void correctKeySchema() throws Exception {
        final var schema = new MilvusSchema(null);

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Schema.INT64_SCHEMA)
                .build();

        schema.validateKey(keySchema);
    }

    @Test
    public void keySchemaNotStruct() {
        final var schema = new MilvusSchema(null);

        assertThrows(DebeziumException.class, () -> {
            schema.validateKey(Schema.INT64_SCHEMA);
        });
    }

    @Test
    public void keySchemaInvalidFieldType() throws Exception {
        final var schema = new MilvusSchema(null);

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Schema.BOOLEAN_SCHEMA)
                .build();

        assertThrows(DebeziumException.class, () -> {
            schema.validateKey(keySchema);
        });
    }

}
