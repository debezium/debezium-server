/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.milvus;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.milvus.MilvusContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.debezium.server.Images;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq.CollectionSchema;
import io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

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

        schema.validateKey("test", keySchema);
    }

    @Test
    public void keySchemaNotStruct() {
        final var schema = new MilvusSchema(null);

        final var error = assertThrows(DebeziumException.class, () -> {
            schema.validateKey("test", Schema.INT64_SCHEMA);
        });
        assertTrue(error.getMessage().contains("Only structs are supported as the key"));
    }

    @Test
    public void keySchemaInvalidFieldType() throws Exception {
        final var schema = new MilvusSchema(null);

        final var keySchema = SchemaBuilder.struct()
                .field("pk", Schema.BOOLEAN_SCHEMA)
                .build();

        final var error = assertThrows(DebeziumException.class, () -> {
            schema.validateKey("test", keySchema);
        });
        assertTrue(error.getMessage().contains("Only STRING and INT64 type can be used as key"));
    }

    @Test
    public void correctValueSchema() {
        final var envelope = envelopeSchema(SchemaBuilder.struct()
                .field("key", Schema.INT64_SCHEMA)
                .field("col1", Schema.STRING_SCHEMA)
                .field("col2", FloatVector.builder().build())
                .build());
        createMockMilvus().validateValue("test", envelope);
    }

    @Test
    public void valueSchemaNotStruct() {
        final var error = assertThrows(DebeziumException.class, () -> {
            createMockMilvus().validateValue("test", Schema.INT64_SCHEMA);
        });
        assertTrue(error.getMessage().contains("Only structs are supported"));
    }

    @Test
    public void wrongColumName() {
        final var envelope = envelopeSchema(SchemaBuilder.struct()
                .field("key", Schema.INT64_SCHEMA)
                .field("com1", Schema.STRING_SCHEMA)
                .field("col2", FloatVector.builder().build())
                .build());

        final var error = assertThrows(DebeziumException.class, () -> {
            createMockMilvus().validateValue("test", envelope);
        });
        assertTrue(error.getMessage().contains("Schema field 'com1' does not match the collection field"));
    }

    @Test
    public void wrongColumType() {
        final var envelope = envelopeSchema(SchemaBuilder.struct()
                .field("key", Schema.INT64_SCHEMA)
                .field("col1", Schema.STRING_SCHEMA)
                .field("col2", DoubleVector.builder().build())
                .build());

        final var error = assertThrows(DebeziumException.class, () -> {
            createMockMilvus().validateValue("test", envelope);
        });
        assertTrue(error.getMessage().contains("Type for field 'col2' in collection 'test' does not match the mapped type"));
    }

    private MilvusSchema createMockMilvus() {
        final var milvusClient = Mockito.mock(MilvusClientV2.class);
        final var collectionDescription = Mockito.mock(DescribeCollectionResp.class);
        final var collectionSchema = Mockito.mock(CollectionSchema.class);
        final var schemaFields = List.of(
                FieldSchema.builder().name("key").dataType(DataType.Int64).build(),
                FieldSchema.builder().name("col1").dataType(DataType.VarChar).build(),
                FieldSchema.builder().name("col2").dataType(DataType.Float16Vector).build());
        Mockito.when(collectionSchema.getFieldSchemaList()).thenReturn(schemaFields);
        Mockito.when(collectionDescription.getCollectionSchema()).thenReturn(collectionSchema);
        Mockito.when(milvusClient.describeCollection(Mockito.any())).thenReturn(collectionDescription);
        final var schema = new MilvusSchema(milvusClient);
        return schema;
    }

    private Schema envelopeSchema(Schema valueSchema) {
        final var sourceSchema = SchemaBuilder.struct()
                .field("lsn", Schema.INT32_SCHEMA)
                .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_us", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
                .field("db", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        return Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(valueSchema)
                .withSource(sourceSchema)
                .build().schema();
    }

    @Test
    public void testMilvusContainerIsRunning() throws InterruptedException {
        var container = new MilvusContainer(DockerImageName.parse(Images.MILVUS_IMAGE).asCompatibleSubstituteFor("milvusdb/milvus"))
                .withStartupTimeout(Duration.ofSeconds(90));
        container.start();
        Thread.sleep(30000);
        container.stop();
    }

}
