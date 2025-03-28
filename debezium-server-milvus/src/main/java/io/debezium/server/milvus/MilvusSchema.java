/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.milvus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.debezium.util.BoundedConcurrentHashMap;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;

/**
 * Manages collections in Milvus database. Maps Kafka Connect schema into Milvus schema and validates
 * contraints.
 *
 * @author Jiri Pechanec
 */
public class MilvusSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusChangeConsumer.class);

    private final MilvusClientV2 milvusClient;
    private final Map<String, List<MilvusField>> collections = new BoundedConcurrentHashMap<>(1_024);

    public MilvusSchema(MilvusClientV2 milvusClient) {
        this.milvusClient = milvusClient;
    }

    private List<MilvusField> getCollection(String collectionName) {
        return collections.get(collectionName);
    }

    private List<MilvusField> addCollection(String collectionName, Schema schema) {
        final var fields = new ArrayList<MilvusField>();

        final var request = DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build();
        final var collectionDescription = milvusClient.describeCollection(request);
        final var collectionSchema = collectionDescription.getCollectionSchema();
        for (var field : collectionSchema.getFieldSchemaList()) {
            fields.add(new MilvusField(field.getName(), field.getIsPrimaryKey(), field.getDataType(), schema));
        }
        LOGGER.info("Adding collection '{}' with fields {} to known schemas", collectionName, fields);
        collections.put(collectionName, fields);
        return fields;
    }

    public void validateValue(String collectionName, Schema schema) {
        if (schema == null) {
            // Tombstone message
            return;
        }

        if (schema.type() != Schema.Type.STRUCT) {
            throw new DebeziumException(String.format("Only structs are supported as the value for collection '%s' but got '%s'", collectionName, schema.type()));
        }

        if (Envelope.isEnvelopeSchema(schema)) {
            // Message is envelope, so only after part is used
            schema = schema.field(Envelope.FieldName.AFTER).schema();
        }

        if (schema == null) {
            // Delete message
            return;
        }

        var fields = getCollection(collectionName);
        if (fields == null) {
            fields = addCollection(collectionName, schema);
        }

        if (fields.size() != schema.fields().size()) {
            throw new DebeziumException(String.format("Schema field count %d does not match the collection field count %d in collection '%s'",
                    schema.fields().size(), fields.size(), collectionName));
        }

        for (int i = 0; i < schema.fields().size(); i++) {
            final var schemaField = schema.fields().get(i);
            final var milvusField = fields.get(i);
            if (!milvusField.name.equals(schemaField.name())) {
                throw new DebeziumException(
                        String.format("Schema field '%s' does not match the collection field '%s' in collection '%s'",
                                schemaField.name(), milvusField.name, collectionName));
            }
            if (milvusField.dataType != connectTypeToMilvusType(schemaField.schema())) {
                throw new DebeziumException(String.format(
                        "Type for field '%s' in collection '%s' does not match the mapped type %s != %s (%s)",
                        milvusField.name, collectionName, milvusField.dataType,
                        connectTypeToMilvusType(schemaField.schema()), schemaField.schema()));
            }
        }
    }

    private DataType connectTypeToMilvusType(Schema schema) {
        if (schema.name() != null) {
            switch (schema.name()) {
                case Json.LOGICAL_NAME:
                    return DataType.JSON;
                case DoubleVector.LOGICAL_NAME:
                    return DataType.FloatVector;
                case FloatVector.LOGICAL_NAME:
                    return DataType.Float16Vector;
            }
        }

        switch (schema.type()) {
            case BOOLEAN:
                return DataType.Bool;
            case INT8:
                return DataType.Int8;
            case INT16:
                return DataType.Int16;
            case INT32:
                return DataType.Int32;
            case INT64:
                return DataType.Int64;
            case FLOAT32:
                return DataType.Float;
            case FLOAT64:
                return DataType.Double;
            case STRING:
                return DataType.VarChar;
            case ARRAY:
                // TODO
            default:
                throw new DebeziumException("Unsupported type " + schema.name() + "(" + schema.type() + ")");
        }
        // Datatypes not yet covered
        // BINARY_VECTOR - no source type available yet
        // BFLOAT16_VECTOR - could be mapped from FloatVector
        // SPARSE_FLOAT_VECTOR - currently only SparseDoubleVector is available, precision can be reduced
    }

    public void validateKey(String collectionName, Schema schema) {
        if (schema.type() != Schema.Type.STRUCT) {
            throw new DebeziumException(
                    String.format("Only structs are supported as the key for collection '%s' but got '%s'",
                            collectionName, schema.type()));
        }

        if (schema.fields().size() != 1) {
            throw new DebeziumException(
                    String.format("Key for collection '%s' must have exactly one field", collectionName));
        }

        final var keyField = schema.fields().get(0);
        if (keyField.schema().type() != Schema.STRING_SCHEMA.type()
                && keyField.schema().type() != Schema.INT64_SCHEMA.type()) {
            throw new DebeziumException(
                    String.format("Only STRING and INT64 type can be used as key but got '%s' for collection '%s'",
                            keyField.schema().type(), collectionName));
        }
    }

    private record MilvusField(String name, boolean isPrimaryKey, DataType dataType, Schema schema) {
    }
}
