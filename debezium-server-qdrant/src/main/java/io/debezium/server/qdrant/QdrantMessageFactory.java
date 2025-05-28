/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.qdrant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.data.Uuid;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.qdrant.client.PointIdFactory;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.VectorsFactory;
import io.qdrant.client.grpc.JsonWithInt.Value;
import io.qdrant.client.grpc.Points.PointId;
import io.qdrant.client.grpc.Points.Vectors;

/**
 * Validates change messages coming from source system. Creates record for Qdrant points.
 *
 * @author Jiri Pechanec
 */
public class QdrantMessageFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(QdrantMessageFactory.class);

    private final Map<String, String> requestedVectorFieldNames = new HashMap<>();
    private final Map<String, Set<String>> fieldNamesPerCollection = new HashMap<>();

    public QdrantMessageFactory(Optional<String> vectorFieldNamesStr, Map<String, Object> fieldNamesPerCollectionStr) {
        if (vectorFieldNamesStr.isPresent()) {
            initializeRequestedVectorFieldNames(vectorFieldNamesStr.get().trim());
        }
        else {
            LOGGER.info("No vector field names configured, only implicit mapping will be used");
        }
        if (fieldNamesPerCollectionStr != null && !fieldNamesPerCollectionStr.isEmpty()) {
            initializeFieldNamesPerCollection(fieldNamesPerCollectionStr);
        }
        else {
            LOGGER.info("No field names per collection configured, all fields will be included");
        }
    }

    private void initializeFieldNamesPerCollection(Map<String, Object> fieldNamesPerCollectionStr) {
        fieldNamesPerCollectionStr.forEach((collectionName, fieldNamesValue) -> {
            final var fieldNamesStr = ((String) fieldNamesValue).trim();
            if (fieldNamesStr.isEmpty()) {
                throw new DebeziumException("Field names for collection '%s' cannot be empty".formatted(collectionName));
            }
            final Set<String> fieldNames = Set.of(fieldNamesStr.split(",")).stream()
                    .map(String::trim)
                    .filter(name -> !name.isEmpty())
                    .collect(Collectors.toSet());
            if (fieldNames.isEmpty()) {
                throw new DebeziumException("Field names for collection '%s' cannot be empty".formatted(collectionName));
            }
            fieldNamesPerCollection.put(collectionName, fieldNames);
            LOGGER.debug("Field names for collection '{}': {}", collectionName, fieldNames);
        });
    }

    private void initializeRequestedVectorFieldNames(String vectorFieldNamesStr) {
        LOGGER.debug("Requested vector field names: '{}'", vectorFieldNamesStr);
        if (vectorFieldNamesStr.isEmpty()) {
            throw new DebeziumException("Vector field names cannot be empty");
        }
        final var vectorFields = vectorFieldNamesStr.split(",");
        for (var field : vectorFields) {
            final var parts = field.trim().split(":");
            if (parts.length != 2) {
                throw new DebeziumException("Invalid vector field format: '%s'".formatted(field));
            }
            final var collection = parts[0].trim();
            final var fieldName = parts[1].trim();
            if (collection.isEmpty() || fieldName.isEmpty()) {
                throw new DebeziumException("Invalid vector field format: '%s'".formatted(field));
            }
            LOGGER.debug("Requested vector field for collection '{}': '{}'", collection, fieldName);

            if (requestedVectorFieldNames.containsKey(collection)) {
                throw new DebeziumException(
                        "Multiple vector fields requested for collection '%s': '%s' and '%s'".formatted(
                                collection, requestedVectorFieldNames.get(collection), fieldName));
            }
            requestedVectorFieldNames.put(collection, fieldName);
        }
    }

    /**
     * Validates the key schema for a collection.
     * The key must be a Struct with exactly one field, which can be either INT64 or UUID.
     * This is a limitation of Qdrant.
     *
     * @param collectionName the name of the collection
     * @param schema the schema of the key
     */
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
        if (keyField.schema().type() == Schema.INT64_SCHEMA.type()) {
            return;
        }
        if (isUuid(keyField)) {
            return;
        }
        throw new DebeziumException(
                String.format("Only UUID and INT64 type can be used as key but got '(%s)(%s)' for collection '%s'",
                        keyField.schema().type(), keyField.schema().name(), collectionName));
    }

    /**
     * Converts a key and value from a Struct to a Map of Qdrant Values for the payload.
     * Key and vector fields are not included in the payload.
     * If field names are configured for the collection, only those fields are included.
     *
     * @param collectionName the name of the collection
     * @param key the key Struct
     * @param value the value Struct
     * @return a Map of field names to Qdrant Values
     */
    public Map<String, Value> toPayloadMap(String collectionName, Struct key, Struct value) {
        final Map<String, Value> values = new HashMap<>();

        final var keyFieldName = key.schema().fields().get(0).name();

        final Set<String> includeList = fieldNamesPerCollection.get(collectionName);

        final List<String> fieldList = new ArrayList<>();
        for (var field : value.schema().fields()) {
            String fieldName = field.name();
            // Key fields are not added to payload
            if (fieldName.equals(keyFieldName)) {
                continue;
            }
            // Vector fields are not added to payload
            if (isVector(field.schema())) {
                continue;
            }
            if (includeList == null || includeList.contains(fieldName)) {
                fieldList.add(fieldName);
            }
        }

        for (var fieldName : fieldList) {
            values.put(fieldName, fieldToValue(fieldName, collectionName, value));
        }
        return values;
    }

    /**
     * Converts a field in a Struct to a Value for Qdrant based on the schema type.
     *
     * @param fieldName the name of the field
     * @param collectionName the name of the collection
     * @param struct the Struct containing the field
     * @return the Qdrant value
     */
    private Value fieldToValue(String fieldName, String collectionName, Struct struct) {
        final var schema = struct.schema().field(fieldName).schema();

        if (struct.get(fieldName) == null) {
            return ValueFactory.nullValue();
        }

        switch (schema.type()) {
            case INT8:
                return ValueFactory.value(struct.getInt8(fieldName));
            case INT16:
                return ValueFactory.value(struct.getInt16(fieldName));
            case INT32:
                return ValueFactory.value(struct.getInt32(fieldName));
            case INT64:
                return ValueFactory.value(struct.getInt64(fieldName));
            case FLOAT32:
                return ValueFactory.value(struct.getFloat32(fieldName));
            case FLOAT64:
                return ValueFactory.value(struct.getFloat64(fieldName));
            case BOOLEAN:
                return ValueFactory.value(struct.getBoolean(fieldName));
            case STRING:
                return ValueFactory.value(struct.getString(fieldName));
            default:
                throw new DebeziumException("Unsupported type %s for field '%s' in collection '%s'".formatted(schema.type(),
                        fieldName, collectionName));
        }
    }

    /**
     * Retrieves the vector field from a Struct based on the collection name.
     * If a specific vector field was configured for the collection, it is returned.
     * Otherwise, if there is only one field with logical type FloatVector or DoubleVector then is returned.
     * If not an error is raised.
     *
     * @param collectionName the name of the collection
     * @param struct the Struct containing the fields
     * @return the vector Field
     */
    private Field getVectorField(String collectionName, Struct struct) {
        Field vectorField = null;

        // Check if a specific vector field was configured for this collection
        String requestedFieldName = requestedVectorFieldNames.get(collectionName);

        if (requestedFieldName != null) {
            // Look for the requested field in the schema
            vectorField = struct.schema().field(requestedFieldName);
            if (vectorField == null) {
                throw new DebeziumException("Requested vector field '%s' not found in collection '%s'"
                        .formatted(requestedFieldName, collectionName));
            }

            // Verify the field has the correct logical type
            Schema fieldSchema = vectorField.schema();
            if (!isVector(fieldSchema)) {
                throw new DebeziumException("Field '%s' in collection '%s' is not of logical type '%s'"
                        .formatted(requestedFieldName, collectionName, FloatVector.LOGICAL_NAME));
            }

            return vectorField;
        }

        // No specific field requested, find any field with FloatVector logical type
        for (Field field : struct.schema().fields()) {
            Schema fieldSchema = field.schema();
            if (isVector(fieldSchema)) {
                if (vectorField != null) {
                    throw new DebeziumException("Multiple fields with logical type '%s' found in collection '%s'"
                            .formatted(FloatVector.LOGICAL_NAME, collectionName));
                }
                vectorField = field;
            }
        }

        if (vectorField == null) {
            throw new DebeziumException("No field with logical type '%s' found in collection '%s'"
                    .formatted(FloatVector.LOGICAL_NAME, collectionName));
        }

        return vectorField;
    }

    /**
     * Converts a key Struct to a PointId for Qdrant.
     * The key must have exactly one field, which can be either INT64 or UUID.
     * This is limitation of Qdrant.
     *
     * @param key the key Struct
     * @return the PointId
     */
    public PointId toPointId(Struct key) {
        final var keyField = key.schema().fields().get(0);

        return keyField.schema().type() == Schema.INT64_SCHEMA.type()
                ? PointIdFactory.id(key.getInt64(keyField.name()))
                : PointIdFactory.id(UUID.fromString(key.getString(keyField.name())));
    }

    /**
     * Converts a Struct to Vectors for Qdrant.
     * The vector field must be either FloatVector or DoubleVector.
     * If the vector field is FloatVector, it is returned as is.
     * If it is DoubleVector, it is converted to FloatVector with loss of precision.
     *
     * @param collectionName the name of the collection
     * @param struct the Struct containing the vector field
     * @return the Vectors
     */
    public Vectors toVectors(String collectionName, Struct struct) {
        final var vectorField = getVectorField(collectionName, struct);
        if (isFloatVector(vectorField.schema())) {
            return VectorsFactory.vectors(struct.getArray(vectorField.name()));
        }
        final List<Double> doubleVector = struct.getArray(vectorField.name());
        final List<Float> floatVector = new ArrayList<>();
        for (double d : doubleVector) {
            floatVector.add((float) d);
        }
        return VectorsFactory.vectors(floatVector);
    }

    private boolean isFloatVector(Schema fieldSchema) {
        return fieldSchema.type() == Type.ARRAY && FloatVector.LOGICAL_NAME.equals(fieldSchema.name());
    }

    private boolean isDoubleVector(Schema fieldSchema) {
        return fieldSchema.type() == Type.ARRAY && DoubleVector.LOGICAL_NAME.equals(fieldSchema.name());
    }

    private boolean isVector(Schema fieldSchema) {
        return isFloatVector(fieldSchema) || isDoubleVector(fieldSchema);
    }

    private boolean isUuid(final Field keyField) {
        return keyField.schema().type() == Schema.STRING_SCHEMA.type() &&
                Uuid.LOGICAL_NAME.equals(keyField.schema().name());
    }
}
