/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import static org.apache.kafka.connect.data.Decimal.LOGICAL_NAME;
import static org.apache.kafka.connect.data.Decimal.SCALE_FIELD;

import java.math.BigDecimal;
import java.util.List;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TinyIntType;

import io.debezium.DebeziumException;

/**
 * Converter for all Kafka Connect to Apache Fluss type mapping.
 *
 * <p>Handles both schema-level conversion (Connect {@link org.apache.kafka.connect.data.Schema} to Fluss
 * {@link DataType}) and runtime value conversion (Connect field values to Fluss row values).
 *
 * @author Chris Cranford
 */
public class FlussTypeConverter {

    record DecimalParameters(int precision, int scale) {
        static DecimalParameters from(org.apache.kafka.connect.data.Schema schema) {
            final int scale = Integer.parseInt(schema.parameters().get(SCALE_FIELD));
            final int precision = schema.parameters().containsKey("connect.decimal.precision")
                    ? Integer.parseInt(schema.parameters().get("connect.decimal.precision"))
                    : 38;
            return new DecimalParameters(precision, scale);
        }
    }

    /**
     * Builds a Fluss {@link Schema} from a Kafka Connect row schema.
     *
     * @param connectSchema the Connect schema describing the row fields
     * @param primaryKeyFields names of columns that form the primary key; empty list for log tables
     * @return a Fluss Schema ready for table creation
     */
    public static Schema toFlussSchema(org.apache.kafka.connect.data.Schema connectSchema, List<String> primaryKeyFields) {
        final Schema.Builder builder = Schema.newBuilder();
        for (org.apache.kafka.connect.data.Field field : connectSchema.fields()) {
            builder.column(field.name(), toFlussDataType(field.schema()));
        }

        if (!primaryKeyFields.isEmpty()) {
            builder.primaryKey(primaryKeyFields.toArray(new String[0]));
        }

        return builder.build();
    }

    /**
     * Converts a Kafka Connect {@link org.apache.kafka.connect.data.Schema} to the matching Fluss {@link DataType}.
     *
     * @param connectSchema the Connect field schema
     * @return corresponding Fluss DataType
     */
    static DataType toFlussDataType(org.apache.kafka.connect.data.Schema connectSchema) {
        // Logical types take priority over the base type
        if (LOGICAL_NAME.equals(connectSchema.name())) {
            final DecimalParameters params = DecimalParameters.from(connectSchema);
            return new DecimalType(params.precision(), params.scale());
        }

        return switch (connectSchema.type()) {
            case INT8 -> new TinyIntType();
            case INT16 -> new SmallIntType();
            case INT32 -> new IntType();
            case INT64 -> new BigIntType();
            case FLOAT32 -> new FloatType();
            case FLOAT64 -> new DoubleType();
            case BOOLEAN -> new BooleanType();
            case STRING -> new StringType();
            case BYTES -> new BytesType();
            default -> throw new DebeziumException("Unsupported Connect type for Fluss schema creation: " + connectSchema.type());
        };
    }

    /**
     * Converts a Kafka Connect field value to its Fluss-compatible Java representation.
     *
     * @param value the value from a Connect {@link org.apache.kafka.connect.data.Struct}
     * @param schema the Connect schema of the field
     * @return the value in the form expected by Fluss {@link org.apache.fluss.row.GenericRow#setField}
     */
    static Object toFlussValue(Object value, org.apache.kafka.connect.data.Schema schema) {
        if (value == null || schema == null) {
            return null;
        }

        if (LOGICAL_NAME.equals(schema.name())) {
            // Struct.get() returns the logical BigDecimal directly for Decimal fields
            final DecimalParameters params = DecimalParameters.from(schema);
            return Decimal.fromBigDecimal((BigDecimal) value, params.precision(), params.scale());
        }

        return switch (schema.type()) {
            case INT8 -> ((Number) value).byteValue();
            case INT16 -> ((Number) value).shortValue();
            case INT32 -> ((Number) value).intValue();
            case INT64 -> ((Number) value).longValue();
            case FLOAT32 -> ((Number) value).floatValue();
            case FLOAT64 -> ((Number) value).doubleValue();
            case BOOLEAN -> (Boolean) value;
            case STRING -> BinaryString.fromString((String) value);
            case BYTES -> (byte[]) value;
            default -> value;
        };
    }
}