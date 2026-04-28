/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import static org.apache.kafka.connect.data.Decimal.LOGICAL_NAME;
import static org.apache.kafka.connect.data.Decimal.SCALE_FIELD;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;
import org.apache.kafka.connect.data.Struct;

import io.debezium.DebeziumException;
import io.debezium.data.Enum;
import io.debezium.data.EnumSet;
import io.debezium.data.Json;
import io.debezium.data.TsVector;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.Xml;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.debezium.data.vector.SparseDoubleVector;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Strings;

/**
 * Converter for all Kafka Connect to Apache Fluss type mapping.
 *
 * <p>Handles both schema-level conversion (Connect {@link org.apache.kafka.connect.data.Schema} to Fluss
 * {@link DataType}) and runtime value conversion (Connect field values to Fluss row values).
 *
 * @author Chris Cranford
 */
public class FlussTypeConverter {

    private static final long MILLIS_PER_DAY = 86_400_000L;

    private static final Set<String> DEBEZIUM_STRING_LOGICAL_TYPES = Set.of(
            Json.LOGICAL_NAME,
            Uuid.LOGICAL_NAME,
            Xml.LOGICAL_NAME,
            TsVector.LOGICAL_NAME,
            Enum.LOGICAL_NAME,
            EnumSet.LOGICAL_NAME);

    private static final Set<String> DEBEZIUM_SERIALIZE_AS_STRING_LOGICAL_TYPES = Set.of(
            FloatVector.LOGICAL_NAME,
            DoubleVector.LOGICAL_NAME,
            SparseDoubleVector.LOGICAL_NAME);

    private static final Set<String> DEBEZIUM_GEOMETRY_LOGICAL_TYPES = Set.of(
            Geometry.LOGICAL_NAME,
            Geography.LOGICAL_NAME,
            Point.LOGICAL_NAME);

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
    public Schema toFlussSchema(org.apache.kafka.connect.data.Schema connectSchema, List<String> primaryKeyFields) {
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
    public DataType toFlussDataType(org.apache.kafka.connect.data.Schema connectSchema) {
        final String schemaName = Strings.defaultIfBlank(connectSchema.name(), "");

        if (LOGICAL_NAME.equals(schemaName)) {
            final DecimalParameters params = DecimalParameters.from(connectSchema);
            return new DecimalType(params.precision(), params.scale());
        }

        if (io.debezium.data.Bits.LOGICAL_NAME.equals(schemaName)) {
            return new BytesType();
        }

        if (DEBEZIUM_STRING_LOGICAL_TYPES.contains(schemaName)
                || DEBEZIUM_SERIALIZE_AS_STRING_LOGICAL_TYPES.contains(schemaName)
                || VariableScaleDecimal.LOGICAL_NAME.equals(schemaName)) {
            return new StringType();
        }

        if (DEBEZIUM_GEOMETRY_LOGICAL_TYPES.contains(schemaName)) {
            return new BytesType();
        }

        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schemaName)
                || io.debezium.time.Date.SCHEMA_NAME.equals(schemaName)) {
            return new DateType();
        }

        if (org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(schemaName)
                || io.debezium.time.Time.SCHEMA_NAME.equals(schemaName)) {
            return new TimeType();
        }

        if (MicroTime.SCHEMA_NAME.equals(schemaName) || NanoTime.SCHEMA_NAME.equals(schemaName)) {
            // Fluss has no sub-millisecond TimeType, store raw value as BIGINT
            return new BigIntType();
        }

        if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(schemaName)
                || io.debezium.time.Timestamp.SCHEMA_NAME.equals(schemaName)
                || MicroTimestamp.SCHEMA_NAME.equals(schemaName)) {
            return new TimestampType();
        }

        if (NanoTimestamp.SCHEMA_NAME.equals(schemaName)) {
            // Fluss has no nanosecond-precision TimestampType, store raw value as BIGINT
            return new BigIntType();
        }

        if (ZonedTimestamp.SCHEMA_NAME.equals(schemaName)) {
            return new LocalZonedTimestampType();
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
    public Object toFlussValue(Object value, org.apache.kafka.connect.data.Schema schema) {
        if (value == null || schema == null) {
            return null;
        }

        final String schemaName = Strings.defaultIfBlank(schema.name(), "");

        if (LOGICAL_NAME.equals(schemaName)) {
            // Struct.get() returns the logical BigDecimal directly for Decimal fields
            final DecimalParameters params = DecimalParameters.from(schema);
            return Decimal.fromBigDecimal((BigDecimal) value, params.precision(), params.scale());
        }

        if (io.debezium.data.Bits.LOGICAL_NAME.equals(schemaName)) {
            return value;
        }

        if (DEBEZIUM_STRING_LOGICAL_TYPES.contains(schemaName)) {
            return BinaryString.fromString((String) value);
        }

        if (DEBEZIUM_SERIALIZE_AS_STRING_LOGICAL_TYPES.contains(schemaName)) {
            return BinaryString.fromString(value.toString());
        }

        if (VariableScaleDecimal.LOGICAL_NAME.equals(schemaName)) {
            final BigDecimal decimal = VariableScaleDecimal.toLogical((Struct) value).getDecimalValue().orElse(null);
            return BinaryString.fromString(decimal != null ? decimal.toPlainString() : "NaN");
        }

        if (DEBEZIUM_GEOMETRY_LOGICAL_TYPES.contains(schemaName)) {
            return ((Struct) value).getBytes(Geometry.WKB_FIELD);
        }

        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schemaName)) {
            return (int) (((java.util.Date) value).getTime() / MILLIS_PER_DAY);
        }

        if (org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(schemaName)) {
            return (int) ((java.util.Date) value).getTime();
        }

        if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(schemaName)) {
            return TimestampNtz.fromMillis(((java.util.Date) value).getTime());
        }

        if (io.debezium.time.Timestamp.SCHEMA_NAME.equals(schemaName)) {
            return TimestampNtz.fromMillis(((Number) value).longValue());
        }

        if (MicroTimestamp.SCHEMA_NAME.equals(schemaName)) {
            return TimestampNtz.fromMicros(((Number) value).longValue());
        }

        if (ZonedTimestamp.SCHEMA_NAME.equals(schemaName)) {
            return TimestampLtz.fromInstant(ZonedDateTime.parse((String) value).toInstant());
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