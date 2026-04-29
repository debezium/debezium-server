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
import io.debezium.data.Bits;
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

    private static final String KAFKA_DATE_LOGICAL_NAME = org.apache.kafka.connect.data.Date.LOGICAL_NAME;
    private static final String KAFKA_TIME_LOGICAL_NAME = org.apache.kafka.connect.data.Time.LOGICAL_NAME;
    private static final String KAFKA_TIMESTAMP_LOGICAL_NAME = org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME;
    private static final String DBZ_DATE_SCHEMA_NAME = io.debezium.time.Date.SCHEMA_NAME;
    private static final String DBZ_TIME_SCHEMA_NAME = io.debezium.time.Time.SCHEMA_NAME;
    private static final String DBZ_TIMESTAMP_SCHEMA_NAME = io.debezium.time.Timestamp.SCHEMA_NAME;

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

        return switch (schemaName) {
            case String s when LOGICAL_NAME.equals(s) -> {
                final DecimalParameters params = DecimalParameters.from(connectSchema);
                yield new DecimalType(params.precision(), params.scale());
            }
            case String s when Bits.LOGICAL_NAME.equals(s) ->
                new BytesType();
            case String s when DEBEZIUM_STRING_LOGICAL_TYPES.contains(s)
                    || DEBEZIUM_SERIALIZE_AS_STRING_LOGICAL_TYPES.contains(s)
                    || VariableScaleDecimal.LOGICAL_NAME.equals(s) ->
                new StringType();
            case String s when DEBEZIUM_GEOMETRY_LOGICAL_TYPES.contains(s) ->
                new BytesType();
            case String s when KAFKA_DATE_LOGICAL_NAME.equals(s)
                    || DBZ_DATE_SCHEMA_NAME.equals(s) ->
                new DateType();
            case String s when KAFKA_TIME_LOGICAL_NAME.equals(s)
                    || DBZ_TIME_SCHEMA_NAME.equals(s) ->
                new TimeType();
            case String s when MicroTime.SCHEMA_NAME.equals(s)
                    || NanoTime.SCHEMA_NAME.equals(s) ->
                // Fluss has no sub-millisecond TimeType, store raw value as BIGINT
                new BigIntType();
            case String s when KAFKA_TIMESTAMP_LOGICAL_NAME.equals(s)
                    || DBZ_TIMESTAMP_SCHEMA_NAME.equals(s)
                    || MicroTimestamp.SCHEMA_NAME.equals(s) ->
                new TimestampType();
            case String s when NanoTimestamp.SCHEMA_NAME.equals(s) ->
                // Fluss has no nanosecond-precision TimestampType, store raw value as BIGINT
                new BigIntType();
            case String s when ZonedTimestamp.SCHEMA_NAME.equals(s) ->
                new LocalZonedTimestampType();
            default -> switch (connectSchema.type()) {
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
        return switch (schemaName) {
            case String s when LOGICAL_NAME.equals(s) -> {
                // Struct.get() returns the logical BigDecimal directly for Decimal fields
                final DecimalParameters params = DecimalParameters.from(schema);
                yield Decimal.fromBigDecimal((BigDecimal) value, params.precision(), params.scale());
            }
            case String s when Bits.LOGICAL_NAME.equals(s) ->
                value;
            case String s when DEBEZIUM_STRING_LOGICAL_TYPES.contains(s) ->
                BinaryString.fromString((String) value);
            case String s when DEBEZIUM_SERIALIZE_AS_STRING_LOGICAL_TYPES.contains(s) ->
                BinaryString.fromString(value.toString());
            case String s when VariableScaleDecimal.LOGICAL_NAME.equals(s) -> {
                final BigDecimal decimal = VariableScaleDecimal.toLogical((Struct) value).getDecimalValue().orElse(null);
                yield BinaryString.fromString(decimal != null ? decimal.toPlainString() : "NaN");
            }
            case String s when DEBEZIUM_GEOMETRY_LOGICAL_TYPES.contains(s) ->
                ((Struct) value).getBytes(Geometry.WKB_FIELD);
            case String s when KAFKA_DATE_LOGICAL_NAME.equals(s) ->
                org.apache.kafka.connect.data.Date.fromLogical(schema, (java.util.Date) value);
            case String s when KAFKA_TIME_LOGICAL_NAME.equals(s) ->
                (int) ((java.util.Date) value).getTime();
            case String s when KAFKA_TIMESTAMP_LOGICAL_NAME.equals(s) ->
                TimestampNtz.fromMillis(((java.util.Date) value).getTime());
            case String s when DBZ_TIMESTAMP_SCHEMA_NAME.equals(s) ->
                TimestampNtz.fromMillis(((Number) value).longValue());
            case String s when MicroTimestamp.SCHEMA_NAME.equals(s) ->
                TimestampNtz.fromMicros(((Number) value).longValue());
            case String s when ZonedTimestamp.SCHEMA_NAME.equals(s) ->
                TimestampLtz.fromInstant(ZonedDateTime.parse((String) value).toInstant());
            default -> switch (schema.type()) {
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
        };
    }
}