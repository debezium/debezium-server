/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import static org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.BYTES_SCHEMA;
import static org.apache.kafka.connect.data.Schema.FLOAT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT16_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT8_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

/**
 * Unit tests for {@link FlussTypeConverter}.
 *
 * @author Chris Cranford
 */
public class FlussTypeConverterTest {

    private FlussTypeConverter typeConverter;

    @BeforeEach
    void beforeEach() {
        typeConverter = new FlussTypeConverter();
    }

    @Test
    void toFlussDataTypeInt8() {
        assertThat(typeConverter.toFlussDataType(INT8_SCHEMA)).isInstanceOf(TinyIntType.class);
    }

    @Test
    void toFlussDataTypeInt16() {
        assertThat(typeConverter.toFlussDataType(INT16_SCHEMA)).isInstanceOf(SmallIntType.class);
    }

    @Test
    void toFlussDataTypeInt32() {
        assertThat(typeConverter.toFlussDataType(INT32_SCHEMA)).isInstanceOf(IntType.class);
    }

    @Test
    void toFlussDataTypeInt64() {
        assertThat(typeConverter.toFlussDataType(INT64_SCHEMA)).isInstanceOf(BigIntType.class);
    }

    @Test
    void toFlussDataTypeFloat32() {
        assertThat(typeConverter.toFlussDataType(FLOAT32_SCHEMA)).isInstanceOf(FloatType.class);
    }

    @Test
    void toFlussDataTypeFloat64() {
        assertThat(typeConverter.toFlussDataType(FLOAT64_SCHEMA)).isInstanceOf(DoubleType.class);
    }

    @Test
    void toFlussDataTypeBoolean() {
        assertThat(typeConverter.toFlussDataType(BOOLEAN_SCHEMA)).isInstanceOf(BooleanType.class);
    }

    @Test
    void toFlussDataTypeString() {
        assertThat(typeConverter.toFlussDataType(STRING_SCHEMA)).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussDataTypeBytes() {
        assertThat(typeConverter.toFlussDataType(BYTES_SCHEMA)).isInstanceOf(BytesType.class);
    }

    @Test
    void toFlussDataTypeDecimalWithExplicitPrecision() {
        final org.apache.kafka.connect.data.Schema decimalSchema = org.apache.kafka.connect.data.Decimal.builder(4)
                .parameter("connect.decimal.precision", "10")
                .build();

        final var result = typeConverter.toFlussDataType(decimalSchema);
        assertThat(result).isInstanceOf(DecimalType.class);

        final DecimalType decimalType = (DecimalType) result;
        assertThat(decimalType.getPrecision()).isEqualTo(10);
        assertThat(decimalType.getScale()).isEqualTo(4);
    }

    @Test
    void toFlussDataTypeDecimalWithDefaultPrecision() {
        final org.apache.kafka.connect.data.Schema decimalSchema = org.apache.kafka.connect.data.Decimal.builder(2).build();

        final var result = typeConverter.toFlussDataType(decimalSchema);
        assertThat(result).isInstanceOf(DecimalType.class);

        final DecimalType decimalType = (DecimalType) result;
        assertThat(decimalType.getPrecision()).isEqualTo(38);
        assertThat(decimalType.getScale()).isEqualTo(2);
    }

    @Test
    void toFlussDataTypeUnsupportedTypeThrows() {
        final org.apache.kafka.connect.data.Schema mapSchema = SchemaBuilder.map(STRING_SCHEMA, INT32_SCHEMA).build();
        assertThatThrownBy(() -> typeConverter.toFlussDataType(mapSchema))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Unsupported Connect type");
    }

    @Test
    void toFlussValueNullValue() {
        assertThat(typeConverter.toFlussValue(null, INT32_SCHEMA)).isNull();
    }

    @Test
    void toFlussValueNullSchema() {
        assertThat(typeConverter.toFlussValue(42, null)).isNull();
    }

    @Test
    void toFlussValueInt8() {
        final Object result = typeConverter.toFlussValue((byte) 1, INT8_SCHEMA);
        assertThat(result).isInstanceOf(Byte.class).isEqualTo((byte) 1);
    }

    @Test
    void toFlussValueInt16() {
        final Object result = typeConverter.toFlussValue((short) 100, INT16_SCHEMA);
        assertThat(result).isInstanceOf(Short.class).isEqualTo((short) 100);
    }

    @Test
    void toFlussValueInt32() {
        final Object result = typeConverter.toFlussValue(42, INT32_SCHEMA);
        assertThat(result).isInstanceOf(Integer.class).isEqualTo(42);
    }

    @Test
    void toFlussValueInt64() {
        final Object result = typeConverter.toFlussValue(123456789L, INT64_SCHEMA);
        assertThat(result).isInstanceOf(Long.class).isEqualTo(123456789L);
    }

    @Test
    void toFlussValueFloat32() {
        final Object result = typeConverter.toFlussValue(1.5f, FLOAT32_SCHEMA);
        assertThat(result).isInstanceOf(Float.class).isEqualTo(1.5f);
    }

    @Test
    void toFlussValueFloat64() {
        final Object result = typeConverter.toFlussValue(3.14, FLOAT64_SCHEMA);
        assertThat(result).isInstanceOf(Double.class).isEqualTo(3.14);
    }

    @Test
    void toFlussValueBoolean() {
        final Object result = typeConverter.toFlussValue(true, BOOLEAN_SCHEMA);
        assertThat(result).isInstanceOf(Boolean.class).isEqualTo(true);
    }

    @Test
    void toFlussValueString() {
        final Object result = typeConverter.toFlussValue("hello", STRING_SCHEMA);
        assertThat(result).isInstanceOf(BinaryString.class);
        assertThat(result.toString()).isEqualTo("hello");
    }

    @Test
    void toFlussValueBytes() {
        final byte[] bytes = new byte[]{ 1, 2, 3 };
        final Object result = typeConverter.toFlussValue(bytes, BYTES_SCHEMA);
        assertThat(result).isInstanceOf(byte[].class).isEqualTo(bytes);
    }

    @Test
    void toFlussValueDecimalWithExplicitPrecision() {
        final org.apache.kafka.connect.data.Schema decimalSchema = org.apache.kafka.connect.data.Decimal.builder(4)
                .parameter("connect.decimal.precision", "10")
                .build();
        final BigDecimal bigDecimal = new BigDecimal("12.3456");

        final Object result = typeConverter.toFlussValue(bigDecimal, decimalSchema);
        assertThat(result).isInstanceOf(Decimal.class);

        final Decimal decimal = (Decimal) result;
        assertThat(decimal.toBigDecimal().stripTrailingZeros())
                .isEqualByComparingTo(bigDecimal.stripTrailingZeros());
    }

    @Test
    void toFlussValueDecimalWithDefaultPrecision() {
        final org.apache.kafka.connect.data.Schema decimalSchema = org.apache.kafka.connect.data.Decimal.builder(2).build();
        final BigDecimal bigDecimal = new BigDecimal("99.99");

        final Object result = typeConverter.toFlussValue(bigDecimal, decimalSchema);
        assertThat(result).isInstanceOf(Decimal.class);
    }

    @Test
    void toFlussDataTypeConnectDate() {
        assertThat(typeConverter.toFlussDataType(org.apache.kafka.connect.data.Date.SCHEMA))
                .isInstanceOf(DateType.class);
    }

    @Test
    void toFlussDataTypeDebeziumDate() {
        assertThat(typeConverter.toFlussDataType(io.debezium.time.Date.builder().build()))
                .isInstanceOf(DateType.class);
    }

    @Test
    void toFlussDataTypeConnectTime() {
        assertThat(typeConverter.toFlussDataType(org.apache.kafka.connect.data.Time.SCHEMA))
                .isInstanceOf(TimeType.class);
    }

    @Test
    void toFlussDataTypeDebeziumTime() {
        assertThat(typeConverter.toFlussDataType(io.debezium.time.Time.builder().build()))
                .isInstanceOf(TimeType.class);
    }

    @Test
    void toFlussDataTypeDebeziumMicroTime() {
        assertThat(typeConverter.toFlussDataType(MicroTime.builder().build())).isInstanceOf(BigIntType.class);
    }

    @Test
    void toFlussDataTypeDebeziumNanoTime() {
        assertThat(typeConverter.toFlussDataType(NanoTime.builder().build())).isInstanceOf(BigIntType.class);
    }

    @Test
    void toFlussDataTypeConnectTimestamp() {
        assertThat(typeConverter.toFlussDataType(org.apache.kafka.connect.data.Timestamp.SCHEMA))
                .isInstanceOf(TimestampType.class);
    }

    @Test
    void toFlussDataTypeDebeziumTimestamp() {
        assertThat(typeConverter.toFlussDataType(io.debezium.time.Timestamp.builder().build()))
                .isInstanceOf(TimestampType.class);
    }

    @Test
    void toFlussDataTypeDebeziumMicroTimestamp() {
        assertThat(typeConverter.toFlussDataType(MicroTimestamp.builder().build())).isInstanceOf(TimestampType.class);
    }

    @Test
    void toFlussDataTypeDebeziumNanoTimestamp() {
        assertThat(typeConverter.toFlussDataType(NanoTimestamp.builder().build())).isInstanceOf(BigIntType.class);
    }

    @Test
    void toFlussDataTypeDebeziumZonedTimestamp() {
        assertThat(typeConverter.toFlussDataType(ZonedTimestamp.builder().build())).isInstanceOf(LocalZonedTimestampType.class);
    }

    @Test
    void toFlussValueConnectDate() {
        final Date date = new Date(2 * 86_400_000L); // day 2
        final Object result = typeConverter.toFlussValue(date, org.apache.kafka.connect.data.Date.SCHEMA);
        assertThat(result).isInstanceOf(Integer.class).isEqualTo(2);
    }

    @Test
    void toFlussValueConnectTime() {
        final Date time = new Date(3_600_000L); // 1 hour in ms
        final Object result = typeConverter.toFlussValue(time, org.apache.kafka.connect.data.Time.SCHEMA);
        assertThat(result).isInstanceOf(Integer.class).isEqualTo(3_600_000);
    }

    @Test
    void toFlussValueConnectTimestamp() {
        final long epochMs = 1_700_000_000_000L;
        final Date ts = new Date(epochMs);
        final Object result = typeConverter.toFlussValue(ts, org.apache.kafka.connect.data.Timestamp.SCHEMA);
        assertThat(result).isInstanceOf(TimestampNtz.class);
        assertThat(((TimestampNtz) result).getMillisecond()).isEqualTo(epochMs);
    }

    @Test
    void toFlussValueDebeziumTimestamp() {
        final long epochMs = 1_700_000_000_000L;
        final Object result = typeConverter.toFlussValue(epochMs, io.debezium.time.Timestamp.builder().build());
        assertThat(result).isInstanceOf(TimestampNtz.class);
        assertThat(((TimestampNtz) result).getMillisecond()).isEqualTo(epochMs);
    }

    @Test
    void toFlussValueDebeziumMicroTimestamp() {
        final long epochMicros = 1_700_000_000_000_000L;
        final Object result = typeConverter.toFlussValue(epochMicros, MicroTimestamp.builder().build());
        assertThat(result).isInstanceOf(TimestampNtz.class);
        assertThat(((TimestampNtz) result).toEpochMicros()).isEqualTo(epochMicros);
    }

    @Test
    void toFlussValueDebeziumZonedTimestamp() {
        final String isoTs = "2023-11-15T10:30:00.000+05:30";
        final Object result = typeConverter.toFlussValue(isoTs, ZonedTimestamp.builder().build());
        assertThat(result).isInstanceOf(TimestampLtz.class);
    }

    @Test
    void toFlussDataTypeDebeziumBits() {
        assertThat(typeConverter.toFlussDataType(Bits.builder(8).build())).isInstanceOf(BytesType.class);
    }

    @Test
    void toFlussDataTypeDebeziumJson() {
        assertThat(typeConverter.toFlussDataType(Json.builder().build())).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussDataTypeDebeziumUuid() {
        assertThat(typeConverter.toFlussDataType(Uuid.builder().build())).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussDataTypeDebeziumXml() {
        assertThat(typeConverter.toFlussDataType(Xml.builder().build())).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussDataTypeDebeziumEnum() {
        assertThat(typeConverter.toFlussDataType(Enum.builder("A,B,C").build())).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussDataTypeDebeziumEnumSet() {
        assertThat(typeConverter.toFlussDataType(EnumSet.builder("A,B,C").build())).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussDataTypeDebeziumTsVector() {
        assertThat(typeConverter.toFlussDataType(TsVector.builder().build())).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussDataTypeDebeziumVariableScaleDecimal() {
        assertThat(typeConverter.toFlussDataType(VariableScaleDecimal.builder().build())).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussDataTypeDebeziumGeometry() {
        assertThat(typeConverter.toFlussDataType(Geometry.builder().build())).isInstanceOf(BytesType.class);
    }

    @Test
    void toFlussDataTypeDebeziumGeography() {
        assertThat(typeConverter.toFlussDataType(Geography.builder().build())).isInstanceOf(BytesType.class);
    }

    @Test
    void toFlussDataTypeDebeziumPoint() {
        assertThat(typeConverter.toFlussDataType(Point.builder().build())).isInstanceOf(BytesType.class);
    }

    @Test
    void toFlussDataTypeDebeziumFloatVector() {
        assertThat(typeConverter.toFlussDataType(FloatVector.builder().build())).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussDataTypeDebeziumDoubleVector() {
        assertThat(typeConverter.toFlussDataType(DoubleVector.builder().build())).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussDataTypeDebeziumSparseDoubleVector() {
        assertThat(typeConverter.toFlussDataType(SparseDoubleVector.builder().build())).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussValueDebeziumBits() {
        final byte[] bits = new byte[]{ 0b00001111 };
        final Object result = typeConverter.toFlussValue(bits, Bits.builder(8).build());
        assertThat(result).isInstanceOf(byte[].class).isEqualTo(bits);
    }

    @Test
    void toFlussValueDebeziumJson() {
        final Object result = typeConverter.toFlussValue("{\"key\":1}", Json.builder().build());
        assertThat(result).isInstanceOf(BinaryString.class);
        assertThat(result.toString()).isEqualTo("{\"key\":1}");
    }

    @Test
    void toFlussValueDebeziumUuid() {
        final String uuid = "550e8400-e29b-41d4-a716-446655440000";
        final Object result = typeConverter.toFlussValue(uuid, Uuid.builder().build());
        assertThat(result).isInstanceOf(BinaryString.class);
        assertThat(result.toString()).isEqualTo(uuid);
    }

    @Test
    void toFlussValueDebeziumVariableScaleDecimal() {
        final org.apache.kafka.connect.data.Schema schema = VariableScaleDecimal.builder().build();
        final org.apache.kafka.connect.data.Struct struct = VariableScaleDecimal.fromLogical(schema, new BigDecimal("12.345"));
        final Object result = typeConverter.toFlussValue(struct, schema);
        assertThat(result).isInstanceOf(BinaryString.class);
        assertThat(new BigDecimal(result.toString())).isEqualByComparingTo(new BigDecimal("12.345"));
    }

    @Test
    void toFlussValueDebeziumGeometry() {
        final byte[] wkb = new byte[]{ 1, 2, 3, 4 };
        final org.apache.kafka.connect.data.Schema schema = Geometry.builder().build();
        final org.apache.kafka.connect.data.Struct struct = Geometry.createValue(schema, wkb, null);
        final Object result = typeConverter.toFlussValue(struct, schema);
        assertThat(result).isInstanceOf(byte[].class).isEqualTo(wkb);
    }

    @Test
    void toFlussValueDebeziumFloatVector() {
        final Object result = typeConverter.toFlussValue(List.of(1.0f, 2.0f, 3.0f), FloatVector.builder().build());
        assertThat(result).isInstanceOf(BinaryString.class);
        assertThat(result.toString()).isNotEmpty();
    }

    @Test
    void toFlussValueDebeziumDoubleVector() {
        final Object result = typeConverter.toFlussValue(List.of(1.0, 2.0, 3.0), DoubleVector.builder().build());
        assertThat(result).isInstanceOf(BinaryString.class);
        assertThat(result.toString()).isNotEmpty();
    }

    @Test
    void toFlussSchemaWithoutPrimaryKey() {
        final org.apache.kafka.connect.data.Schema connectSchema = SchemaBuilder.struct()
                .field("id", INT32_SCHEMA)
                .field("name", STRING_SCHEMA)
                .build();

        final Schema flussSchema = typeConverter.toFlussSchema(connectSchema, List.of());

        assertThat(flussSchema.getColumns()).hasSize(2);
        assertThat(flussSchema.getColumns().get(0).getName()).isEqualTo("id");
        assertThat(flussSchema.getColumns().get(0).getDataType()).isInstanceOf(IntType.class);
        assertThat(flussSchema.getColumns().get(1).getName()).isEqualTo("name");
        assertThat(flussSchema.getColumns().get(1).getDataType()).isInstanceOf(StringType.class);
        assertThat(flussSchema.getPrimaryKey()).isEmpty();
    }

    @Test
    void toFlussSchemaWithPrimaryKey() {
        final org.apache.kafka.connect.data.Schema connectSchema = SchemaBuilder.struct()
                .field("id", INT32_SCHEMA)
                .field("name", STRING_SCHEMA)
                .field("age", INT32_SCHEMA)
                .build();

        final Schema flussSchema = typeConverter.toFlussSchema(connectSchema, List.of("id"));

        assertThat(flussSchema.getColumns()).hasSize(3);
        assertThat(flussSchema.getPrimaryKey()).isPresent();
        assertThat(flussSchema.getPrimaryKey().get().getColumnNames()).containsExactly("id");
    }

    @Test
    void toFlussSchemaWithCompositePrimaryKey() {
        final org.apache.kafka.connect.data.Schema connectSchema = SchemaBuilder.struct()
                .field("tenant_id", INT32_SCHEMA)
                .field("user_id", INT64_SCHEMA)
                .field("value", STRING_SCHEMA)
                .build();

        final Schema flussSchema = typeConverter.toFlussSchema(connectSchema, List.of("tenant_id", "user_id"));

        assertThat(flussSchema.getPrimaryKey()).isPresent();
        assertThat(flussSchema.getPrimaryKey().get().getColumnNames()).containsExactly("tenant_id", "user_id");
    }

    @Test
    void toFlussSchemaAllSupportedTypes() {
        final org.apache.kafka.connect.data.Schema connectSchema = SchemaBuilder.struct()
                .field("f_int8", INT8_SCHEMA)
                .field("f_int16", INT16_SCHEMA)
                .field("f_int32", INT32_SCHEMA)
                .field("f_int64", INT64_SCHEMA)
                .field("f_float32", FLOAT32_SCHEMA)
                .field("f_float64", FLOAT64_SCHEMA)
                .field("f_bool", BOOLEAN_SCHEMA)
                .field("f_string", STRING_SCHEMA)
                .field("f_bytes", BYTES_SCHEMA)
                .field("f_decimal", org.apache.kafka.connect.data.Decimal.builder(2).build())
                .build();

        final Schema flussSchema = typeConverter.toFlussSchema(connectSchema, List.of());

        final Map<String, Class<?>> expectedTypes = Map.of(
                "f_int8", TinyIntType.class,
                "f_int16", SmallIntType.class,
                "f_int32", IntType.class,
                "f_int64", BigIntType.class,
                "f_float32", FloatType.class,
                "f_float64", DoubleType.class,
                "f_bool", BooleanType.class,
                "f_string", StringType.class,
                "f_bytes", BytesType.class,
                "f_decimal", DecimalType.class);

        assertThat(flussSchema.getColumns()).hasSize(10);
        for (var column : flussSchema.getColumns()) {
            assertThat(column.getDataType())
                    .as("Column %s", column.getName())
                    .isInstanceOf(expectedTypes.get(column.getName()));
        }
    }
}