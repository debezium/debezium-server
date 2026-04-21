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
import java.util.List;
import java.util.Map;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TinyIntType;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

/**
 * Unit tests for {@link FlussTypeConverter}.
 *
 * @author Chris Cranford
 */
public class FlussTypeConverterTest {

    @Test
    void toFlussDataTypeInt8() {
        assertThat(FlussTypeConverter.toFlussDataType(INT8_SCHEMA)).isInstanceOf(TinyIntType.class);
    }

    @Test
    void toFlussDataTypeInt16() {
        assertThat(FlussTypeConverter.toFlussDataType(INT16_SCHEMA)).isInstanceOf(SmallIntType.class);
    }

    @Test
    void toFlussDataTypeInt32() {
        assertThat(FlussTypeConverter.toFlussDataType(INT32_SCHEMA)).isInstanceOf(IntType.class);
    }

    @Test
    void toFlussDataTypeInt64() {
        assertThat(FlussTypeConverter.toFlussDataType(INT64_SCHEMA)).isInstanceOf(BigIntType.class);
    }

    @Test
    void toFlussDataTypeFloat32() {
        assertThat(FlussTypeConverter.toFlussDataType(FLOAT32_SCHEMA)).isInstanceOf(FloatType.class);
    }

    @Test
    void toFlussDataTypeFloat64() {
        assertThat(FlussTypeConverter.toFlussDataType(FLOAT64_SCHEMA)).isInstanceOf(DoubleType.class);
    }

    @Test
    void toFlussDataTypeBoolean() {
        assertThat(FlussTypeConverter.toFlussDataType(BOOLEAN_SCHEMA)).isInstanceOf(BooleanType.class);
    }

    @Test
    void toFlussDataTypeString() {
        assertThat(FlussTypeConverter.toFlussDataType(STRING_SCHEMA)).isInstanceOf(StringType.class);
    }

    @Test
    void toFlussDataTypeBytes() {
        assertThat(FlussTypeConverter.toFlussDataType(BYTES_SCHEMA)).isInstanceOf(BytesType.class);
    }

    @Test
    void toFlussDataTypeDecimalWithExplicitPrecision() {
        final org.apache.kafka.connect.data.Schema decimalSchema = org.apache.kafka.connect.data.Decimal.builder(4)
                .parameter("connect.decimal.precision", "10")
                .build();

        final var result = FlussTypeConverter.toFlussDataType(decimalSchema);
        assertThat(result).isInstanceOf(DecimalType.class);

        final DecimalType decimalType = (DecimalType) result;
        assertThat(decimalType.getPrecision()).isEqualTo(10);
        assertThat(decimalType.getScale()).isEqualTo(4);
    }

    @Test
    void toFlussDataTypeDecimalWithDefaultPrecision() {
        final org.apache.kafka.connect.data.Schema decimalSchema = org.apache.kafka.connect.data.Decimal.builder(2).build();

        final var result = FlussTypeConverter.toFlussDataType(decimalSchema);
        assertThat(result).isInstanceOf(DecimalType.class);

        final DecimalType decimalType = (DecimalType) result;
        assertThat(decimalType.getPrecision()).isEqualTo(38);
        assertThat(decimalType.getScale()).isEqualTo(2);
    }

    @Test
    void toFlussDataTypeUnsupportedTypeThrows() {
        final org.apache.kafka.connect.data.Schema mapSchema = SchemaBuilder.map(STRING_SCHEMA, INT32_SCHEMA).build();
        assertThatThrownBy(() -> FlussTypeConverter.toFlussDataType(mapSchema))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Unsupported Connect type");
    }

    @Test
    void toFlussValueNullValue() {
        assertThat(FlussTypeConverter.toFlussValue(null, INT32_SCHEMA)).isNull();
    }

    @Test
    void toFlussValueNullSchema() {
        assertThat(FlussTypeConverter.toFlussValue(42, null)).isNull();
    }

    @Test
    void toFlussValueInt8() {
        final Object result = FlussTypeConverter.toFlussValue((byte) 1, INT8_SCHEMA);
        assertThat(result).isInstanceOf(Byte.class).isEqualTo((byte) 1);
    }

    @Test
    void toFlussValueInt16() {
        final Object result = FlussTypeConverter.toFlussValue((short) 100, INT16_SCHEMA);
        assertThat(result).isInstanceOf(Short.class).isEqualTo((short) 100);
    }

    @Test
    void toFlussValueInt32() {
        final Object result = FlussTypeConverter.toFlussValue(42, INT32_SCHEMA);
        assertThat(result).isInstanceOf(Integer.class).isEqualTo(42);
    }

    @Test
    void toFlussValueInt64() {
        final Object result = FlussTypeConverter.toFlussValue(123456789L, INT64_SCHEMA);
        assertThat(result).isInstanceOf(Long.class).isEqualTo(123456789L);
    }

    @Test
    void toFlussValueFloat32() {
        final Object result = FlussTypeConverter.toFlussValue(1.5f, FLOAT32_SCHEMA);
        assertThat(result).isInstanceOf(Float.class).isEqualTo(1.5f);
    }

    @Test
    void toFlussValueFloat64() {
        final Object result = FlussTypeConverter.toFlussValue(3.14, FLOAT64_SCHEMA);
        assertThat(result).isInstanceOf(Double.class).isEqualTo(3.14);
    }

    @Test
    void toFlussValueBoolean() {
        final Object result = FlussTypeConverter.toFlussValue(true, BOOLEAN_SCHEMA);
        assertThat(result).isInstanceOf(Boolean.class).isEqualTo(true);
    }

    @Test
    void toFlussValueString() {
        final Object result = FlussTypeConverter.toFlussValue("hello", STRING_SCHEMA);
        assertThat(result).isInstanceOf(BinaryString.class);
        assertThat(result.toString()).isEqualTo("hello");
    }

    @Test
    void toFlussValueBytes() {
        final byte[] bytes = new byte[]{ 1, 2, 3 };
        final Object result = FlussTypeConverter.toFlussValue(bytes, BYTES_SCHEMA);
        assertThat(result).isInstanceOf(byte[].class).isEqualTo(bytes);
    }

    @Test
    void toFlussValueDecimalWithExplicitPrecision() {
        final org.apache.kafka.connect.data.Schema decimalSchema = org.apache.kafka.connect.data.Decimal.builder(4)
                .parameter("connect.decimal.precision", "10")
                .build();
        final BigDecimal bigDecimal = new BigDecimal("12.3456");

        final Object result = FlussTypeConverter.toFlussValue(bigDecimal, decimalSchema);
        assertThat(result).isInstanceOf(Decimal.class);

        final Decimal decimal = (Decimal) result;
        assertThat(decimal.toBigDecimal().stripTrailingZeros())
                .isEqualByComparingTo(bigDecimal.stripTrailingZeros());
    }

    @Test
    void toFlussValueDecimalWithDefaultPrecision() {
        final org.apache.kafka.connect.data.Schema decimalSchema = org.apache.kafka.connect.data.Decimal.builder(2).build();
        final BigDecimal bigDecimal = new BigDecimal("99.99");

        final Object result = FlussTypeConverter.toFlussValue(bigDecimal, decimalSchema);
        assertThat(result).isInstanceOf(Decimal.class);
    }

    @Test
    void toFlussSchemaWithoutPrimaryKey() {
        final org.apache.kafka.connect.data.Schema connectSchema = SchemaBuilder.struct()
                .field("id", INT32_SCHEMA)
                .field("name", STRING_SCHEMA)
                .build();

        final Schema flussSchema = FlussTypeConverter.toFlussSchema(connectSchema, List.of());

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

        final Schema flussSchema = FlussTypeConverter.toFlussSchema(connectSchema, List.of("id"));

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

        final Schema flussSchema = FlussTypeConverter.toFlussSchema(connectSchema, List.of("tenant_id", "user_id"));

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

        final Schema flussSchema = FlussTypeConverter.toFlussSchema(connectSchema, List.of());

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