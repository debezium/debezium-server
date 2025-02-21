/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.milvus;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.milvus.v2.client.MilvusClientV2;

/**
 * Manages collections in Milvus database. Maps Kafka Connect schema into Milvus schema and validates
 * contraints.
 *
 * @author Jiri Pechanec
 */
public class MilvusSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusChangeConsumer.class);

    private MilvusClientV2 milvusClient;

    public MilvusSchema(MilvusClientV2 milvusClient) {
        this.milvusClient = milvusClient;
    }

    private void remapDatatype() {
        // TODO mapping of types
        /*
         * INT64: numpy.int64
         * VARCHAR: VARCHAR
         * Scalar field supports:
         *
         * BOOL: Boolean (true or false)
         * INT8: numpy.int8
         * INT16: numpy.int16
         * INT32: numpy.int32
         * INT64: numpy.int64
         * FLOAT: numpy.float32
         * DOUBLE: numpy.double
         * VARCHAR: VARCHAR
         * JSON: JSON
         * Array: Array
         *
         * BINARY_VECTOR: Stores binary data as a sequence of 0s and 1s, used for compact feature representation in image processing and information retrieval.
         * FLOAT_VECTOR: Stores 32-bit floating-point numbers, commonly used in scientific computing and machine learning for representing real numbers.
         * FLOAT16_VECTOR: Stores 16-bit half-precision floating-point numbers, used in deep learning and GPU computations for memory and bandwidth efficiency.
         * BFLOAT16_VECTOR: Stores 16-bit floating-point numbers with reduced precision but the same exponent range as Float32, popular in deep learning for reducing memory and computational requirements
         * without significantly impacting accuracy.
         * SPARSE_FLOAT_VECTOR:
         */
    }

    public void validateKey(Schema schema) {
        if (schema.type() != Schema.Type.STRUCT) {
            throw new DebeziumException("Only structs are supported as the key");
        }
        if (schema.fields().size() != 1) {
            throw new DebeziumException("Key must have exactly one field");
        }
        final var keyField = schema.fields().get(0);
        if (keyField.schema().type() != Schema.STRING_SCHEMA.type()
                && keyField.schema().type() != Schema.INT64_SCHEMA.type()) {
            throw new DebeziumException("Only string and int64 type can be used as key");
        }
    }
}
