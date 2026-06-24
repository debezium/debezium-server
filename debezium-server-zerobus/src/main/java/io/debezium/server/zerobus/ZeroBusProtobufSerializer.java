/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import io.debezium.DebeziumException;

/**
 * Serializes mapped Debezium change events into the protobuf envelope sent to ZeroBus.
 */
public class ZeroBusProtobufSerializer {

    private static final String MESSAGE_NAME = "DebeziumZeroBusRecord";
    private static final DescriptorProto DESCRIPTOR_PROTO = DescriptorProto.newBuilder()
            .setName(MESSAGE_NAME)
            .addField(field("target_table", 1, FieldDescriptorProto.Type.TYPE_STRING))
            .addField(field("destination", 2, FieldDescriptorProto.Type.TYPE_STRING))
            .addField(field("partition", 3, FieldDescriptorProto.Type.TYPE_INT32))
            .addField(field("operation", 4, FieldDescriptorProto.Type.TYPE_STRING))
            .addField(field("idempotency_key", 5, FieldDescriptorProto.Type.TYPE_STRING))
            .addField(field("key", 6, FieldDescriptorProto.Type.TYPE_STRING))
            .addField(field("value", 7, FieldDescriptorProto.Type.TYPE_STRING))
            .addField(field("source_position", 8, FieldDescriptorProto.Type.TYPE_STRING))
            .addField(field("headers", 9, FieldDescriptorProto.Type.TYPE_STRING))
            .build();

    private static final Descriptors.Descriptor DESCRIPTOR = descriptor(DESCRIPTOR_PROTO);

    private final ObjectMapper mapper = new ObjectMapper();

    public DescriptorProto descriptorProto() {
        return DESCRIPTOR_PROTO;
    }

    public Descriptors.Descriptor descriptor() {
        return DESCRIPTOR;
    }

    public byte[] serialize(ZeroBusRecord record) {
        try {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(DESCRIPTOR);
            set(builder, "target_table", record.targetTable());
            set(builder, "destination", record.destination());
            if (record.partition() != null) {
                builder.setField(DESCRIPTOR.findFieldByName("partition"), record.partition());
            }
            set(builder, "operation", record.operation().name().toLowerCase(Locale.ROOT));
            set(builder, "idempotency_key", record.idempotencyKey());
            set(builder, "key", valueAsText(record.key()));
            set(builder, "value", valueAsText(record.value()));
            set(builder, "source_position", mapAsJson(record.sourcePosition()));
            set(builder, "headers", mapAsJson(record.headers()));
            return builder.build().toByteArray();
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to serialize ZeroBus protobuf record", e);
        }
    }

    private static FieldDescriptorProto field(String name, int number, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(type)
                .build();
    }

    private static Descriptors.Descriptor descriptor(DescriptorProto descriptorProto) {
        try {
            FileDescriptorProto fileDescriptorProto = FileDescriptorProto.newBuilder()
                    .setName("debezium_zerobus_record.proto")
                    .setSyntax("proto3")
                    .addMessageType(descriptorProto)
                    .build();
            return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, new Descriptors.FileDescriptor[0])
                    .findMessageTypeByName(MESSAGE_NAME);
        }
        catch (Descriptors.DescriptorValidationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static void set(DynamicMessage.Builder builder, String fieldName, String value) {
        if (value != null) {
            builder.setField(DESCRIPTOR.findFieldByName(fieldName), value);
        }
    }

    private String valueAsText(Object value) throws IOException {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return stringAsCanonicalJsonOrText(new String(bytes, StandardCharsets.UTF_8));
        }
        if (value instanceof String string) {
            return stringAsCanonicalJsonOrText(string);
        }
        return mapper.writeValueAsString(value);
    }

    private String mapAsJson(Map<String, String> value) throws IOException {
        if (value == null || value.isEmpty()) {
            return null;
        }
        return mapper.writeValueAsString(value);
    }

    private String stringAsCanonicalJsonOrText(String value) throws IOException {
        try {
            JsonNode json = mapper.readTree(value);
            return mapper.writeValueAsString(json);
        }
        catch (IOException ignored) {
            return value;
        }
    }
}
