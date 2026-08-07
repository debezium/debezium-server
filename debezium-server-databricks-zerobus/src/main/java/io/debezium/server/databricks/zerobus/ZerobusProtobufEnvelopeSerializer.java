/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;
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

/** Serializes the stable CDC envelope to the descriptor supplied to a Zerobus Protobuf stream. */
final class ZerobusProtobufEnvelopeSerializer {

    private static final String MESSAGE_NAME = "DebeziumZerobusEnvelope";
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

    DescriptorProto descriptorProto() {
        return DESCRIPTOR_PROTO;
    }

    Descriptors.Descriptor descriptor() {
        return DESCRIPTOR;
    }

    byte[] serialize(ZerobusEnvelope record) {
        try {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(DESCRIPTOR);
            set(builder, "target_table", record.targetTable());
            set(builder, "destination", record.destination());
            if (record.partition() != null) {
                builder.setField(DESCRIPTOR.findFieldByName("partition"), record.partition());
            }
            set(builder, "operation", record.operation().name().toLowerCase(Locale.ROOT));
            set(builder, "idempotency_key", record.idempotencyKey());
            set(builder, "key", canonicalText(record.key()));
            set(builder, "value", canonicalText(record.value()));
            set(builder, "source_position", mapAsJson(record.sourcePosition()));
            set(builder, "headers", mapAsJson(record.headers()));
            return builder.build().toByteArray();
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to serialize Zerobus Protobuf envelope", e);
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
                    .setName("debezium_zerobus_envelope.proto")
                    .setSyntax("proto2")
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

    private String canonicalText(Object value) throws IOException {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value);
        try {
            JsonNode json = mapper.readTree(text);
            return mapper.writeValueAsString(json);
        }
        catch (IOException e) {
            return text;
        }
    }

    private String mapAsJson(Map<String, String> value) throws IOException {
        return value == null || value.isEmpty() ? null : mapper.writeValueAsString(value);
    }
}
