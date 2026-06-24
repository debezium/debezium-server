/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.protobuf.DynamicMessage;

class ZeroBusProtobufSerializerTest {

    @Test
    void serializesEnvelopeAsDynamicProtobuf() throws Exception {
        ZeroBusProtobufSerializer serializer = new ZeroBusProtobufSerializer();
        ZeroBusRecord record = new ZeroBusRecord(
                "main.bronze.customers",
                "server.inventory.customers",
                0,
                "{\"id\":1}",
                "{\"after\":{\"id\":1,\"name\":\"Anne\"}}",
                Map.of("offset.lsn", "100", "offset.txId", "7"),
                Map.of("source.lsn", "100"),
                ZeroBusOperation.UPDATE,
                "server.inventory.customers|partition=0|key={\"id\":1}|source_position=offset.lsn=100&offset.txId=7");

        DynamicMessage message = DynamicMessage.parseFrom(serializer.descriptor(), serializer.serialize(record));

        assertThat(message.getField(serializer.descriptor().findFieldByName("target_table"))).isEqualTo("main.bronze.customers");
        assertThat(message.getField(serializer.descriptor().findFieldByName("destination"))).isEqualTo("server.inventory.customers");
        assertThat(message.hasField(serializer.descriptor().findFieldByName("partition"))).isTrue();
        assertThat(message.getField(serializer.descriptor().findFieldByName("partition"))).isEqualTo(0);
        assertThat(message.getField(serializer.descriptor().findFieldByName("operation"))).isEqualTo("update");
        assertThat(message.getField(serializer.descriptor().findFieldByName("key"))).isEqualTo("{\"id\":1}");
        assertThat(message.getField(serializer.descriptor().findFieldByName("value"))).isEqualTo("{\"after\":{\"id\":1,\"name\":\"Anne\"}}");
        assertThat(message.getField(serializer.descriptor().findFieldByName("source_position")).toString()).contains("\"offset.lsn\":\"100\"");
        assertThat(message.getField(serializer.descriptor().findFieldByName("headers")).toString()).contains("\"source.lsn\":\"100\"");
        assertThat(message.getField(serializer.descriptor().findFieldByName("idempotency_key")).toString()).contains("source_position=offset.lsn=100");
    }

    @Test
    void descriptorMatchesZeroBusEnvelopeFields() {
        ZeroBusProtobufSerializer serializer = new ZeroBusProtobufSerializer();

        assertThat(serializer.descriptorProto().getName()).isEqualTo("DebeziumZeroBusRecord");
        assertThat(serializer.descriptorProto().getFieldList())
                .extracting(field -> field.getName() + ":" + field.getNumber())
                .containsExactly(
                        "target_table:1",
                        "destination:2",
                        "partition:3",
                        "operation:4",
                        "idempotency_key:5",
                        "key:6",
                        "value:7",
                        "source_position:8",
                        "headers:9");
    }

    @Test
    void nullPartitionIsDistinctFromPartitionZero() throws Exception {
        ZeroBusProtobufSerializer serializer = new ZeroBusProtobufSerializer();
        ZeroBusRecord record = new ZeroBusRecord(
                "main.bronze.customers",
                "server.inventory.customers",
                null,
                "{\"id\":1}",
                "{\"after\":{\"id\":1}}",
                Map.of(),
                Map.of(),
                ZeroBusOperation.UPDATE,
                "stable-idempotency-key");

        DynamicMessage message = DynamicMessage.parseFrom(serializer.descriptor(), serializer.serialize(record));

        assertThat(message.hasField(serializer.descriptor().findFieldByName("partition"))).isFalse();
    }
}
