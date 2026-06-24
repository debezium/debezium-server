/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

class ZeroBusArrowSerializerTest {

    @Test
    void serializesEnvelopeAsArrowBatch() {
        ZeroBusArrowSerializer serializer = new ZeroBusArrowSerializer();
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

        try (VectorSchemaRoot root = serializer.serialize(java.util.List.of(record))) {
            assertThat(root.getRowCount()).isEqualTo(1);
            assertThat(string(root, "target_table", 0)).isEqualTo("main.bronze.customers");
            assertThat(string(root, "destination", 0)).isEqualTo("server.inventory.customers");
            assertThat(((IntVector) root.getVector("partition")).get(0)).isEqualTo(0);
            assertThat(string(root, "operation", 0)).isEqualTo("update");
            assertThat(string(root, "key", 0)).isEqualTo("{\"id\":1}");
            assertThat(string(root, "value", 0)).isEqualTo("{\"after\":{\"id\":1,\"name\":\"Anne\"}}");
            assertThat(string(root, "source_position", 0)).contains("\"offset.lsn\":\"100\"");
            assertThat(string(root, "headers", 0)).contains("\"source.lsn\":\"100\"");
            assertThat(string(root, "idempotency_key", 0)).contains("source_position=offset.lsn=100");
        }
        finally {
            serializer.close();
        }
    }

    @Test
    void schemaMatchesZeroBusEnvelopeFields() {
        ZeroBusArrowSerializer serializer = new ZeroBusArrowSerializer();

        assertThat(serializer.schema().getFields())
                .extracting(field -> field.getName() + ":" + field.getType())
                .containsExactly(
                        "target_table:Utf8",
                        "destination:Utf8",
                        "partition:Int(32, true)",
                        "operation:Utf8",
                        "idempotency_key:Utf8",
                        "key:Utf8",
                        "value:Utf8",
                        "source_position:Utf8",
                        "headers:Utf8");
        serializer.close();
    }

    private static String string(VectorSchemaRoot root, String field, int row) {
        return ((VarCharVector) root.getVector(field)).getObject(row).toString();
    }
}
