/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class ZeroBusJsonSerializerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void serializesDebeziumJsonStringsAsNestedJson() throws Exception {
        ZeroBusRecord record = new ZeroBusRecord(
                "main.bronze.customers",
                "server.inventory.customers",
                0,
                "{\"id\":1}",
                "{\"after\":{\"id\":1,\"name\":\"Anne\"}}",
                Map.of("offset.lsn", "100", "offset.txId", "7"),
                Map.of("source.lsn", "100"),
                ZeroBusOperation.CHANGE,
                "server.inventory.customers|partition=0|key={\"id\":1}|source_position=offset.lsn=100&offset.txId=7");

        JsonNode json = mapper.readTree(new ZeroBusJsonSerializer().serialize(record));

        assertThat(json.get("target_table").asText()).isEqualTo("main.bronze.customers");
        assertThat(json.get("partition").asInt()).isEqualTo(0);
        assertThat(json.get("operation").asText()).isEqualTo("change");
        assertThat(json.get("key").get("id").asInt()).isEqualTo(1);
        assertThat(json.get("value").get("after").get("name").asText()).isEqualTo("Anne");
        assertThat(json.get("source_position").get("offset.lsn").asText()).isEqualTo("100");
        assertThat(json.get("headers").get("source.lsn").asText()).isEqualTo("100");
        assertThat(json.get("idempotency_key").asText()).contains("source_position=offset.lsn=100");
    }

    @Test
    void preservesNonJsonStringsAsText() throws Exception {
        ZeroBusRecord record = new ZeroBusRecord(
                "main.bronze.customers",
                "server.inventory.customers",
                null,
                "plain-key",
                "plain-value",
                Map.of(),
                Map.of(),
                ZeroBusOperation.CHANGE,
                "stable-key");

        JsonNode json = mapper.readTree(new ZeroBusJsonSerializer().serialize(record));

        assertThat(json.get("key").asText()).isEqualTo("plain-key");
        assertThat(json.get("value").asText()).isEqualTo("plain-value");
    }

    @Test
    void serializesDisabledIdempotencyAsNull() throws Exception {
        ZeroBusRecord record = new ZeroBusRecord(
                "main.bronze.customers",
                "server.inventory.customers",
                0,
                "{\"id\":1}",
                "{\"after\":{\"id\":1}}",
                Map.of(),
                Map.of(),
                ZeroBusOperation.CHANGE,
                null);

        JsonNode json = mapper.readTree(new ZeroBusJsonSerializer().serialize(record));

        assertThat(json.get("idempotency_key").isNull()).isTrue();
    }
}
