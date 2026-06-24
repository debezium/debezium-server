/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.DebeziumException;

/**
 * Serializes mapped Debezium change events into the JSON rows sent to ZeroBus.
 */
public class ZeroBusJsonSerializer {

    private final ObjectMapper mapper = new ObjectMapper();

    public String serialize(ZeroBusRecord record) {
        ObjectNode root = mapper.createObjectNode();
        root.put("target_table", record.targetTable());
        root.put("destination", record.destination());
        root.put("partition", record.partition());
        root.put("operation", record.operation().name().toLowerCase(Locale.ROOT));
        root.put("idempotency_key", record.idempotencyKey());
        root.set("key", toJsonNode(record.key()));
        root.set("value", toJsonNode(record.value()));
        root.set("source_position", mapper.valueToTree(record.sourcePosition()));
        root.set("headers", mapper.valueToTree(record.headers()));

        try {
            return mapper.writeValueAsString(root);
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Failed to serialize ZeroBus record", e);
        }
    }

    private JsonNode toJsonNode(Object value) {
        if (value == null) {
            return NullNode.getInstance();
        }
        if (value instanceof byte[] bytes) {
            return stringToJsonNode(new String(bytes, StandardCharsets.UTF_8));
        }
        if (value instanceof String string) {
            return stringToJsonNode(string);
        }
        return mapper.valueToTree(value);
    }

    private JsonNode stringToJsonNode(String value) {
        try {
            return mapper.readTree(value);
        }
        catch (IOException ignored) {
            return mapper.getNodeFactory().textNode(value);
        }
    }
}
