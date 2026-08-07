/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;
import java.util.Locale;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.DebeziumException;

/** Serializes the stable CDC envelope to a Zerobus JSON record. */
final class ZerobusJsonEnvelopeSerializer {

    private final ObjectMapper mapper = new ObjectMapper();
    private final String flexibleFieldsEncoding;

    ZerobusJsonEnvelopeSerializer(ZerobusChangeConsumerConfig config) {
        this.flexibleFieldsEncoding = config.getJsonFlexibleFieldsEncoding();
    }

    String serialize(ZerobusEnvelope record) {
        ObjectNode root = mapper.createObjectNode();
        root.put("target_table", record.targetTable());
        root.put("destination", record.destination());
        root.put("partition", record.partition());
        root.put("operation", record.operation().name().toLowerCase(Locale.ROOT));
        root.put("idempotency_key", record.idempotencyKey());
        setFlexibleField(root, "key", toJsonNode(record.key()));
        setFlexibleField(root, "value", toJsonNode(record.value()));
        setFlexibleField(root, "source_position", mapper.valueToTree(record.sourcePosition()));
        setFlexibleField(root, "headers", mapper.valueToTree(record.headers()));

        try {
            return mapper.writeValueAsString(root);
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Failed to serialize Zerobus JSON envelope", e);
        }
    }

    private void setFlexibleField(ObjectNode root, String fieldName, JsonNode value) {
        if (ZerobusChangeConsumerConfig.JSON_FLEXIBLE_FIELDS_STRING.equals(flexibleFieldsEncoding) && !value.isNull()) {
            root.put(fieldName, value.toString());
        }
        else {
            root.set(fieldName, value);
        }
    }

    private JsonNode toJsonNode(Object value) {
        if (value == null) {
            return NullNode.getInstance();
        }
        String text = String.valueOf(value);
        try {
            return mapper.readTree(text);
        }
        catch (IOException e) {
            return mapper.getNodeFactory().textNode(text);
        }
    }
}
