/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;

/** Applies optional sink-native filters before an envelope is routed or serialized. */
final class ZerobusEventFilter {

    record Decision(boolean accepted, String reason) {
        static Decision accept() {
            return new Decision(true, "accepted");
        }

        static Decision drop(String reason) {
            return new Decision(false, reason);
        }
    }

    private final ZerobusChangeConsumerConfig config;
    private final ZerobusEnvelopeMapper mapper;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final Pattern destinationPattern;
    private final Pattern headerValuePattern;
    private final Pattern valuePattern;

    ZerobusEventFilter(ZerobusChangeConsumerConfig config, ZerobusEnvelopeMapper mapper) {
        this.config = config;
        this.mapper = mapper;
        destinationPattern = compile(config.getFilterDestinationRegex());
        headerValuePattern = compile(config.getFilterHeaderValueRegex());
        valuePattern = compile(config.getFilterValueRegex());
    }

    Decision evaluate(BatchEvent record) {
        if (destinationPattern != null && !destinationPattern.matcher(record.destination()).matches()) {
            return Decision.drop("destination");
        }
        if (!config.getFilterOperations().isEmpty() && !config.getFilterOperations().contains(mapper.operation(record))) {
            return Decision.drop("operation");
        }
        if (headerValuePattern != null && !matchesHeader(record)) {
            return Decision.drop("header");
        }
        return valuePattern == null ? Decision.accept() : matchesValue(record);
    }

    private boolean matchesHeader(BatchEvent record) {
        if (record.headers() == null) {
            return false;
        }
        for (Header<Object> header : record.headers()) {
            if (config.getFilterHeaderName().equals(header.getKey())
                    && headerValuePattern.matcher(String.valueOf(header.getValue())).matches()) {
                return true;
            }
        }
        return false;
    }

    private Decision matchesValue(BatchEvent record) {
        try {
            String value = serializedValue(record.value());
            JsonNode selected = jsonMapper.readTree(value).at(config.getFilterValueJsonPointer());
            if (selected.isMissingNode()) {
                return Decision.drop("value_missing");
            }
            String selectedValue = selected.isTextual() ? selected.textValue() : selected.toString();
            return valuePattern.matcher(selectedValue).matches() ? Decision.accept() : Decision.drop("value");
        }
        catch (IOException | RuntimeException e) {
            if (ZerobusChangeConsumerConfig.FILTER_MALFORMED_DROP.equals(config.getFilterMalformedMode())) {
                return Decision.drop("malformed_value");
            }
            throw new DebeziumException("Unable to evaluate Zerobus filter.value.json.pointer against the event value", e);
        }
    }

    private static Pattern compile(String expression) {
        return expression == null ? null : Pattern.compile(expression);
    }

    private static String serializedValue(Object value) {
        if (value == null) {
            return "null";
        }
        return value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(value);
    }
}
