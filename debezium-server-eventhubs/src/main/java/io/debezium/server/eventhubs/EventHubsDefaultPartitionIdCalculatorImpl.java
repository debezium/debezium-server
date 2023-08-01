/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;

/**
 * Simple partitionKey calculator based on the hashcode of record.destination().
 */
public class EventHubsDefaultPartitionIdCalculatorImpl implements EventHubsPartitionIdCalculator {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHubsDefaultPartitionIdCalculatorImpl.class);
    public static final String PARTITIONING_SELECTOR_DESTINATION = "destination";
    public static final String PARTITIONING_SELECTOR_KEY = "key";
    public static final String PARTITIONING_SELECTOR_VALUE = "value";
    private final String partitioningSelector;
    private List<String> partitioningFieldPaths;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public EventHubsDefaultPartitionIdCalculatorImpl(String partitioningSelector, String partitioningField) {
        this.partitioningSelector = partitioningSelector;

        if (!partitioningField.equals("")) {
            this.partitioningFieldPaths = Arrays.asList(partitioningField.split("\\."));
        }
    }

    @Override
    public Integer derivePartitionIdFromRecord(ChangeEvent<Object, Object> record, Integer partitionCount) {
        if (this.partitioningSelector.equals(PARTITIONING_SELECTOR_DESTINATION)) {
            if (record.destination() == null) {
                return 0;
            }

            return Math.abs(record.destination().hashCode()) % partitionCount;
        }

        Object partitioningInput;

        if (this.partitioningSelector.equals(PARTITIONING_SELECTOR_KEY)) {
            if (record.key() == null) {
                return 0;
            }
            partitioningInput = record.key();
        }
        else if (this.partitioningSelector.equals(PARTITIONING_SELECTOR_VALUE)) {
            if (record.value() == null) {
                return 0;
            }

            partitioningInput = record.value();
        }
        else {
            throw new RuntimeException("Invalid partitioning selector");
        }
        String partitioningField = parsePartitioningField(partitioningInput).orElseThrow(() -> new RuntimeException("Invalid partitioning field"));

        int partition = Math.abs(partitioningField.hashCode()) % partitionCount;
        LOGGER.trace("Sending to partition: {}", partition);

        return partition;
    }

    private Optional<String> parsePartitioningField(Object input) {
        LOGGER.trace("Parsing partitioning field from: {}", input.toString());
        try {
            JsonNode message = MAPPER.readTree(input.toString());

            for (String property : this.partitioningFieldPaths) {
                if (message.has(property)) {
                    message = message.get(property);
                }
                else {
                    LOGGER.warn("Could not find match, sending empty");
                    return Optional.empty();
                }
            }

            LOGGER.trace("Found match: {}", message.textValue());
            return Optional.of(message.textValue());
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }
}
