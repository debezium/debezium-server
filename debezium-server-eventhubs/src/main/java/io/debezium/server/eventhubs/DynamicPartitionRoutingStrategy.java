/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

public enum DynamicPartitionRoutingStrategy {
    DEFAULT,
    KEY,
    PARTITIONID;

    public static DynamicPartitionRoutingStrategy fromString(String value) {
        try {
            return DynamicPartitionRoutingStrategy.valueOf(value.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            return DEFAULT;
        }
    }
}