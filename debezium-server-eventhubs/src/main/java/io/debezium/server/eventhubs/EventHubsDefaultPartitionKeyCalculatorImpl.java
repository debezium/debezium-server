/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import io.debezium.engine.ChangeEvent;

/**
 * Simple partitionKey calculator based on the hashcode of record.destination().
 */
public class EventHubsDefaultPartitionKeyCalculatorImpl implements EventHubsPartitionKeyCalculator {
    @Override
    public Integer derivePartitionIdFromRecord(ChangeEvent<Object, Object> record, Integer partitionCount) {
        if (record.destination() == null) {
            return 0;
        }

        return Math.abs(record.destination().hashCode()) % partitionCount;
    }
}
