/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import io.debezium.engine.ChangeEvent;

/**
 * Interface for calculating/deriving the partition key for a given record.
 */
public interface EventHubsPartitionKeyCalculator {
    Integer derivePartitionIdFromRecord(ChangeEvent<Object, Object> record, Integer partitionCount);
}
