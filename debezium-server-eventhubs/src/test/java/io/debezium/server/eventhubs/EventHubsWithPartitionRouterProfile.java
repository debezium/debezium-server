/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class EventHubsWithPartitionRouterProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<String, String>();

        config.put("debezium.sink.eventhubs.partitionid", "");
        config.put("debezium.sink.eventhubs.partitionkey", "");

        config.put("debezium.transforms", "PartitionRouter");
        config.put("debezium.transforms.PartitionRouter.type", "io.debezium.transforms.partitions.PartitionRouting");
        config.put("debezium.transforms.PartitionRouter.partition.payload.fields", "source.db");
        config.put("debezium.transforms.PartitionRouter.partition.topic.num", "5");

        return config;
    }
}
