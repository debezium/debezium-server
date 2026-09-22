/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Routes each record to the partition {@code id % 2} with the {@code PartitionRouting} SMT.
 */
public class PartitionRoutingProfile implements QuarkusTestProfile {

    public static final int PARTITION_COUNT = 2;

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<>();
        config.put("debezium.source.table.include.list", "inventory.customers,inventory.products");
        config.put("debezium.transforms", "addheader,routing");
        config.put("debezium.transforms.routing.type", "io.debezium.transforms.partitions.PartitionRouting");
        config.put("debezium.transforms.routing.partition.payload.fields", "change.id");
        config.put("debezium.transforms.routing.partition.topic.num", String.valueOf(PARTITION_COUNT));
        return config;
    }
}
