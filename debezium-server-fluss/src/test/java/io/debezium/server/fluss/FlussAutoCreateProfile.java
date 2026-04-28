/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Test profile that auto-creates the target table for the specified source include tables.
 *
 * @author Chris Cranford
 */
public class FlussAutoCreateProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.sink.fluss.table.auto.create", "true",
                "debezium.source.table.include.list", "inventory.orders");
    }
}