/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Test profile that enables upsert mode and uses a distinct topic prefix so the target
 * Fluss table is separate from the log table used by {@link FlussIT}.
 *
 * @author Chris Cranford
 */
public class FlussUpsertModeProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.source.topic.prefix", "testcu",
                "debezium.sink.fluss.primary.key.mode", "upsert");
    }
}