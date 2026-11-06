/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Initial-only snapshot with one record per batch, so that the snapshot is delivered over several batches.
 */
public class ShutdownAfterSnapshotProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.shutdown.after.snapshot", "true",
                "debezium.source.snapshot.mode", "initial_only",
                "debezium.source.max.batch.size", "1");
    }
}
