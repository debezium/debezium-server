/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Same as {@link ShutdownAfterSnapshotProfile}, but the option is off, so the engine must keep running after the snapshot.
 */
public class ShutdownAfterSnapshotDisabledProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.shutdown.after.snapshot", "false",
                "debezium.source.snapshot.mode", "initial_only",
                "debezium.source.max.batch.size", "1");
    }
}
