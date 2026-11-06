/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;

import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Removes the offsets file before the application starts, so that {@code initial_only} takes the snapshot again.
 */
public class FreshOffsetsResource implements QuarkusTestResourceLifecycleManager {

    @Override
    public Map<String, String> start() {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        try {
            // The offset store can't flush into a directory which doesn't exist yet.
            Files.createDirectories(TestConfigSource.OFFSET_STORE_PATH.toAbsolutePath().getParent());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Map.of();
    }

    @Override
    public void stop() {
    }
}
