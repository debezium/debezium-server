/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import java.util.Map;

import io.debezium.embedded.Connect;
import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Specifies for the test to use the {@code Connect} key, value, and header formats.
 *
 * @author Chris Cranford
 */
public class FlussConnectFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        final String format = Connect.class.getSimpleName().toLowerCase();
        return Map.of(
                "debezium.format.key", format,
                "debezium.format.value", format,
                "debezium.format.header", format);
    }
}