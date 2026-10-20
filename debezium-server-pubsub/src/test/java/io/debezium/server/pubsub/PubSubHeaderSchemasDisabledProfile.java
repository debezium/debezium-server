/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Test profile that turns off header schemas ({@code debezium.format.header.schemas.enable=false}).
 *
 * <p>Without schemas the header converter no longer wraps a header value in a JSON schema envelope, so
 * the sink must publish the value of the raw Connect header instead of the converter-serialized one.
 */
public class PubSubHeaderSchemasDisabledProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("debezium.format.header.schemas.enable", "false");
    }
}
