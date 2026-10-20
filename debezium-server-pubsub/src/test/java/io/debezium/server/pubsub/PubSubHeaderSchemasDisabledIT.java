/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import static org.assertj.core.api.Assertions.assertThat;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.google.pubsub.v1.PubsubMessage;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration test that verifies the Pub/Sub sink forwards raw Connect header values as message attributes
 * when header schemas are disabled, i.e. with {@code debezium.format.header.schemas.enable=false}.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(PubSubTestResourceLifecycleManager.class)
@TestProfile(PubSubHeaderSchemasDisabledProfile.class)
public class PubSubHeaderSchemasDisabledIT extends PubSubBaseIT {

    @ConfigProperty(name = "debezium.source.database.port")
    String postgresPort;

    @Override
    protected int postgresPort() {
        return Integer.parseInt(postgresPort);
    }

    @Override
    protected void assertHeaderAttributes(PubsubMessage message) {
        // With header schemas disabled the converter serializes the inserted literal as a JSON string
        // ("\"headerValue\""); the attribute must carry the raw value instead.
        assertThat(message.getAttributesMap()).containsEntry("headerKey", "headerValue");
    }
}
