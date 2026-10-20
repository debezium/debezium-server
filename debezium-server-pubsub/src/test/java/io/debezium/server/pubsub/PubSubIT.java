/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to a Google Cloud PubSub stream running on a Google PubSub Emulator
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(PubSubTestResourceLifecycleManager.class)
public class PubSubIT extends PubSubBaseIT {

    @ConfigProperty(name = "debezium.source.database.port")
    String postgresPort;

    @Override
    protected int postgresPort() {
        return Integer.parseInt(postgresPort);
    }
}
