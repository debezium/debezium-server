/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import io.debezium.runtime.events.ConnectorStartedEvent;
import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration test that verifies basic reading from PostgreSQL database.
 *
 * @author Oren Elias
 */
@QuarkusTest
@TestProfile(DebeziumServerFileConfigProviderProfile.class)
@QuarkusTestResource(value = PostgresTestResourceLifecycleManager.class, restrictToAnnotatedClass = true)
@EnabledIfSystemProperty(named = "test.apicurio", matches = "false", disabledReason = "DebeziumServerConfigProvidersIT doesn't run with apicurio profile.")
@DisabledIfSystemProperty(named = "debezium.format.key", matches = "protobuf")
@DisabledIfSystemProperty(named = "debezium.format.value", matches = "protobuf")
public class DebeziumServerConfigProvidersIT {

    private static final int MESSAGE_COUNT = 4;

    @Inject
    TestConsumer testConsumer;

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        if (!TestConfigSource.isItTest()) {
            return;
        }

    }

    void connectorCompleted(@Observes DebeziumCompletionEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError();
        }
    }

    @Test
    public void testPostgresWithJson() {
        Testing.Print.enable();

        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .until(() -> (testConsumer.getValues().size() >= MESSAGE_COUNT));
        assertThat(testConsumer.getValues().size()).isEqualTo(MESSAGE_COUNT);
        assertThat(((String) testConsumer.getValues().get(MESSAGE_COUNT - 1))).contains(
                "\"after\":{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\",\"email\":\"annek@noanswer.org\"}");
    }
}
