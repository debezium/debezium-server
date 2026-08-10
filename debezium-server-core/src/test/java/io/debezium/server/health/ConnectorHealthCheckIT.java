/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.health;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

import java.time.Duration;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.debezium.runtime.events.ConnectorStartedEvent;
import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.debezium.server.TestConfigSource;
import io.debezium.server.TestConsumer;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(value = PostgresTestResourceLifecycleManager.class, restrictToAnnotatedClass = true)
@EnabledIfSystemProperty(named = "test.apicurio", matches = "false", disabledReason = "Health check IT doesn't run with apicurio profile.")
@DisabledIfSystemProperty(named = "debezium.format.value", matches = "protobuf", disabledReason = "Protobuf format requires schema registry, not available in health check tests.")
public class ConnectorHealthCheckIT {

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
    void livenessReportsUpWhenConnectorIsRunning() {
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .untilAsserted(() -> given()
                        .when().get("/q/health/live")
                        .then()
                        .statusCode(200)
                        .body("status", equalTo("UP"))
                        .body("checks.name", hasItem("debezium"))
                        .body("checks.find { it.name == 'debezium' }.status", equalTo("UP")));
    }

}
