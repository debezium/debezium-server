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

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.debezium.server.TestConfigSource;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(FailingConnectionProfile.class)
@QuarkusTestResource(value = PostgresTestResourceLifecycleManager.class, restrictToAnnotatedClass = true)
@EnabledIfSystemProperty(named = "test.apicurio", matches = "false", disabledReason = "Health check IT doesn't run with apicurio profile.")
@DisabledIfSystemProperty(named = "debezium.format.value", matches = "protobuf", disabledReason = "Protobuf format requires schema registry, not available in health check tests.")
public class ConnectorHealthCheckFailingConnectionIT {

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
    }

    @Test
    void livenessReportsDownWhenConnectorFails() {
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .untilAsserted(() -> given()
                        .when().get("/q/health/live")
                        .then()
                        .statusCode(503)
                        .body("status", equalTo("DOWN"))
                        .body("checks.name", hasItem("debezium"))
                        .body("checks.find { it.name == 'debezium' }.status", equalTo("DOWN")));
    }

    @Test
    void readinessReportsDownWhenConnectorFails() {
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .untilAsserted(() -> given()
                        .when().get("/q/health/ready")
                        .then()
                        .statusCode(503)
                        .body("status", equalTo("DOWN"))
                        .body("checks.name", hasItem("debezium"))
                        .body("checks.find { it.name == 'debezium' }.status", equalTo("DOWN")));
    }

}
