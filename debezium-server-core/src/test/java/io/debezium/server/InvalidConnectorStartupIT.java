/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.quarkus.test.LogCollectingTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Verifies that an invalid connector class causes a clean startup failure with ClassNotFoundException,
 * not an infinite retry loop or StackOverflowError (DBZ-8703).
 */
@QuarkusTest
@TestProfile(InvalidConnectorTestProfile.class)
@QuarkusTestResource(value = LogCollectingTestResource.class, restrictToAnnotatedClass = true, initArgs = {
        @ResourceArg(name = LogCollectingTestResource.INCLUDE, value = "io\\.debezium\\..*"),
})
public class InvalidConnectorStartupIT {

    @Test
    public void shouldFailCleanlyWithInvalidConnectorClass() {
        Awaitility.await()
                .atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .until(() -> LogCollectingTestResource.current()
                        .getRecords()
                        .stream()
                        .anyMatch(r -> r.getMessage() != null && r.getMessage().contains("Connector completed")));

        boolean hasClassNotFound = LogCollectingTestResource.current()
                .getRecords()
                .stream()
                .anyMatch(r -> r.getMessage() != null && r.getMessage().contains("ClassNotFoundException"));

        boolean hasStackOverflow = LogCollectingTestResource.current()
                .getRecords()
                .stream()
                .anyMatch(r -> r.getMessage() != null && r.getMessage().contains("StackOverflowError"));

        assertThat(hasClassNotFound)
                .as("Expected ClassNotFoundException in logs")
                .isTrue();
        assertThat(hasStackOverflow)
                .as("Expected no StackOverflowError in logs")
                .isFalse();
    }
}
