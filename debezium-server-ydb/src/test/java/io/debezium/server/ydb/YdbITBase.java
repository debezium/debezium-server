/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import jakarta.enterprise.event.Observes;

import org.awaitility.Awaitility;

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.util.Testing;

public abstract class YdbITBase {

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        Testing.Print.enable();
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) {
        if (!event.isSuccess()) {
            throw new RuntimeException(event.getError().orElseThrow());
        }
    }

    protected void assertOffsetsPersisted() {
        Awaitility.await()
                .atMost(Duration.ofSeconds(YdbTestConfigSource.waitForSeconds()))
                .ignoreExceptions()
                .untilAsserted(() -> assertThat(offsetRowCount()).isGreaterThan(0));
    }

    private static long offsetRowCount() throws Exception {
        return TestUtils.jdbcRowCount(
                YdbTestConfigSource.JDBC_URL,
                YdbTestResourceLifecycleManager.JDBC_USER,
                YdbTestResourceLifecycleManager.JDBC_PASSWORD,
                YdbTestConfigSource.OFFSET_TABLE);
    }
}
