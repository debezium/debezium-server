/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Negative case: with the option off, the snapshot is delivered but the engine keeps running.
 */
@QuarkusTest
@QuarkusTestResource(value = PostgresTestResourceLifecycleManager.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = FreshOffsetsResource.class, restrictToAnnotatedClass = true)
@TestProfile(ShutdownAfterSnapshotDisabledProfile.class)
@EnabledIfSystemProperty(named = "test.apicurio", matches = "false", disabledReason = "Shutdown tests don't run with apicurio profile.")
public class DebeziumServerShutdownAfterSnapshotDisabledIT {

    static final List<DebeziumCompletionEvent> COMPLETIONS = Collections.synchronizedList(new ArrayList<>());

    @Inject
    TestConsumer testConsumer;

    void onCompletion(@Observes DebeziumCompletionEvent event) {
        COMPLETIONS.add(event);
    }

    @Test
    void shouldKeepRunningAfterTheSnapshot() throws Exception {
        final long expectedRows = ShutdownAfterSnapshotSupport.expectedRows();
        Awaitility.await().atMost(Duration.ofMinutes(2)).until(() -> testConsumer.getValues().size() == expectedRows);

        Awaitility.await().during(Duration.ofSeconds(15)).atMost(Duration.ofSeconds(30)).until(() -> COMPLETIONS.isEmpty());
        assertThat(COMPLETIONS).isEmpty();
    }
}
