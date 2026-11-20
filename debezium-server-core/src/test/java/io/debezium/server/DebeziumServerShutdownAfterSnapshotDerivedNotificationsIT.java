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
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Without any notification setting, {@code debezium.shutdown.after.snapshot=true} still stops the engine once the snapshot was
 * delivered: the server enables the {@code sink} channel by itself, and keeps those notifications from the sink.
 */
@QuarkusTest
@QuarkusTestResource(value = PostgresTestResourceLifecycleManager.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = FreshOffsetsResource.class, restrictToAnnotatedClass = true)
@TestProfile(ShutdownAfterSnapshotDerivedNotificationsProfile.class)
@EnabledIfSystemProperty(named = "test.apicurio", matches = "false", disabledReason = "Shutdown tests don't run with apicurio profile.")
@DisabledIfSystemProperty(named = "debezium.format.key", matches = "protobuf")
@DisabledIfSystemProperty(named = "debezium.format.value", matches = "protobuf")
public class DebeziumServerShutdownAfterSnapshotDerivedNotificationsIT {

    static final List<DebeziumCompletionEvent> COMPLETIONS = Collections.synchronizedList(new ArrayList<>());
    static final AtomicBoolean SNAPSHOT_STORED_AT_COMPLETION = new AtomicBoolean();

    @Inject
    TestConsumer testConsumer;

    void onCompletion(@Observes DebeziumCompletionEvent event) {
        // The offsets are stored before the completion callback runs: record what the file holds at that moment.
        try {
            SNAPSHOT_STORED_AT_COMPLETION.set(ShutdownAfterSnapshotSupport.completedSnapshotInOffsets());
        }
        catch (Exception e) {
            SNAPSHOT_STORED_AT_COMPLETION.set(false);
        }
        COMPLETIONS.add(event);
    }

    @Test
    void shouldStopAfterTheSnapshotWithoutDeliveringTheNotifications() throws Exception {
        Awaitility.await().atMost(Duration.ofMinutes(2)).until(() -> !COMPLETIONS.isEmpty());

        assertThat(COMPLETIONS).hasSize(1);
        assertThat(COMPLETIONS.get(0).isSuccess()).isTrue();
        final List<Object> values = List.copyOf(testConsumer.getValues());
        assertThat(values).hasSize((int) ShutdownAfterSnapshotSupport.expectedRows());
        assertThat(values).noneMatch(ShutdownAfterSnapshotSupport::isNotification);
        assertThat(SNAPSHOT_STORED_AT_COMPLETION.get()).isTrue();
    }
}
