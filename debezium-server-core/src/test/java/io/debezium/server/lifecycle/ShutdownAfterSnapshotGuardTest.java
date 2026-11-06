/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

class ShutdownAfterSnapshotGuardTest {

    @Test
    void shouldNotBeDeliveredWhileSnapshotIsRunning() {
        final ShutdownAfterSnapshotGuard guard = new ShutdownAfterSnapshotGuard(() -> false, () -> 0);

        assertThat(guard.isSnapshotDelivered()).isFalse();
    }

    @Test
    void shouldNotBeDeliveredWhileRecordsRemainInTheQueue() {
        final ShutdownAfterSnapshotGuard guard = new ShutdownAfterSnapshotGuard(() -> true, () -> 3);

        assertThat(guard.isSnapshotDelivered()).isFalse();
    }

    @Test
    void shouldBeDeliveredWhenSnapshotCompletedAndQueueIsEmpty() {
        final ShutdownAfterSnapshotGuard guard = new ShutdownAfterSnapshotGuard(() -> true, () -> 0);

        assertThat(guard.isSnapshotDelivered()).isTrue();
    }

    @Test
    void shouldReadSnapshotFlagBeforeQueue() {
        // The flag must be read first: a queue read before the snapshot completed could be empty while records are still to come.
        final List<String> reads = new ArrayList<>();
        final ShutdownAfterSnapshotGuard guard = new ShutdownAfterSnapshotGuard(() -> {
            reads.add("snapshot");
            return true;
        }, () -> {
            reads.add("queue");
            return 0;
        });

        guard.isSnapshotDelivered();

        assertThat(reads).containsExactly("snapshot", "queue");
    }

    @Test
    void shouldAcceptInitialOnlyWithASingleTaskAndDatabase() {
        ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", 1, null, true);
        ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", 1, "inventory", true);
    }

    @Test
    void shouldRejectModesWhichStream() {
        assertThatThrownBy(() -> ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial", 1, null, true))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("initial_only");
    }

    @Test
    void shouldRejectSeveralTasks() {
        assertThatThrownBy(() -> ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", 2, null, true))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("tasks.max");
    }

    @Test
    void shouldRejectSeveralDatabases() {
        assertThatThrownBy(() -> ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", 1, "db1,db2", true))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("database.names");
    }

    @Test
    void shouldRequireShutdownOnCompletion() {
        assertThatThrownBy(() -> ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", 1, null, false))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("shutdown.on.completion");
    }
}
