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
    void shouldAcceptInitialOnly() {
        ShutdownAfterSnapshotGuard.requireInitialOnly("initial_only");
    }

    @Test
    void shouldRejectModesWhichStream() {
        assertThatThrownBy(() -> ShutdownAfterSnapshotGuard.requireInitialOnly("initial"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("requires 'debezium.source.snapshot.mode=initial_only'")
                .hasMessageContaining("'initial'");
    }
}
