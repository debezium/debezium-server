/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

import io.debezium.DebeziumException;

/**
 * Decides when an {@code initial_only} snapshot has been fully delivered, so the engine can be stopped.
 * <p>
 * The snapshot completes only after its last record was enqueued, and {@code initial_only} never streams, so no record is
 * enqueued after completion. Therefore, if the snapshot is completed and the queue is empty, every record was already taken
 * from the queue, and the consumer is the only thread taking records. Calling this from the consumer, after the batch was
 * delivered, means nothing is pending and nothing is in flight.
 */
public class ShutdownAfterSnapshotGuard {

    private final BooleanSupplier snapshotCompleted;
    private final IntSupplier queueSize;

    public ShutdownAfterSnapshotGuard(BooleanSupplier snapshotCompleted, IntSupplier queueSize) {
        this.snapshotCompleted = snapshotCompleted;
        this.queueSize = queueSize;
    }

    /**
     * @return {@code true} when the snapshot completed and no record is left in the queue. The snapshot flag is read first:
     *         reading the queue before it could miss records enqueued while the snapshot was still running.
     */
    public boolean isSnapshotDelivered() {
        final boolean completed = snapshotCompleted.getAsBoolean();
        return completed && queueSize.getAsInt() == 0;
    }

    /**
     * Fails fast when {@code debezium.shutdown.after.snapshot} is enabled with a snapshot mode which keeps streaming. Such a mode
     * would lose the stream, which the user has to ask for explicitly.
     *
     * @param snapshotMode the value of {@code debezium.source.snapshot.mode}
     */
    public static void requireInitialOnly(String snapshotMode) {
        if (!"initial_only".equals(snapshotMode)) {
            throw new DebeziumException("'debezium.shutdown.after.snapshot' requires 'debezium.source.snapshot.mode=initial_only', "
                    + "but the snapshot mode is '" + snapshotMode + "'");
        }
    }
}
