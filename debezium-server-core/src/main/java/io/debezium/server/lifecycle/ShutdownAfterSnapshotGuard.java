/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

import io.debezium.DebeziumException;
import io.debezium.util.Strings;

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
     * Fails fast when {@code debezium.shutdown.after.snapshot} is enabled with a configuration in which it would not work as
     * documented:
     * <ul>
     * <li>a snapshot mode which keeps streaming would lose the stream, which the user has to ask for explicitly;</li>
     * <li>with several tasks or databases, the snapshot metrics and the queue of a single one are read, so the server could stop
     * while another one is still taking its snapshot;</li>
     * <li>without {@code debezium.shutdown.on.completion}, the engine would stop but the process would keep running.</li>
     * </ul>
     *
     * @param snapshotMode the value of {@code debezium.source.snapshot.mode}
     * @param tasksMax the value of {@code debezium.source.tasks.max}
     * @param databaseNames the value of {@code debezium.source.database.names}, or {@code null} if not set
     * @param shutdownOnCompletion the value of {@code debezium.shutdown.on.completion}
     */
    public static void requireSupportedConfiguration(String snapshotMode, int tasksMax, String databaseNames, boolean shutdownOnCompletion) {
        if (!"initial_only".equalsIgnoreCase(snapshotMode)) {
            throw new DebeziumException("'debezium.shutdown.after.snapshot' requires 'debezium.source.snapshot.mode=initial_only', "
                    + "but the snapshot mode is '" + snapshotMode + "'");
        }
        if (tasksMax > 1) {
            throw new DebeziumException("'debezium.shutdown.after.snapshot' supports a single task, but 'debezium.source.tasks.max' is " + tasksMax);
        }
        // Parsed like the connectors do, so a trailing or doubled comma does not count as another database.
        if (Strings.setOfTrimmed(databaseNames, name -> name.isEmpty() ? null : name).size() > 1) {
            throw new DebeziumException("'debezium.shutdown.after.snapshot' supports a single database, but 'debezium.source.database.names' is '"
                    + databaseNames + "'");
        }
        if (!shutdownOnCompletion) {
            throw new DebeziumException("'debezium.shutdown.after.snapshot' requires 'debezium.shutdown.on.completion=true', "
                    + "otherwise the process keeps running after the engine stops");
        }
    }
}
