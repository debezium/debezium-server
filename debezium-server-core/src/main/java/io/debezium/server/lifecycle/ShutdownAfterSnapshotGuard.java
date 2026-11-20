/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.DebeziumException;
import io.debezium.pipeline.notification.InitialSnapshotNotificationService;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.SnapshotStatus;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.SchemaFactory;
import io.debezium.util.Strings;

/**
 * Recognizes the notification that ends an {@code initial_only} snapshot, so the engine can be stopped once it has been delivered.
 * <p>
 * With the {@code sink} notification channel, the connector enqueues each notification as a record in the same queue as the
 * change events. The initial snapshot {@code COMPLETED} notification is enqueued after the last snapshot event, and it carries
 * the offset of the completed snapshot, so when it reaches the consumer every snapshot event has been handed to the sink
 * before it. On a restart after a completed snapshot, the connector sends {@code SKIPPED} instead, through the same path.
 */
public class ShutdownAfterSnapshotGuard {

    private static final String SINK_CHANNEL = "sink";
    private static final Set<String> FINAL_STATUSES = Set.of(SnapshotStatus.COMPLETED.name(), SnapshotResult.SnapshotResultStatus.SKIPPED.name());

    /**
     * @param record a record of a batch that the sink has handled
     * @return {@code true} if the record is the notification that the initial snapshot was completed or skipped
     */
    public boolean isEndOfInitialSnapshot(SourceRecord record) {
        if (record == null || !SchemaFactory.get().isNotificationSchema(record.valueSchema()) || !(record.value() instanceof Struct)) {
            return false;
        }
        final Struct notification = (Struct) record.value();
        return InitialSnapshotNotificationService.INITIAL_SNAPSHOT.equals(notification.getString(Notification.AGGREGATE_TYPE))
                && FINAL_STATUSES.contains(notification.getString(Notification.TYPE));
    }

    /**
     * Fails fast when {@code debezium.shutdown.after.snapshot} is enabled with a configuration in which it would not work as
     * documented:
     * <ul>
     * <li>a snapshot mode which keeps streaming would lose the stream, which the user has to ask for explicitly;</li>
     * <li>without the {@code sink} notification channel, the notification that ends the snapshot never reaches the consumer;</li>
     * <li>with several tasks or databases, the first one to finish its snapshot would stop the server while another one is
     * still taking its snapshot;</li>
     * <li>without {@code debezium.shutdown.on.completion}, the engine would stop but the process would keep running.</li>
     * </ul>
     *
     * @param snapshotMode the value of {@code debezium.source.snapshot.mode}
     * @param notificationChannels the value of {@code debezium.source.notification.enabled.channels}, or {@code null} if not set
     * @param tasksMax the value of {@code debezium.source.tasks.max}
     * @param databaseNames the value of {@code debezium.source.database.names}, or {@code null} if not set
     * @param shutdownOnCompletion the value of {@code debezium.shutdown.on.completion}
     */
    public static void requireSupportedConfiguration(String snapshotMode, String notificationChannels, int tasksMax, String databaseNames,
                                                     boolean shutdownOnCompletion) {
        if (!"initial_only".equalsIgnoreCase(snapshotMode)) {
            throw new DebeziumException("'debezium.shutdown.after.snapshot' requires 'debezium.source.snapshot.mode=initial_only', "
                    + "but the snapshot mode is '" + snapshotMode + "'");
        }
        if (!Strings.setOfTrimmed(notificationChannels, channel -> channel.isEmpty() ? null : channel).contains(SINK_CHANNEL)) {
            throw new DebeziumException("'debezium.shutdown.after.snapshot' requires the 'sink' notification channel in "
                    + "'debezium.source.notification.enabled.channels', together with 'debezium.source.notification.sink.topic.name'");
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
