/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.pipeline.notification.Notification;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;

class ShutdownAfterSnapshotGuardTest {

    private final ShutdownAfterSnapshotGuard guard = new ShutdownAfterSnapshotGuard();

    @Test
    void shouldRecognizeTheCompletedInitialSnapshot() {
        assertThat(guard.isEndOfInitialSnapshot(notification("Initial Snapshot", "COMPLETED"))).isTrue();
    }

    @Test
    void shouldRecognizeTheSkippedInitialSnapshot() {
        assertThat(guard.isEndOfInitialSnapshot(notification("Initial Snapshot", "SKIPPED"))).isTrue();
    }

    @Test
    void shouldIgnoreTheOtherInitialSnapshotNotifications() {
        assertThat(guard.isEndOfInitialSnapshot(notification("Initial Snapshot", "STARTED"))).isFalse();
        assertThat(guard.isEndOfInitialSnapshot(notification("Initial Snapshot", "IN_PROGRESS"))).isFalse();
        assertThat(guard.isEndOfInitialSnapshot(notification("Initial Snapshot", "TABLE_SCAN_COMPLETED"))).isFalse();
    }

    @Test
    void shouldIgnoreTheNotificationsOfOtherSnapshots() {
        assertThat(guard.isEndOfInitialSnapshot(notification("Incremental Snapshot", "COMPLETED"))).isFalse();
    }

    @Test
    void shouldIgnoreChangeEvents() {
        final Schema schema = SchemaBuilder.struct().name("testc.inventory.customers.Value").field("type", Schema.STRING_SCHEMA).build();
        final SourceRecord event = new SourceRecord(Map.of(), Map.of(), "testc.inventory.customers", schema, new Struct(schema).put("type", "COMPLETED"));

        assertThat(guard.isEndOfInitialSnapshot(event)).isFalse();
        assertThat(guard.isEndOfInitialSnapshot(null)).isFalse();
    }

    @Test
    void shouldAcceptInitialOnlyWithTheSinkChannelASingleTaskAndDatabase() {
        ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", "sink", 1, null, true);
        ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", "log, sink", 1, "inventory", true);
    }

    @Test
    void shouldAcceptTheSnapshotModeInAnyCase() {
        ShutdownAfterSnapshotGuard.requireSupportedConfiguration("INITIAL_ONLY", "sink", 1, null, true);
        ShutdownAfterSnapshotGuard.requireSupportedConfiguration("Initial_Only", "sink", 1, null, true);
    }

    @Test
    void shouldIgnoreEmptyItemsInDatabaseNames() {
        ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", "sink", 1, "inventory,", true);
        ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", "sink", 1, " inventory , , ", true);
    }

    @Test
    void shouldRejectModesWhichStream() {
        assertThatThrownBy(() -> ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial", "sink", 1, null, true))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("initial_only");
    }

    @Test
    void shouldRequireTheSinkNotificationChannel() {
        assertThatThrownBy(() -> ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", null, 1, null, true))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("notification.enabled.channels");
        assertThatThrownBy(() -> ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", "log,jmx", 1, null, true))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("notification.enabled.channels");
    }

    @Test
    void shouldRejectSeveralTasks() {
        assertThatThrownBy(() -> ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", "sink", 2, null, true))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("tasks.max");
    }

    @Test
    void shouldRejectSeveralDatabases() {
        assertThatThrownBy(() -> ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", "sink", 1, "db1,db2", true))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("database.names");
    }

    @Test
    void shouldRequireShutdownOnCompletion() {
        assertThatThrownBy(() -> ShutdownAfterSnapshotGuard.requireSupportedConfiguration("initial_only", "sink", 1, null, false))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("shutdown.on.completion");
    }

    /** Builds a notification record the way the {@code sink} notification channel does. */
    private static SourceRecord notification(String aggregateType, String type) {
        final Schema valueSchema = SchemaFactory.get().notificationValueSchema(SchemaNameAdjuster.NO_OP);
        final Struct value = new Struct(valueSchema)
                .put(Notification.ID_KEY, "id")
                .put(Notification.TYPE, type)
                .put(Notification.AGGREGATE_TYPE, aggregateType)
                .put(Notification.ADDITIONAL_DATA, Map.of())
                .put(Notification.TIMESTAMP, System.currentTimeMillis());
        return new SourceRecord(Map.of(), Map.of(), "testc.notifications", valueSchema, value);
    }
}
