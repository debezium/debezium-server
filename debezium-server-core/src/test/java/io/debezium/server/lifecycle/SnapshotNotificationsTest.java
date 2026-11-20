/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

import io.debezium.pipeline.notification.Notification;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.smallrye.config.SmallRyeConfigBuilder;

class SnapshotNotificationsTest {

    @Test
    void shouldKeepTheServerNotificationsFromTheSink() {
        SnapshotNotifications notifications = SnapshotNotifications.from(config(Map.of(
                "debezium.shutdown.after.snapshot", "true",
                "quarkus.debezium.notification.sink.topic.name", SnapshotNotifications.INTERNAL_TOPIC)));
        BatchEvent row = event("row", new SourceRecord(Map.of(), Map.of(), "inventory.customers", Schema.STRING_SCHEMA, "row"));
        BatchEvent completed = event("completed", notification());

        SnapshotNotifications.Split split = notifications.split(events(row, completed));

        assertThat(split.delivered().records()).containsExactly(row);
        assertThat(split.delivered().engine()).isEqualTo("default");
        assertThat(split.notifications()).containsExactly(completed);
    }

    @Test
    void shouldDeliverTheNotificationsWhenTheUserEnabledTheSinkChannel() {
        SnapshotNotifications notifications = SnapshotNotifications.from(config(Map.of(
                "debezium.shutdown.after.snapshot", "true",
                "quarkus.debezium.notification.enabled.channels", "sink",
                "quarkus.debezium.notification.sink.topic.name", "inventory.notifications")));
        CapturingEvents<BatchEvent> events = events(event("completed", notification()));

        SnapshotNotifications.Split split = notifications.split(events);

        assertThat(split.delivered()).isSameAs(events);
        assertThat(split.notifications()).isEmpty();
    }

    @Test
    void shouldDeliverEverythingWithoutShutdownAfterSnapshot() {
        SnapshotNotifications notifications = SnapshotNotifications.from(config(Map.of(
                "quarkus.debezium.notification.sink.topic.name", SnapshotNotifications.INTERNAL_TOPIC)));
        CapturingEvents<BatchEvent> events = events(event("completed", notification()));

        assertThat(notifications.split(events).delivered()).isSameAs(events);
    }

    @Test
    void shouldRecognizeTheSinkChannel() {
        assertThat(SnapshotNotifications.hasSinkChannel("log, sink")).isTrue();
        assertThat(SnapshotNotifications.hasSinkChannel("log,jmx")).isFalse();
        assertThat(SnapshotNotifications.hasSinkChannel(null)).isFalse();
    }

    private static Config config(Map<String, String> properties) {
        return new SmallRyeConfigBuilder().withDefaultValues(properties).build();
    }

    private static CapturingEvents<BatchEvent> events(BatchEvent... records) {
        return new CapturingEvents<>() {
            @Override
            public List<BatchEvent> records() {
                return List.of(records);
            }

            @Override
            public String destination() {
                return null;
            }

            @Override
            public String source() {
                return "postgres";
            }

            @Override
            public String engine() {
                return "default";
            }
        };
    }

    private static BatchEvent event(String name, SourceRecord record) {
        return new BatchEvent() {
            @Override
            public Object key() {
                return null;
            }

            @Override
            public Object value() {
                return record.value();
            }

            @Override
            public Integer partition() {
                return null;
            }

            @Override
            public SourceRecord record() {
                return record;
            }

            @Override
            public String destination() {
                return record.topic();
            }

            @Override
            public void commit() {
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }

    /** Builds a notification record the way the {@code sink} notification channel does. */
    private static SourceRecord notification() {
        final Schema valueSchema = SchemaFactory.get().notificationValueSchema(SchemaNameAdjuster.NO_OP);
        final Struct value = new Struct(valueSchema)
                .put(Notification.ID_KEY, "id")
                .put(Notification.TYPE, "COMPLETED")
                .put(Notification.AGGREGATE_TYPE, "Initial Snapshot")
                .put(Notification.ADDITIONAL_DATA, Map.of())
                .put(Notification.TIMESTAMP, System.currentTimeMillis());
        return new SourceRecord(Map.of(), Map.of(), SnapshotNotifications.INTERNAL_TOPIC, valueSchema, value);
    }
}
