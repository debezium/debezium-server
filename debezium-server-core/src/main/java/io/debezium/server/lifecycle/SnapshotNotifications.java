/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import static io.debezium.server.configuration.DebeziumProperties.PROP_SHUTDOWN_AFTER_SNAPSHOT;
import static io.debezium.server.configuration.DebeziumProperties.QUARKUS_DEBEZIUM_PREFIX;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.microprofile.config.Config;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.schema.SchemaFactory;
import io.debezium.util.Strings;

/**
 * The notifications of the {@code sink} channel that {@code debezium.shutdown.after.snapshot} relies on.
 * <p>
 * When the user didn't enable the {@code sink} channel, the server enables it for the option, with an internal topic, and keeps
 * those notifications from the sink: they are only there to stop the engine. When the user enabled the channel, the
 * notifications are delivered as usual.
 */
public final class SnapshotNotifications {

    public static final String NOTIFICATION_CHANNELS = "notification.enabled.channels";
    public static final String NOTIFICATION_TOPIC = "notification.sink.topic.name";
    public static final String SINK_CHANNEL = "sink";
    public static final String INTERNAL_TOPIC = "__debezium-server-snapshot-notifications";

    private static final SnapshotNotifications DELIVER = new SnapshotNotifications(false);

    private final boolean dropped;

    private SnapshotNotifications(boolean dropped) {
        this.dropped = dropped;
    }

    /**
     * The server enabled the {@code sink} channel for itself when the engine sends the notifications to the internal topic.
     *
     * @param config the server configuration
     * @return the handling of the notifications for this configuration
     */
    public static SnapshotNotifications from(Config config) {
        final boolean shutdownAfterSnapshot = config.getOptionalValue(PROP_SHUTDOWN_AFTER_SNAPSHOT, Boolean.class).orElse(false);
        final String topic = config.getOptionalValue(QUARKUS_DEBEZIUM_PREFIX + NOTIFICATION_TOPIC, String.class).orElse(null);
        if (!shutdownAfterSnapshot || !INTERNAL_TOPIC.equals(topic)) {
            return DELIVER;
        }
        return new SnapshotNotifications(true);
    }

    /**
     * @return {@code true} if the server enabled the {@code sink} channel for itself, so its notifications don't reach the sink
     */
    public boolean enabledByServer() {
        return dropped;
    }

    /**
     * @param channels a comma-separated list of notification channels, or {@code null}
     * @return {@code true} if the list contains the {@code sink} channel
     */
    public static boolean hasSinkChannel(String channels) {
        return Strings.setOfTrimmed(channels, channel -> channel.isEmpty() ? null : channel).contains(SINK_CHANNEL);
    }

    /**
     * Splits a batch into the events for the sink and the notifications that the server enabled for itself.
     *
     * @param events the batch handed to the consumer
     * @return the batch without the server's notifications, and those notifications
     */
    public Split split(CapturingEvents<BatchEvent> events) {
        if (!dropped) {
            return new Split(events, List.of());
        }

        final List<BatchEvent> delivered = new ArrayList<>(events.records().size());
        final List<BatchEvent> notifications = new ArrayList<>();
        for (BatchEvent event : events.records()) {
            if (event.record() != null && SchemaFactory.get().isNotificationSchema(event.record().valueSchema())) {
                notifications.add(event);
            }
            else {
                delivered.add(event);
            }
        }

        if (notifications.isEmpty()) {
            return new Split(events, List.of());
        }
        return new Split(new FilteredEvents(events, delivered), notifications);
    }

    /**
     * @param delivered the events for the sink
     * @param notifications the notifications kept from the sink; they still have to be committed
     */
    public record Split(CapturingEvents<BatchEvent> delivered, List<BatchEvent> notifications) {
    }

    private record FilteredEvents(CapturingEvents<BatchEvent> events, List<BatchEvent> records) implements CapturingEvents<BatchEvent> {

        @Override
        public String destination() {
            return events.destination();
        }

        @Override
        public String source() {
            return events.source();
        }

        @Override
        public String engine() {
            return events.engine();
        }
    }
}
