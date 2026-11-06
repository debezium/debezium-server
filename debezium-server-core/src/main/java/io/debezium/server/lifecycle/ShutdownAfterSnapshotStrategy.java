/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import static io.debezium.server.configuration.DebeziumProperties.PROP_SOURCE_PREFIX;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.DebeziumShutdown;
import io.debezium.runtime.DebeziumShutdown.Phase;
import io.debezium.server.configuration.DebeziumServerConfig;
import io.quarkus.runtime.Startup;

/**
 * Stops the engine once the initial snapshot has been delivered to the sink, when {@code debezium.shutdown.after.snapshot} is enabled.
 * <p>
 * The engine evaluates the method after the sink handled the batch that contains the record, so the offsets of the delivered
 * records are committed before the engine stops. The server then exits through {@code debezium.shutdown.on.completion}.
 * The bean always exists: with the option disabled, the method never stops the engine.
 */
@ApplicationScoped
@Startup
public class ShutdownAfterSnapshotStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownAfterSnapshotStrategy.class);

    private final boolean enabled;
    private final ShutdownAfterSnapshotGuard guard;

    @Inject
    public ShutdownAfterSnapshotStrategy(DebeziumServerConfig serverConfig, Config config) {
        this(serverConfig.shutdownAfterSnapshot(), new ShutdownAfterSnapshotGuard());
        if (enabled) {
            ShutdownAfterSnapshotGuard.requireSupportedConfiguration(
                    config.getOptionalValue(PROP_SOURCE_PREFIX + "snapshot.mode", String.class).orElse("initial"),
                    config.getOptionalValue(PROP_SOURCE_PREFIX + "notification.enabled.channels", String.class).orElse(null),
                    config.getOptionalValue(PROP_SOURCE_PREFIX + "tasks.max", Integer.class).orElse(1),
                    config.getOptionalValue(PROP_SOURCE_PREFIX + "database.names", String.class).orElse(null),
                    serverConfig.shutdownOnCompletion());
        }
    }

    ShutdownAfterSnapshotStrategy(boolean enabled, ShutdownAfterSnapshotGuard guard) {
        this.enabled = enabled;
        this.guard = guard;
    }

    /**
     * @param record a record of a batch that the sink has handled
     * @return {@code true} when the engine has to stop, which is the case for the notification that ends the initial snapshot
     */
    @DebeziumShutdown(Phase.AFTER)
    public boolean stopAfterInitialSnapshot(SourceRecord record) {
        if (!enabled || !guard.isEndOfInitialSnapshot(record)) {
            return false;
        }
        LOGGER.info("Initial snapshot delivered to the sink, stopping the engine");
        return true;
    }
}
