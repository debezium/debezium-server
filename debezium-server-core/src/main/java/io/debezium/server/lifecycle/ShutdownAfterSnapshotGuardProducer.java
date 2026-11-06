/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Produces;

import io.debezium.server.DebeziumMetrics;
import io.quarkus.arc.properties.IfBuildProperty;

/**
 * Builds the {@link ShutdownAfterSnapshotGuard} from the Debezium JMX metrics.
 * <p>
 * Like {@link DebeziumMetrics}, it exists only in JVM mode. Keeping the reference to {@link DebeziumMetrics} out of
 * the change consumer leaves that class unreachable in a native image, where its platform MBean server is not supported.
 */
@Dependent
@IfBuildProperty(name = "quarkus.native.enabled", stringValue = "false")
public class ShutdownAfterSnapshotGuardProducer {

    @Produces
    @Dependent
    ShutdownAfterSnapshotGuard shutdownAfterSnapshotGuard(DebeziumMetrics metrics) {
        return new ShutdownAfterSnapshotGuard(metrics::snapshotCompleted, metrics::streamingQueueCurrentSize);
    }
}
