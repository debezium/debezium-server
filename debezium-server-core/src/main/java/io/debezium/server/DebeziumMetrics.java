/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server;

import java.lang.management.ManagementFactory;
import java.util.Objects;

import jakarta.enterprise.context.Dependent;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

/**
 * Reads debezium source pipeline metrics.
 * NOTE: calls for reading metrics should be made after debezium connector initialized,
 * after connector registers metrics, otherwise it will throw `Debezium Mbean not found` error
 *
 * @author Ismail Simsek
 */

@Dependent
public class DebeziumMetrics {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DebeziumMetrics.class);
    public static final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

    private ObjectName snapshotMetricsObjectName;
    private ObjectName snapshotPartitionMetricsObjectName;
    private ObjectName streamingMetricsObjectName;
    private ObjectName streamingPartitionMetricsObjectName;

    private static ObjectName getDebeziumMbean(String context, boolean partitioned) {
        ObjectName debeziumMbean = null;

        for (ObjectName mbean : mbeanServer.queryNames(null, null)) {

            if (mbean.getCanonicalName().contains("debezium.")
                    && mbean.getCanonicalName().contains("type=connector-metrics")
                    && mbean.getCanonicalName().contains("context=" + context)
                    && (mbean.getCanonicalName().contains("debezium.sql_server:")
                            ? mbean.getCanonicalName().contains("database=") == partitioned
                            : true)) {
                LOGGER.debug("Using {} MBean to get {} metrics", mbean, context);
                debeziumMbean = mbean;
                break;
            }

        }

        Objects.requireNonNull(debeziumMbean, "Debezium MBean (context=" + context + ") not found!");

        return debeziumMbean;
    }

    public ObjectName getSnapshotMetricsObjectName() {

        if (snapshotMetricsObjectName == null) {
            snapshotMetricsObjectName = getDebeziumMbean("snapshot", false);
        }

        return snapshotMetricsObjectName;
    }

    public ObjectName getSnapshotPartitionMetricsObjectName() {

        if (snapshotPartitionMetricsObjectName == null) {
            snapshotPartitionMetricsObjectName = getDebeziumMbean("snapshot", true);
        }

        return snapshotPartitionMetricsObjectName;
    }

    public ObjectName getStreamingMetricsObjectName() {

        if (streamingMetricsObjectName == null) {
            streamingMetricsObjectName = getDebeziumMbean("streaming", false);
        }

        return streamingMetricsObjectName;
    }

    public ObjectName getStreamingPartitionMetricsObjectName() {

        if (streamingPartitionMetricsObjectName == null) {
            streamingPartitionMetricsObjectName = getDebeziumMbean("streaming", true);
        }

        return streamingPartitionMetricsObjectName;
    }

    public int maxQueueSize() {
        try {
            return (int) mbeanServer.getAttribute(getStreamingMetricsObjectName(), "QueueTotalCapacity");
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public boolean snapshotRunning() {
        try {
            return (boolean) mbeanServer.getAttribute(getSnapshotPartitionMetricsObjectName(), "SnapshotRunning");
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public boolean snapshotCompleted() {
        try {
            return (boolean) mbeanServer.getAttribute(getSnapshotPartitionMetricsObjectName(), "SnapshotCompleted");
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public int streamingQueueRemainingCapacity() {
        try {
            return (int) mbeanServer.getAttribute(getStreamingMetricsObjectName(), "QueueRemainingCapacity");
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public int streamingQueueCurrentSize() {
        return maxQueueSize() - streamingQueueRemainingCapacity();
    }

    public long streamingMilliSecondsBehindSource() {
        try {
            return (long) mbeanServer.getAttribute(getStreamingPartitionMetricsObjectName(), "MilliSecondsBehindSource");
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public void logMetrics() {
        LOGGER.info("Debezium Metrics: snapshotCompleted={} snapshotRunning={} "
                + "streamingQueueCurrentSize={} streamingQueueRemainingCapacity={} maxQueueSize={} streamingMilliSecondsBehindSource={}",
                this.snapshotCompleted(),
                this.snapshotRunning(),
                this.streamingQueueCurrentSize(),
                this.streamingQueueRemainingCapacity(),
                this.maxQueueSize(),
                this.streamingMilliSecondsBehindSource());
    }
}
