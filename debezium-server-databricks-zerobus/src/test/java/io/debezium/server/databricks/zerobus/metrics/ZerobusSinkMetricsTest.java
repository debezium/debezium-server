/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ZerobusSinkMetrics}. They need no Zerobus SDK, connection or native library,
 * so they run in any CI environment.
 */
class ZerobusSinkMetricsTest {

    @Test
    void ingestedSplitsByOperation() {
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc");
        m.recordIngested("c", -1);
        m.recordIngested("c", -1);
        m.recordIngested("u", -1);
        m.recordIngested("d", -1);
        m.recordIngested("r", -1);

        assertThat(m.getTotalRecordsIngested()).isEqualTo(5);
        assertThat(m.getTotalInserts()).isEqualTo(2);
        assertThat(m.getTotalUpdates()).isEqualTo(1);
        assertThat(m.getTotalDeletes()).isEqualTo(1);
        assertThat(m.getTotalReads()).isEqualTo(1);
    }

    @Test
    void unknownOrNullOperationStillCountsAsIngested() {
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc");
        m.recordIngested(null, -1);
        m.recordIngested("t", -1); // truncate op is not tracked per-op

        assertThat(m.getTotalRecordsIngested()).isEqualTo(2);
        assertThat(m.getTotalInserts()).isZero();
        assertThat(m.getTotalUpdates()).isZero();
        assertThat(m.getTotalDeletes()).isZero();
        assertThat(m.getTotalReads()).isZero();
    }

    @Test
    void skippedAndErrorCountersIncrement() {
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("rest");
        m.recordSkipped();
        m.recordSkipped();
        m.recordError();

        assertThat(m.getTotalRecordsSkipped()).isEqualTo(2);
        assertThat(m.getTotalErrors()).isEqualTo(1);
        assertThat(m.getTotalRecordsIngested()).isZero();
    }

    @Test
    void freshnessIsNowMinusLastSourceTimestamp() {
        AtomicLong now = new AtomicLong(10_000L);
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc", now::get);

        // before any timestamped event, freshness is unknown
        assertThat(m.getMilliSecondsBehindSource()).isEqualTo(-1);

        m.recordIngested("c", 8_500L);
        assertThat(m.getMilliSecondsBehindSource()).isEqualTo(1_500L);

        // a newer event reduces the lag
        m.recordIngested("u", 9_800L);
        assertThat(m.getMilliSecondsBehindSource()).isEqualTo(200L);
    }

    @Test
    void freshnessNeverGoesNegativeIfClockSkews() {
        AtomicLong now = new AtomicLong(1_000L);
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc", now::get);
        m.recordIngested("c", 2_000L); // source ts ahead of local clock (skew)
        assertThat(m.getMilliSecondsBehindSource()).isZero();
    }

    @Test
    void ingestWithoutTimestampDoesNotResetFreshness() {
        AtomicLong now = new AtomicLong(5_000L);
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc", now::get);
        m.recordIngested("c", 4_000L);
        assertThat(m.getMilliSecondsBehindSource()).isEqualTo(1_000L);

        // a record with no source ts (-1) must leave the last known freshness intact
        m.recordIngested("c", -1);
        assertThat(m.getMilliSecondsBehindSource()).isEqualTo(1_000L);
    }

    @Test
    void flushTracksCountLastAndMaxDuration() {
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc");
        m.flushed(10);
        m.flushed(30);
        m.flushed(20);

        assertThat(m.getTotalFlushes()).isEqualTo(3);
        assertThat(m.getLastFlushDurationMillis()).isEqualTo(20);
        assertThat(m.getMaxFlushDurationMillis()).isEqualTo(30);
    }

    @Test
    void activeStreamsIncrementsAndDecrementsWithoutGoingNegative() {
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc");
        m.streamOpened();
        m.streamOpened();
        assertThat(m.getActiveStreams()).isEqualTo(2);

        m.streamClosed();
        assertThat(m.getActiveStreams()).isEqualTo(1);

        m.streamClosed();
        m.streamClosed(); // extra close must not underflow
        assertThat(m.getActiveStreams()).isZero();
    }

    @Test
    void routeIsExposed() {
        assertThat(new ZerobusSinkMetrics("grpc").getRoute()).isEqualTo("grpc");
        assertThat(new ZerobusSinkMetrics("rest").getRoute()).isEqualTo("rest");
    }

    @Test
    void registerExposesMBeanUnderConventionalObjectNameThenUnregisters() throws Exception {
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc");
        ObjectName name = new ObjectName("debezium.zerobus:type=connector-metrics,context=sink,server=grpc,task=0");
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            m.register();
            assertThat(server.isRegistered(name)).isTrue();
            assertThat((String) server.getAttribute(name, "Route")).isEqualTo("grpc");
        }
        finally {
            m.unregister();
        }
        assertThat(server.isRegistered(name)).isFalse();
    }
}
