/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
        assertThat(new ZerobusSinkMetrics("kafka").getRoute()).isEqualTo("kafka");
    }

    @Test
    void timeSinceLastEventIsUnknownUntilTheFirstEvent() {
        AtomicLong now = new AtomicLong(10_000L);
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc", now::get);

        assertThat(m.getMilliSecondsSinceLastEvent()).isEqualTo(-1L);

        m.recordIngested("c", -1);
        assertThat(m.getMilliSecondsSinceLastEvent()).isZero();

        // The gauge grows while the sink forwards nothing, which is what distinguishes a stalled
        // pipeline from a merely idle source.
        now.set(12_500L);
        assertThat(m.getMilliSecondsSinceLastEvent()).isEqualTo(2_500L);

        // A new event resets it, even when the event carries no source timestamp.
        m.recordIngested("u", -1);
        assertThat(m.getMilliSecondsSinceLastEvent()).isZero();
    }

    @Test
    void timeSinceLastEventIsIndependentOfFreshness() {
        AtomicLong now = new AtomicLong(10_000L);
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc", now::get);
        m.recordIngested("c", 9_000L);

        now.set(20_000L);
        // Both grow when the pipeline stalls, but they answer different questions: how old the last
        // applied change is at the source, versus how long the sink has been silent.
        assertThat(m.getMilliSecondsBehindSource()).isEqualTo(11_000L);
        assertThat(m.getMilliSecondsSinceLastEvent()).isEqualTo(10_000L);
    }

    @Test
    void connectedReflectsTheSinkLifecycle() {
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc");
        assertThat(m.isConnected()).isFalse();

        m.setConnected(true);
        assertThat(m.isConnected()).isTrue();

        m.setConnected(false);
        assertThat(m.isConnected()).isFalse();
    }

    @Test
    void countersHoldUnderConcurrentUpdates() throws Exception {
        // The class is annotated @ThreadSafe: the gRPC route updates the counters from the batch
        // thread while flushes complete elsewhere, and on the Kafka route sends and acknowledgements
        // run on different threads altogether.
        ZerobusSinkMetrics m = new ZerobusSinkMetrics("grpc");
        int threads = 8;
        int perThread = 1_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    m.recordIngested("c", System.currentTimeMillis());
                    m.recordSkipped();
                    m.recordError();
                    m.flushed(i);
                    m.streamOpened();
                    m.streamClosed();
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<?> future : futures) {
            future.get(60, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        long expected = (long) threads * perThread;
        assertThat(m.getTotalRecordsIngested()).isEqualTo(expected);
        assertThat(m.getTotalInserts()).isEqualTo(expected);
        assertThat(m.getTotalRecordsSkipped()).isEqualTo(expected);
        assertThat(m.getTotalErrors()).isEqualTo(expected);
        assertThat(m.getTotalFlushes()).isEqualTo(expected);
        assertThat(m.getMaxFlushDurationMillis()).isEqualTo(perThread - 1);
        assertThat(m.getActiveStreams()).isZero();
    }

    @Test
    void logMetricsSummaryDoesNotFailOnAnUntouchedInstance() {
        assertThatCode(() -> new ZerobusSinkMetrics("kafka").logMetricsSummary()).doesNotThrowAnyException();
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
