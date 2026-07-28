/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.JmxUtils;

/**
 * JMX metrics for the Databricks Zerobus sink. Counters are updated on the record-processing path
 * and exposed under {@code debezium.zerobus:type=connector-metrics,context=sink,server=<route>,task=0},
 * consistent with how the JDBC sink connector and Debezium source connectors expose their metrics.
 * <p>
 * The counters are deliberately self-contained and do not implement the shared
 * {@code io.debezium.sink.spi.SinkProgressListener} SPI, mirroring how {@code ZerobusTypeSystem}
 * owns its mapping rather than importing {@code io.debezium.sink.*}: this keeps the sink free of the
 * {@code debezium-sink} module dependency. If dbz#2300 lands and that SPI becomes the canonical
 * deps-free contract for Debezium Server sinks, these metrics are a natural candidate to migrate
 * onto it in a follow-up.
 */
@ThreadSafe
public class ZerobusSinkMetrics implements ZerobusSinkMetricsMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZerobusSinkMetrics.class);

    private static final String JMX_OBJECT_NAME_FORMAT = "debezium.zerobus:type=connector-metrics,context=sink,server=%s,task=%s";
    private static final String TASK_ID = "0";

    private final String route;
    private final ObjectName objectName;
    private final LongSupplier clock;

    private final AtomicLong totalRecordsIngested = new AtomicLong();
    private final AtomicLong totalRecordsSkipped = new AtomicLong();
    private final AtomicLong totalErrors = new AtomicLong();
    private final AtomicLong totalFlushes = new AtomicLong();
    private final AtomicLong totalInserts = new AtomicLong();
    private final AtomicLong totalUpdates = new AtomicLong();
    private final AtomicLong totalDeletes = new AtomicLong();
    private final AtomicLong totalReads = new AtomicLong();
    private final AtomicLong activeStreams = new AtomicLong();
    private final AtomicLong lastSourceTsMs = new AtomicLong(-1L);
    private final AtomicLong lastFlushDurationMillis = new AtomicLong();
    private final AtomicLong maxFlushDurationMillis = new AtomicLong();

    public ZerobusSinkMetrics(String route) {
        this(route, System::currentTimeMillis);
    }

    ZerobusSinkMetrics(String route, LongSupplier clock) {
        this.route = route;
        this.clock = clock;
        final String name = String.format(JMX_OBJECT_NAME_FORMAT, route, TASK_ID);
        try {
            this.objectName = new ObjectName(name);
        }
        catch (MalformedObjectNameException e) {
            throw new DebeziumException("Invalid metric name '" + name + "'", e);
        }
    }

    public void register() {
        JmxUtils.registerMXBean(objectName, this);
    }

    public void unregister() {
        JmxUtils.unregisterMXBean(objectName);
    }

    // -- instrumentation hooks (called from the consumers) --------------------------------------

    /**
     * Records one successfully forwarded change event.
     *
     * @param op          the Debezium operation ({@code c}/{@code u}/{@code d}/{@code r}), may be {@code null}
     * @param sourceTsMs  the event's {@code source.ts_ms}, or {@code -1} when unavailable
     */
    public void recordIngested(String op, long sourceTsMs) {
        totalRecordsIngested.incrementAndGet();
        if (op != null) {
            switch (op) {
                case "c":
                    totalInserts.incrementAndGet();
                    break;
                case "u":
                    totalUpdates.incrementAndGet();
                    break;
                case "d":
                    totalDeletes.incrementAndGet();
                    break;
                case "r":
                    totalReads.incrementAndGet();
                    break;
                default:
                    break;
            }
        }
        if (sourceTsMs >= 0) {
            lastSourceTsMs.set(sourceTsMs);
        }
    }

    /** Records one skipped record (tombstone / null payload / non-qualified destination). */
    public void recordSkipped() {
        totalRecordsSkipped.incrementAndGet();
    }

    /** Records one ingestion or flush failure. */
    public void recordError() {
        totalErrors.incrementAndGet();
    }

    /** Records a completed flush (gRPC) or record POST (REST) and its duration. */
    public void flushed(long durationMillis) {
        totalFlushes.incrementAndGet();
        lastFlushDurationMillis.set(durationMillis);
        maxFlushDurationMillis.accumulateAndGet(durationMillis, Math::max);
    }

    public void streamOpened() {
        activeStreams.incrementAndGet();
    }

    public void streamClosed() {
        activeStreams.updateAndGet(v -> v > 0 ? v - 1 : 0);
    }

    /** Emits a single INFO line summarizing the current counters — for periodic throughput logging. */
    public void logMetricsSummary() {
        LOGGER.info("Zerobus sink metrics [{}]: ingested={} (c={} u={} d={} r={}) skipped={} errors={} "
                + "flushes={} activeStreams={} behindSourceMs={} lastFlushMs={} maxFlushMs={}",
                route, getTotalRecordsIngested(), getTotalInserts(), getTotalUpdates(), getTotalDeletes(),
                getTotalReads(), getTotalRecordsSkipped(), getTotalErrors(), getTotalFlushes(),
                getActiveStreams(), getMilliSecondsBehindSource(), getLastFlushDurationMillis(),
                getMaxFlushDurationMillis());
    }

    // -- MXBean getters -------------------------------------------------------------------------

    @Override
    public long getTotalRecordsIngested() {
        return totalRecordsIngested.get();
    }

    @Override
    public long getTotalRecordsSkipped() {
        return totalRecordsSkipped.get();
    }

    @Override
    public long getTotalErrors() {
        return totalErrors.get();
    }

    @Override
    public long getTotalFlushes() {
        return totalFlushes.get();
    }

    @Override
    public long getTotalInserts() {
        return totalInserts.get();
    }

    @Override
    public long getTotalUpdates() {
        return totalUpdates.get();
    }

    @Override
    public long getTotalDeletes() {
        return totalDeletes.get();
    }

    @Override
    public long getTotalReads() {
        return totalReads.get();
    }

    @Override
    public long getActiveStreams() {
        return activeStreams.get();
    }

    @Override
    public long getMilliSecondsBehindSource() {
        final long ts = lastSourceTsMs.get();
        if (ts < 0) {
            return -1L;
        }
        return Math.max(0L, clock.getAsLong() - ts);
    }

    @Override
    public long getLastFlushDurationMillis() {
        return lastFlushDurationMillis.get();
    }

    @Override
    public long getMaxFlushDurationMillis() {
        return maxFlushDurationMillis.get();
    }

    @Override
    public String getRoute() {
        return route;
    }
}
