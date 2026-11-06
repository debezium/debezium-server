/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import io.debezium.server.databricks.zerobus.metrics.ZerobusSinkMetrics;

/**
 * Makes a long wait for Zerobus to acknowledge a batch visible while it lasts.
 * <p>
 * The sink does not commit the source offsets of a batch until Zerobus has acknowledged its records,
 * so a slow acknowledgement looks like an idle sink unless something reports it. This monitor marks the
 * wait in {@link ZerobusSinkMetrics} (exposed over JMX as {@code CurrentFlushDurationMillis}) and, once
 * the wait has lasted {@code warnThresholdMs}, logs a WARN with the target table, the number of records
 * still awaiting acknowledgement and the elapsed time. The WARN repeats at the same interval for as long
 * as the wait continues. A wait that ran past the threshold and then completed is reported at INFO with
 * its total duration.
 * <p>
 * The periodic reporting runs on a single daemon thread that is created on the first watched wait and
 * stopped by {@link #close()}.
 */
final class ZerobusAckWaitMonitor implements AutoCloseable {

    /** An acknowledgement the sink waits for, such as a stream flush or a wait for one offset. */
    @FunctionalInterface
    interface Acknowledgement {
        void await() throws Exception;
    }

    private final ZerobusSinkMetrics metrics;
    private final Logger logger;
    private ScheduledExecutorService watchdog;

    ZerobusAckWaitMonitor(ZerobusSinkMetrics metrics, Logger logger) {
        this.metrics = metrics;
        this.logger = logger;
    }

    /**
     * Runs one acknowledgement wait for {@code table} while watching it.
     *
     * @param table the target table whose stream is being waited on
     * @param pendingRecords the number of records handed to the stream since its last acknowledgement
     * @param warnThresholdMs the wait after which the wait is logged, repeated at the same interval;
     *            {@code 0} or a negative value disables the logging. The JMX duration is reported regardless.
     * @param acknowledgement the wait itself
     * @throws Exception whatever {@code acknowledgement} throws; the wait is unmarked in all cases
     */
    void await(String table, int pendingRecords, long warnThresholdMs, Acknowledgement acknowledgement) throws Exception {
        final long startNanos = System.nanoTime();
        final AtomicBoolean warned = new AtomicBoolean();
        metrics.ackWaitStarted();
        final ScheduledFuture<?> warning = warnThresholdMs > 0
                ? scheduler().scheduleAtFixedRate(() -> logStillPending(table, pendingRecords, startNanos, warnThresholdMs, warned),
                        warnThresholdMs, warnThresholdMs, TimeUnit.MILLISECONDS)
                : null;
        try {
            acknowledgement.await();
        }
        finally {
            if (warning != null) {
                warning.cancel(false);
            }
            metrics.ackWaitEnded();
        }
        if (warned.get()) {
            logger.info("Zerobus acknowledged the {} records for table '{}' after {} ms",
                    pendingRecords, table, elapsedMs(startNanos));
        }
    }

    private void logStillPending(String table, int pendingRecords, long startNanos, long warnThresholdMs, AtomicBoolean warned) {
        warned.set(true);
        logger.warn("Zerobus has not acknowledged the {} records for table '{}' after {} ms. The offsets of this batch "
                + "are not committed until it is acknowledged (threshold: debezium.sink.zerobus.flush.warn.threshold.ms={})",
                pendingRecords, table, elapsedMs(startNanos), warnThresholdMs);
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    private synchronized ScheduledExecutorService scheduler() {
        if (watchdog == null) {
            watchdog = Executors.newSingleThreadScheduledExecutor(runnable -> {
                final Thread thread = new Thread(runnable, "zerobus-ack-wait-watchdog");
                thread.setDaemon(true);
                return thread;
            });
        }
        return watchdog;
    }

    @Override
    public synchronized void close() {
        if (watchdog != null) {
            watchdog.shutdownNow();
            watchdog = null;
        }
    }
}
