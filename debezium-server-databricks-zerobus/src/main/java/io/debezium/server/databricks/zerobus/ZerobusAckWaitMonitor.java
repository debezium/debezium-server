/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

/**
 * Makes a long wait for Zerobus to acknowledge a batch visible while it lasts.
 * <p>
 * The sink does not commit the source offsets of a batch until Zerobus has acknowledged its records,
 * so a slow acknowledgement looks like an idle sink unless something reports it. Once a wait has lasted
 * {@code warnThresholdMs}, this monitor logs a WARN with the target table, the number of records still
 * awaiting acknowledgement and the elapsed time. The WARN repeats at the same interval for as long
 * as the wait continues. A wait that ran past the threshold and then completed is reported at INFO with
 * its total duration. A WARN is never logged once the wait has completed. The duration of the wait in
 * progress is reported over JMX by the caller, at the same scope as the flush duration metrics.
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

    private final Logger logger;
    private ScheduledExecutorService watchdog;

    ZerobusAckWaitMonitor(Logger logger) {
        this.logger = logger;
    }

    /**
     * Runs one acknowledgement wait for {@code table} while watching it.
     *
     * @param table the target table whose stream is being waited on
     * @param pendingRecords the number of records handed to the stream since its last acknowledgement
     * @param warnThresholdMs the wait after which the wait is logged, repeated at the same interval; {@code 0} disables
     *            the warning
     * @param acknowledgement the wait itself
     * @throws Exception whatever {@code acknowledgement} throws; the watchdog is stopped in all cases
     */
    void await(String table, int pendingRecords, long warnThresholdMs, Acknowledgement acknowledgement) throws Exception {
        final WaitWatch watch = new WaitWatch(table, pendingRecords, warnThresholdMs, logger);
        final ScheduledFuture<?> warning = warnThresholdMs > 0
                ? scheduler().scheduleAtFixedRate(watch, warnThresholdMs, warnThresholdMs, TimeUnit.MILLISECONDS)
                : null;
        boolean warned = false;
        try {
            acknowledgement.await();
        }
        finally {
            // Marking the wait as finished first means a watchdog run that is already in progress either logged its
            // WARN before this point, or sees the wait as finished and logs nothing.
            warned = watch.finish();
            if (warning != null) {
                warning.cancel(false);
            }
        }
        if (warned) {
            logger.info("Zerobus acknowledged the {} records for table '{}' after {} ms",
                    pendingRecords, table, watch.elapsedMs());
        }
    }

    /**
     * The state of one watched wait, shared by the waiting thread and the watchdog. Both sides take the same lock, so a
     * WARN is either logged before the wait finishes, and then followed by the INFO line, or not at all.
     */
    static final class WaitWatch implements Runnable {

        private final String table;
        private final int pendingRecords;
        private final long warnThresholdMs;
        private final Logger logger;
        private final long startNanos = System.nanoTime();
        private boolean finished;
        private boolean warned;

        WaitWatch(String table, int pendingRecords, long warnThresholdMs, Logger logger) {
            this.table = table;
            this.pendingRecords = pendingRecords;
            this.warnThresholdMs = warnThresholdMs;
            this.logger = logger;
        }

        @Override
        public synchronized void run() {
            if (finished) {
                return;
            }
            warned = true;
            logger.warn("Zerobus has not acknowledged the {} records for table '{}' after {} ms. The offsets of this batch "
                    + "are not committed until it is acknowledged (threshold: debezium.sink.zerobus.flush.warn.threshold.ms={})",
                    pendingRecords, table, elapsedMs(), warnThresholdMs);
        }

        /**
         * Marks the wait as finished, so that no further WARN is logged.
         *
         * @return {@code true} if a WARN was logged for this wait
         */
        synchronized boolean finish() {
            finished = true;
            return warned;
        }

        long elapsedMs() {
            return ZerobusAckWaitMonitor.elapsedMs(startNanos);
        }
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    private synchronized ScheduledExecutorService scheduler() {
        if (watchdog == null) {
            final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, runnable -> {
                final Thread thread = new Thread(runnable, "zerobus-ack-wait-watchdog");
                thread.setDaemon(true);
                return thread;
            });
            // A cancelled warning is removed from the queue at once, instead of staying there until its first fire time.
            executor.setRemoveOnCancelPolicy(true);
            watchdog = executor;
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
