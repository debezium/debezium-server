/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.util;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Retry executor for running callables with retry logic and exponential backoff.
 */
public class RetryExecutor {

    private final int maxRetries;
    private final long initialIntervalMs;
    private final long maxIntervalMs;
    private final double backoffMultiplier;
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryExecutor.class);

    /**
     * RetryExecutor.
     *
     * @param maxRetries Maximum number of retry attempts
     * @param initialIntervalMs Initial wait interval in milliseconds
     * @param maxIntervalMs Maximum wait interval in milliseconds
     * @param backoffMultiplier Multiplier for exponential backoff
     */
    public RetryExecutor(int maxRetries, long initialIntervalMs,
                         long maxIntervalMs, double backoffMultiplier) {
        this.maxRetries = maxRetries;
        this.initialIntervalMs = initialIntervalMs;
        this.maxIntervalMs = maxIntervalMs;
        this.backoffMultiplier = backoffMultiplier;
    }

    /**
     * Execute a callable action with retry logic.
     * Observability services that consuming Debezium logs can track the retries and failures.
     *
     * @param action
     * @param isRetryable Predicate to determine if an exception is retryable
     * @param operationName Name for logging purposes
     */
    public <T> T executeWithRetry(
                                  Callable<T> action,
                                  Predicate<Exception> isRetryable,
                                  String operationName)
            throws InterruptedException {

        int attempts = 0;
        long currentInterval = initialIntervalMs;

        while (true) {
            try {
                T result = action.call();
                if (attempts > 0) {
                    LOGGER.info("Successfully completed {} after {} retry attempt(s)",
                            operationName, attempts);
                }
                return result;
            }
            catch (Exception e) {
                attempts++;

                if (!isRetryable.test(e)) {
                    throw new DebeziumException("Non-retryable error in " + operationName, e);
                }

                if (attempts >= maxRetries) {
                    throw new DebeziumException(
                            String.format("Exceeded max retries (%d) for %s", maxRetries, operationName), e);
                }

                LOGGER.warn("{} failed (attempt {}/{}): {}. Retrying in {}ms...",
                        operationName, attempts, maxRetries, e.getMessage(), currentInterval);

                Metronome.sleeper(Duration.ofMillis(currentInterval), Clock.SYSTEM).pause();
                currentInterval = Math.min((long) (currentInterval * backoffMultiplier), maxIntervalMs);
            }
        }
    }

    /**
     * Execute a runnable action, that has no return statement, with retry logic.
     *
     * @param action
     * @param isRetryable Predicate to determine if an exception is retryable
     * @param operationName Name for logging purposes
     */
    public void executeWithRetry(
                                 ThrowingRunnable action,
                                 Predicate<Exception> isRetryable,
                                 String operationName)
            throws InterruptedException {

        executeWithRetry(() -> {
            action.run();
            return null;
        }, isRetryable, operationName);
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
