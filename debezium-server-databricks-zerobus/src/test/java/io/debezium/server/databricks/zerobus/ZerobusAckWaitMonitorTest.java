/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import io.debezium.server.databricks.zerobus.metrics.ZerobusSinkMetrics;

class ZerobusAckWaitMonitorTest {

    private static final String TABLE = "main.default.customers";

    private final ZerobusSinkMetrics metrics = new ZerobusSinkMetrics("grpc");
    private final Logger logger = mock(Logger.class);
    private final ZerobusAckWaitMonitor monitor = new ZerobusAckWaitMonitor(metrics, logger);

    @AfterEach
    void stopWatchdog() {
        monitor.close();
    }

    @Test
    void warnsWhileTheWaitRunsPastTheThresholdAndRepeatsIt() throws Exception {
        CountDownLatch warnings = new CountDownLatch(2);
        doAnswer(invocation -> {
            warnings.countDown();
            return null;
        }).when(logger).warn(anyString(), any(), any(), any(), any());

        // The acknowledgement only completes after two WARNs, so the test does not depend on timing.
        monitor.await(TABLE, 42, 20, () -> assertThat(warnings.await(10, TimeUnit.SECONDS)).isTrue());

        verify(logger, atLeast(2)).warn(anyString(), eq(42), eq(TABLE), anyLong(), eq(20L));
        verify(logger).info(anyString(), eq(42), eq(TABLE), anyLong());
    }

    @Test
    void staysSilentWhenTheAcknowledgementIsFasterThanTheThreshold() throws Exception {
        monitor.await(TABLE, 3, 60_000, () -> {
        });

        verifyNoInteractions(logger);
    }

    @Test
    void thresholdZeroDisablesTheLogLinesButNotTheDurationGauge() throws Exception {
        monitor.await(TABLE, 3, 0, () -> {
            Thread.sleep(100);
            assertThat(metrics.getCurrentFlushDurationMillis()).isGreaterThanOrEqualTo(50);
        });

        verifyNoInteractions(logger);
        assertThat(metrics.getCurrentFlushDurationMillis()).isZero();
    }

    @Test
    void reportsTheDurationOfTheWaitInProgressAndZeroWhenIdle() throws Exception {
        assertThat(metrics.getCurrentFlushDurationMillis()).isZero();

        monitor.await(TABLE, 3, 0, () -> {
            Thread.sleep(100);
            assertThat(metrics.getCurrentFlushDurationMillis()).isGreaterThanOrEqualTo(50);
        });

        assertThat(metrics.getCurrentFlushDurationMillis()).isZero();
    }

    @Test
    void clearsTheWaitInProgressWhenTheAcknowledgementFails() {
        assertThatThrownBy(() -> monitor.await(TABLE, 3, 20, () -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class).hasMessage("boom");

        assertThat(metrics.getCurrentFlushDurationMillis()).isZero();
    }
}
