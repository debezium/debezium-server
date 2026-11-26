/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class ShutdownAfterSnapshotStrategyTest {

    @Test
    void shouldNeverStopTheEngineWhenTheOptionIsOffAndNotAskTheGuard() {
        final RecordingGuard guard = new RecordingGuard(true);
        final ShutdownAfterSnapshotStrategy strategy = new ShutdownAfterSnapshotStrategy(false, guard);

        assertThat(strategy.stopAfterInitialSnapshot(record())).isFalse();
        assertThat(guard.calls).isZero();
    }

    @Test
    void shouldDelegateToTheGuardWhenTheOptionIsOn() {
        final SourceRecord record = record();
        final RecordingGuard guard = new RecordingGuard(false);
        final ShutdownAfterSnapshotStrategy strategy = new ShutdownAfterSnapshotStrategy(true, guard);

        assertThat(strategy.stopAfterInitialSnapshot(record)).isFalse();
        assertThat(guard.calls).isEqualTo(1);
        assertThat(guard.lastRecord).isSameAs(record);
    }

    @Test
    void shouldStopTheEngineWhenTheGuardRecognizesTheEndOfTheInitialSnapshot() {
        final RecordingGuard guard = new RecordingGuard(true);
        final ShutdownAfterSnapshotStrategy strategy = new ShutdownAfterSnapshotStrategy(true, guard);

        assertThat(strategy.stopAfterInitialSnapshot(record())).isTrue();
    }

    private static SourceRecord record() {
        return new SourceRecord(Map.of(), Map.of(), "testc.inventory.customers", null, "value");
    }

    private static final class RecordingGuard extends ShutdownAfterSnapshotGuard {

        private final boolean endOfInitialSnapshot;
        private int calls;
        private SourceRecord lastRecord;

        RecordingGuard(boolean endOfInitialSnapshot) {
            this.endOfInitialSnapshot = endOfInitialSnapshot;
        }

        @Override
        public boolean isEndOfInitialSnapshot(SourceRecord record) {
            calls++;
            lastRecord = record;
            return endOfInitialSnapshot;
        }
    }
}
