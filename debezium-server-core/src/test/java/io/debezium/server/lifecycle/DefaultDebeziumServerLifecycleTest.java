/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.debezium.server.configuration.DebeziumServerConfig;

public class DefaultDebeziumServerLifecycleTest {

    @Test
    public void exitsWithFailureCodeWhenEngineFailsAndShutdownEnabled() {
        var recorder = new RecordingDebeziumServerLifecycle(shutdownOnCompletion(true));

        recorder.onCompletion(new DebeziumCompletionEvent(false, "boom", new RuntimeException("boom")));

        assertThat(recorder.exitCode.get()).isEqualTo(1);
    }

    @Test
    public void exitsWithSuccessCodeOnNormalCompletionAndShutdownEnabled() {
        var recorder = new RecordingDebeziumServerLifecycle(shutdownOnCompletion(true));

        recorder.onCompletion(new DebeziumCompletionEvent(true, "done", null));

        assertThat(recorder.exitCode.get()).isEqualTo(0);
    }

    @Test
    public void doesNotExitWhenEngineFailsAndShutdownDisabled() {
        var recorder = new RecordingDebeziumServerLifecycle(shutdownOnCompletion(false));

        recorder.onCompletion(new DebeziumCompletionEvent(false, "boom", new RuntimeException("boom")));

        assertThat(recorder.exitCode.get()).isEqualTo(-1);
    }

    @Test
    public void doesNotExitOnNormalCompletionWhenShutdownDisabled() {
        var recorder = new RecordingDebeziumServerLifecycle(shutdownOnCompletion(false));

        recorder.onCompletion(new DebeziumCompletionEvent(true, "done", null));

        assertThat(recorder.exitCode.get()).isEqualTo(-1);
    }

    private static DebeziumServerConfig shutdownOnCompletion(boolean value) {
        return new DebeziumServerConfig() {
            @Override
            public Api api() {
                return () -> false;
            }

            @Override
            public boolean shutdownOnCompletion() {
                return value;
            }
        };
    }

    private static final class RecordingDebeziumServerLifecycle extends DefaultDebeziumServerLifecycle {

        final AtomicInteger exitCode = new AtomicInteger(-1);

        public RecordingDebeziumServerLifecycle(DebeziumServerConfig config) {
            super(config);
        }

        @Override
        public void exit(int code) {
            exitCode.set(code);
        }
    }
}
