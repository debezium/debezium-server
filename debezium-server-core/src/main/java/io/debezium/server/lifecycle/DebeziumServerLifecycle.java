/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.quarkus.runtime.LaunchMode;
import io.quarkus.runtime.Quarkus;

/**
 * Governs the Debezium Server process lifecycle in response to the embedded engine lifecycle.
 * <p>
 * Implementations observe the terminal {@link DebeziumCompletionEvent} and decide how the
 * server should react to the engine stopping, for example shutting the process down so an
 * external supervisor (systemd, Docker, Kubernetes) can restart the connector.
 */
public interface DebeziumServerLifecycle {

    /**
     * Triggers the asynchronous JVM shutdown with the given exit code, but only when running as
     * a real process ({@link LaunchMode#NORMAL}); in dev and test mode it is a no-op so the dev
     * server and the test suite are not torn down. Exposed as a separate method so tests can
     * assert the shutdown decision without terminating the test JVM.
     *
     * @param code the process exit code to report
     */
    default void exit(int code) {
        if (LaunchMode.current() == LaunchMode.NORMAL) {
            Quarkus.asyncExit(code);
        }
    }

    /**
     * Observes the terminal engine completion event and optionally shuts the server down,
     * reporting exit code {@code 0} on a normal completion and a non-zero code on failure.
     *
     * @param event the completion event fired once when the engine stops; carries the
     *            success flag, message and failure cause
     */
    void onCompletion(DebeziumCompletionEvent event);
}
