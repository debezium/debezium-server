/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.lifecycle;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.debezium.server.configuration.DebeziumServerConfig;

@ApplicationScoped
public class DefaultDebeziumServerLifecycle implements DebeziumServerLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDebeziumServerLifecycle.class);

    private static final int SUCCESS_EXIT_CODE = 0;

    private static final int FAILURE_EXIT_CODE = 1;

    private final DebeziumServerConfig config;

    @Inject
    public DefaultDebeziumServerLifecycle(DebeziumServerConfig config) {
        this.config = config;
    }

    @Override
    public void onCompletion(@Observes DebeziumCompletionEvent event) {
        int exitCode = event.isSuccess() ? SUCCESS_EXIT_CODE : FAILURE_EXIT_CODE;

        if (event.isSuccess()) {
            LOGGER.info("Debezium engine completed normally");
        }
        else {
            LOGGER.error("Debezium engine completed with a failure: {}", event.getMessage(), event.getError());
        }

        if (config.shutdownOnCompletion()) {
            LOGGER.info("Shutting down Debezium Server with exit code {}", exitCode);
            exit(exitCode);
        }
        else {
            LOGGER.warn("'debezium.shutdown.on.completion' is disabled; the process stays alive after the engine stopped");
        }
    }

}
