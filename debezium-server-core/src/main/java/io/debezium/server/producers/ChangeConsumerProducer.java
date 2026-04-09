/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.producers;

import static io.debezium.server.configuration.DebeziumProperties.PROP_SINK_TYPE;

import io.debezium.server.api.ChangeConsumerHandler;
import io.debezium.server.api.DebeziumServerConsumer;
import io.quarkus.arc.Unremovable;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;

import java.util.Optional;

/**
 * CDI producer that creates and validates the {@link ChangeConsumerHandler} based on configuration.
 * <p>
 * This producer is responsible for discovering and instantiating the appropriate sink consumer
 * implementation at application startup. It reads the {@code debezium.sink.type} configuration
 * property and uses CDI bean discovery to locate a matching {@link DebeziumServerConsumer}
 * annotated with {@code @Named} using that sink type identifier.
 * <p>
 * The produced {@link ChangeConsumerHandler} provides access to the selected consumer instance
 * and its capabilities (e.g., tombstone support) to other components in the application.
 *
 * @see ChangeConsumerHandler
 * @see DebeziumServerConsumer
 */
@ApplicationScoped
public class ChangeConsumerProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeConsumerProducer.class);
    private final Instance<DebeziumServerConsumer<CapturingEvents<BatchEvent>>> instance;
    private final Config config;

    @Inject
    public ChangeConsumerProducer(Instance<DebeziumServerConsumer<CapturingEvents<BatchEvent>>> instance, Config config) {
        this.instance = instance;
        this.config = config;
    }


    @Startup
    @Produces
    @Unremovable
    @ApplicationScoped
    public ChangeConsumerHandler produces() {
        final String name = config.getValue(PROP_SINK_TYPE, String.class);

        if (instance.select(NamedLiteral.of(name)).isUnsatisfied()) {
            throw new DebeziumException("No Debezium consumer named '" + name + "' is available");
        }

        if (instance.select(NamedLiteral.of(name)).isAmbiguous()) {
            LOGGER.debug("Found {} candidate consumer(DebeziumServerConsumers)", instance.select(NamedLiteral.of(name)).stream().count());

            throw new DebeziumException("Multiple Debezium consumers named '" + name + "' were found");
        }

        DebeziumServerConsumer<CapturingEvents<BatchEvent>> consumer = instance.select(NamedLiteral.of(name)).get();
        LOGGER.info("Consumer '{}' instantiated", consumer.getClass().getName());

        return new ChangeConsumerHandler() {
            @Override
            public DebeziumServerConsumer<CapturingEvents<BatchEvent>> get() {
                return consumer;
            }

            @Override
            public Optional<Boolean> tombstoneSupport() {
                return consumer.tombstoneSupport();
            }
        };
    }

}
