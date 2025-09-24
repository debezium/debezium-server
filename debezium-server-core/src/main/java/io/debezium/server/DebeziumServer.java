/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.health.Liveness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.embedded.ClientProvided;
import io.debezium.embedded.Connect;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.format.Avro;
import io.debezium.engine.format.Binary;
import io.debezium.engine.format.CloudEvents;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.JsonByteArray;
import io.debezium.engine.format.Protobuf;
import io.debezium.engine.format.SimpleString;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;

/**
 * <p>The entry point of the Quarkus-based standalone server. The server is configured via Quarkus/Microprofile Configuration sources
 * and provides few out-of-the-box target implementations.</p>
 * <p>The implementation uses CDI to find all classes that implements {@link DebeziumEngine.ChangeConsumer} interface.
 * The candidate classes should be annotated with {@code @Named} annotation and should be {@code Dependent}.</p>
 * <p>The configuration option {@code debezium.consumer} provides a name of the consumer that should be used and the value
 * must match to exactly one of the implementation classes.</p>
 *
 * @author Jiri Pechanec
 *
 */
@ApplicationScoped
@Startup
public class DebeziumServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumServer.class);

    private static final String PROP_PREFIX = "debezium.";
    static final String PROP_SOURCE_PREFIX = PROP_PREFIX + "source.";
    private static final String PROP_SINK_PREFIX = PROP_PREFIX + "sink.";
    private static final String PROP_FORMAT_PREFIX = PROP_PREFIX + "format.";
    private static final String PROP_PREDICATES_PREFIX = PROP_PREFIX + "predicates.";
    private static final String PROP_TRANSFORMS_PREFIX = PROP_PREFIX + "transforms.";
    private static final String PROP_HEADER_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "header.";
    private static final String PROP_KEY_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "key.";
    private static final String PROP_VALUE_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "value.";
    private static final String PROP_OFFSET_STORAGE_PREFIX = "offset.storage.";

    private static final String PROP_PREDICATES = PROP_PREFIX + "predicates";
    private static final String PROP_TRANSFORMS = PROP_PREFIX + "transforms";
    static final String PROP_SINK_TYPE = PROP_SINK_PREFIX + "type";

    private static final String PROP_HEADER_FORMAT = PROP_FORMAT_PREFIX + "header";
    private static final String PROP_KEY_FORMAT = PROP_FORMAT_PREFIX + "key";
    private static final String PROP_VALUE_FORMAT = PROP_FORMAT_PREFIX + "value";
    private static final String PROP_TERMINATION_WAIT = PROP_PREFIX + "termination.wait";

    private static final String PROP_ENGINE_FACTORY = PROP_PREFIX + "engine.factory";

    private static final String FORMAT_JSON = Json.class.getSimpleName().toLowerCase();
    private static final String FORMAT_JSON_BYTE_ARRAY = JsonByteArray.class.getSimpleName().toLowerCase();
    private static final String FORMAT_CLOUDEVENT = CloudEvents.class.getSimpleName().toLowerCase();
    private static final String FORMAT_AVRO = Avro.class.getSimpleName().toLowerCase();
    private static final String FORMAT_PROTOBUF = Protobuf.class.getSimpleName().toLowerCase();
    private static final String FORMAT_BINARY = Binary.class.getSimpleName().toLowerCase();
    private static final String FORMAT_STRING = SimpleString.class.getSimpleName().toLowerCase();
    private static final String FORMAT_CONNECT = Connect.class.getSimpleName().toLowerCase();
    private static final String FORMAT_CLIENT_PROVIDED = ClientProvided.class.getSimpleName().toLowerCase();

    private static final Pattern SHELL_PROPERTY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+_+[a-zA-Z0-9_]+$");

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private int returnCode = 0;

    @Inject
    BeanManager beanManager;

    @Inject
    ChangeConsumerFactory changeConsumerFactory;

    @Inject
    @Liveness
    ConnectorLifecycle health;

    private DefaultChangeConsumer consumer;

    private DebeziumEngine<?> engine;
    private final Properties props = new Properties();

    @SuppressWarnings("unchecked")
    @PostConstruct
    public void start() {
        final Config config = loadConfigOrDie();
        final String name = config.getValue(PROP_SINK_TYPE, String.class);

        consumer = changeConsumerFactory.create();

        final Class<Any> keyFormat = (Class<Any>) getFormat(config, PROP_KEY_FORMAT);
        final Class<Any> valueFormat = (Class<Any>) getFormat(config, PROP_VALUE_FORMAT);
        final Class<Any> headerFormat = (Class<Any>) getHeaderFormat(config);

        configToProperties(config, props, PROP_SOURCE_PREFIX, "", true);
        configToProperties(config, props, PROP_FORMAT_PREFIX, "key.converter.", true);
        configToProperties(config, props, PROP_FORMAT_PREFIX, "value.converter.", true);
        configToProperties(config, props, PROP_FORMAT_PREFIX, "header.converter.", true);
        configToProperties(config, props, PROP_KEY_FORMAT_PREFIX, "key.converter.", true);
        configToProperties(config, props, PROP_VALUE_FORMAT_PREFIX, "value.converter.", true);
        configToProperties(config, props, PROP_HEADER_FORMAT_PREFIX, "header.converter.", true);
        configToProperties(config, props, PROP_SINK_PREFIX + name + ".", SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + name + ".", false);
        configToProperties(config, props, PROP_SINK_PREFIX + name + ".", PROP_OFFSET_STORAGE_PREFIX + name + ".", false);

        final Optional<String> transforms = config.getOptionalValue(PROP_TRANSFORMS, String.class);
        if (transforms.isPresent()) {
            props.setProperty("transforms", transforms.get());
            configToProperties(config, props, PROP_TRANSFORMS_PREFIX, "transforms.", true);
        }

        final Optional<String> predicates = config.getOptionalValue(PROP_PREDICATES, String.class);
        if (predicates.isPresent()) {
            props.setProperty("predicates", predicates.get());
            configToProperties(config, props, PROP_PREDICATES_PREFIX, "predicates.", true);
        }

        props.setProperty("name", name);
        LOGGER.debug("Configuration for DebeziumEngine: {}", props);

        final Optional<String> engineFactory = config.getOptionalValue(PROP_ENGINE_FACTORY, String.class);
        engine = DebeziumEngine.create(keyFormat, valueFormat, headerFormat, engineFactory.orElse(ConvertingAsyncEngineBuilderFactory.class.getName()))
                .using(props)
                .using((DebeziumEngine.ConnectorCallback) health)
                .using((DebeziumEngine.CompletionCallback) health)
                .notifying(consumer)
                .build();

        executor.execute(() -> {
            try {
                engine.run();
            }
            finally {
                Quarkus.asyncExit(returnCode);
            }
        });
        LOGGER.info("Engine executor started");
    }

    private void configToProperties(Config config, Properties props, String oldPrefix, String newPrefix, boolean overwrite) {
        for (String name : config.getPropertyNames()) {
            String updatedPropertyName = null;
            if (SHELL_PROPERTY_NAME_PATTERN.matcher(name).matches()) {
                updatedPropertyName = name.replace("_", ".").toLowerCase();
            }
            if (updatedPropertyName != null && updatedPropertyName.startsWith(oldPrefix)) {
                String finalPropertyName = newPrefix + updatedPropertyName.substring(oldPrefix.length());
                if (overwrite || !props.containsKey(finalPropertyName)) {
                    props.setProperty(finalPropertyName, config.getOptionalValue(name, String.class).orElse(""));
                }
            }
            else if (name.startsWith(oldPrefix)) {
                String finalPropertyName = newPrefix + name.substring(oldPrefix.length());
                if (overwrite || !props.containsKey(finalPropertyName)) {
                    props.setProperty(finalPropertyName, config.getConfigValue(name).getValue());
                }
            }
        }
    }

    private Class<?> getFormat(Config config, String property) {
        final String formatName = config.getOptionalValue(property, String.class).orElse(FORMAT_JSON);
        if (FORMAT_JSON.equals(formatName)) {
            return Json.class;
        }
        if (FORMAT_JSON_BYTE_ARRAY.equals(formatName)) {
            return JsonByteArray.class;
        }
        else if (FORMAT_CLOUDEVENT.equals(formatName)) {
            return CloudEvents.class;
        }
        else if (FORMAT_AVRO.equals(formatName)) {
            return Avro.class;
        }
        else if (FORMAT_PROTOBUF.equals(formatName)) {
            return Protobuf.class;
        }
        else if (FORMAT_BINARY.equals(formatName)) {
            return Binary.class;
        }
        else if (FORMAT_STRING.equals(formatName)) {
            return SimpleString.class;
        }
        else if (FORMAT_CONNECT.equalsIgnoreCase(formatName)) {
            return Connect.class;
        }
        else if (FORMAT_CLIENT_PROVIDED.equals(formatName)) {
            return ClientProvided.class;
        }
        throw new DebeziumException("Unknown format '" + formatName + "' for option " + "'" + property + "'");
    }

    private Class<?> getHeaderFormat(Config config) {
        final String formatName = config.getOptionalValue(PROP_HEADER_FORMAT, String.class).orElse(FORMAT_JSON);
        if (FORMAT_JSON.equals(formatName)) {
            return Json.class;
        }
        else if (FORMAT_JSON_BYTE_ARRAY.equals(formatName)) {
            return JsonByteArray.class;
        }
        else if (FORMAT_CONNECT.equals(formatName)) {
            return Connect.class;
        }
        else if (FORMAT_CLIENT_PROVIDED.equals(formatName)) {
            return ClientProvided.class;
        }
        throw new DebeziumException("Unknown format '" + formatName + "' for option " + "'" + PROP_HEADER_FORMAT + "'");
    }

    public void stop(@Observes ShutdownEvent event) {
        try {
            LOGGER.info("Received request to stop the engine");
            final Config config = ConfigProvider.getConfig();
            try {
                engine.close();
            }
            // TODO: eventually catch more specific exception if DBZ-8732 is implemented
            catch (IllegalStateException e) {
                LOGGER.info("Cannot shut down engine now: ", e.getMessage());
            }
            executor.shutdown();
            executor.awaitTermination(config.getOptionalValue(PROP_TERMINATION_WAIT, Integer.class).orElse(10), TimeUnit.SECONDS);
        }
        catch (Exception e) {
            LOGGER.error("Exception while shutting down Debezium", e);
        }
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) {
        if (!event.isSuccess()) {
            returnCode = 1;
        }
    }

    private Config loadConfigOrDie() {
        final Config config = ConfigProvider.getConfig();
        // Check config and exit if we cannot load mandatory option.
        try {
            config.getValue(PROP_SINK_TYPE, String.class);
        }
        catch (NoSuchElementException e) {
            final String configFile = Paths.get(System.getProperty("user.dir"), "config", "application.properties").toString();
            final String errorMessage = String
                    .format("Failed to load mandatory config value '%s'. Please check you have a correct Debezium server config in %s or required "
                            + "properties are defined via system or environment variables.", PROP_SINK_TYPE, configFile);

            // Print to stderr in case of logging misconfiguration.
            // CHECKSTYLE IGNORE check FOR NEXT 1 LINES
            System.err.println(errorMessage);
            LOGGER.error(errorMessage);

            Quarkus.asyncExit();
        }
        return config;
    }

    /**
     * For test purposes only
     */
    DebeziumEngine.ChangeConsumer<?> getConsumer() {
        return consumer.getDelegateConsumer();
    }

    public Properties getProps() {
        return props;
    }

    public DebeziumEngine.Signaler getSignaler() {
        return engine.getSignaler();
    }
}
