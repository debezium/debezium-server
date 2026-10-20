/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;

/**
 * Basic services provided to all change consumers.
 *
 * @author Jiri Pechanec
 *
 */
public class BaseChangeConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseChangeConsumer.class);

    private static final String HEADER_SCHEMAS_ENABLE_PROPERTY = "debezium.format.header.schemas.enable";
    private static final String SCHEMAS_ENABLE_PROPERTY = "debezium.format.schemas.enable";

    protected StreamNameMapper streamNameMapper = (x) -> x;

    @Inject
    Instance<StreamNameMapper> customStreamNameMapper;

    @PostConstruct
    void init() {
        if (customStreamNameMapper.isResolvable()) {
            streamNameMapper = customStreamNameMapper.get();
        }
        LOGGER.info("Using '{}' stream name mapper", streamNameMapper);
    }

    /**
     * Get a subset of the configuration properties that matches the given prefix.
     *
     * @param config    The global configuration object to extract the subset from.
     * @param prefix    The prefix to filter property names.
     *
     * @return          A subset of the original configuration properties containing property names
     *                  without the prefix.
     */
    protected Map<String, Object> getConfigSubset(Config config, String prefix) {
        final Map<String, Object> ret = new HashMap<>();

        for (String propName : config.getPropertyNames()) {
            if (propName.startsWith(prefix)) {
                final String newPropName = propName.substring(prefix.length());
                ret.put(newPropName, config.getConfigValue(propName).getValue());
            }
        }

        return ret;
    }

    /**
     * Get a {@link Configuration} instance from a subset of the MicroProfile configuration properties
     * that matches the given prefix. The resulting configuration performs case-insensitive key lookups,
     * which is necessary because MicroProfile Config maps environment variable names to all-lowercase
     * property names, while Debezium field definitions may use camelCase.
     *
     * @param config    The global MicroProfile configuration object.
     * @param prefix    The prefix to filter property names.
     *
     * @return          A case-insensitive {@link Configuration} containing property names without
     *                  the prefix; never null.
     */
    protected Configuration getConfiguration(Config config, String prefix) {
        final TreeMap<String, String> props = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String propertyName : config.getPropertyNames()) {
            if (propertyName.startsWith(prefix)) {
                final String newPropertyName = propertyName.substring(prefix.length());
                props.put(newPropertyName, config.getConfigValue(propertyName).getValue());
            }
        }
        return new Configuration() {
            @Override
            public String getString(String key) {
                return props.get(key);
            }

            @Override
            public Set<String> keys() {
                return props.keySet();
            }
        };
    }

    protected byte[] getBytes(Object object) {
        if (object instanceof byte[]) {
            return (byte[]) object;
        }
        else if (object instanceof String) {
            return ((String) object).getBytes(StandardCharsets.UTF_8);
        }
        throw new DebeziumException(unsupportedTypeMessage(object));
    }

    protected String getString(Object object) {
        if (object instanceof String) {
            return (String) object;
        }
        throw new DebeziumException(unsupportedTypeMessage(object));
    }

    protected String asString(Object object) {
        return switch (object) {
            case null -> "null";
            case String s -> s;
            case byte[] b -> new String(b, StandardCharsets.UTF_8);
            default -> object.toString();
        };
    }

    protected String unsupportedTypeMessage(Object object) {
        final String type = (object == null) ? "null" : object.getClass().getName();
        return "Unexpected data type '" + type + "'";
    }

    protected Map<String, String> convertHeaders(BatchEvent record) {
        List<Header<Object>> headers = record.headers();
        Map<String, String> result = new HashMap<>();
        for (Header<Object> header : headers) {
            result.put(header.getKey(), getString(header.getValue()));
        }
        return result;
    }

    /**
     * Converts the headers of the given event into a string map. When {@code headerSchemasEnabled} is
     * {@code false}, the raw Connect header values are used to avoid converter-serialized values (a JSON
     * string wrapped in quotes when schemas are disabled); otherwise the converter-serialized values are used.
     */
    protected Map<String, String> resolveHeaders(BatchEvent record, boolean headerSchemasEnabled) {
        return headerSchemasEnabled ? convertHeaders(record) : convertRawHeaders(record);
    }

    /**
     * Extracts raw Connect header values from the {@code SourceRecord}, bypassing the header converter.
     *
     * <p>{@link #convertHeaders(BatchEvent)} reads headers already serialized by the header converter; this
     * reads the plain Java values instead. Headers with a {@code null} value are skipped.
     */
    protected Map<String, String> convertRawHeaders(BatchEvent record) {
        Map<String, String> result = new HashMap<>();
        record.record().headers().forEach(header -> {
            if (header.value() != null) {
                result.put(header.key(), asString(header.value()));
            }
        });
        return result;
    }

    /**
     * Whether the header converter serializes headers with their schemas, i.e. the
     * {@code debezium.format.header.schemas.enable} setting. Falls back to the global
     * {@code debezium.format.schemas.enable} setting, which is also applied to the header converter by
     * {@code DebeziumServerConfigSourceFactory}. Defaults to {@code true} when neither is set.
     */
    protected static boolean isHeaderSchemasEnabled(Config config) {
        return config.getOptionalValue(HEADER_SCHEMAS_ENABLE_PROPERTY, Boolean.class)
                .or(() -> config.getOptionalValue(SCHEMAS_ENABLE_PROPERTY, Boolean.class))
                .orElse(true);
    }
}
