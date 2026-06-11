/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.configuration;

import jakarta.annotation.Priority;

import org.eclipse.microprofile.config.spi.Converter;

/**
 * Overrides the built-in StringConverter to preserve empty string config values
 * that go through {@link DebeziumServerConfigSourceFactory} as sentinels (DBZ-5105).
 */
@Priority(200)
public class EmptyStringConverter implements Converter<String> {

    @Override
    public String convert(String value) {
        if (DebeziumServerConfigSourceFactory.EMPTY_VALUE_SENTINEL.equals(value)) {
            return "";
        }
        if (value.isEmpty()) {
            return null;
        }
        return value;
    }
}
