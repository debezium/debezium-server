/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.configuration;

import jakarta.annotation.Priority;

import io.debezium.server.configuration.DebeziumServerConfigSourceFactory.DebeziumServerConfigSource;
import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.Priorities;

/**
 * Replaces an empty value from {@link DebeziumServerConfigSource} with
 * {@link DebeziumProperties#EMPTY_VALUE_SENTINEL} after the expansion. Thus a remapped value such as
 * {@code ${tables:}} that is expanded to an empty string is kept, as an empty raw value is.
 */
@Priority(Priorities.LIBRARY + 400)
public class EmptyValueConfigSourceInterceptor implements ConfigSourceInterceptor {

    @Override
    public ConfigValue getValue(ConfigSourceInterceptorContext context, String name) {
        ConfigValue value = context.proceed(name);
        if (value != null && value.getValue() != null && value.getValue().isEmpty()
                && DebeziumServerConfigSource.NAME.equals(value.getConfigSourceName())) {
            return value.withValue(DebeziumProperties.EMPTY_VALUE_SENTINEL);
        }
        return value;
    }
}
