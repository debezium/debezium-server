/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.bridge;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.debezium.engine.format.Json;
import io.debezium.engine.format.SerializationFormat;
import io.debezium.runtime.DebeziumSerialization;
import io.quarkus.arc.Unremovable;
import io.quarkus.runtime.Startup;

@ApplicationScoped
public class QuarkusSerializationProducer {

    @Startup
    @Produces
    @Unremovable
    @ApplicationScoped
    public DebeziumSerialization<String, String, String> produces() {
        return new DebeziumSerialization<>() {
            @Override
            public Class<? extends SerializationFormat<String>> getKeyFormat() {
                return Json.class;
            }

            @Override
            public Class<? extends SerializationFormat<String>> getValueFormat() {
                return Json.class;
            }

            @Override
            public Class<? extends SerializationFormat<String>> getHeaderFormat() {
                return Json.class;
            }

            @Override
            public String getEngineId() {
                return "default";
            }

        };
    }
}
