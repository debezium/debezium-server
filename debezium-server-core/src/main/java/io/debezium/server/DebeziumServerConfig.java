/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "debezium")
public interface DebeziumServerConfig {
    Api api();

    interface Api {
        @WithDefault("false")
        boolean enabled();
    }
}
