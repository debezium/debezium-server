/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.util.HashMap;
import java.util.Map;

import io.debezium.embedded.Connect;
import io.quarkus.test.junit.QuarkusTestProfile;

public class DebeziumServerConnectFormatProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<String, String>();
        config.put("debezium.format.key", Connect.class.getSimpleName().toLowerCase());
        config.put("debezium.format.value", Connect.class.getSimpleName().toLowerCase());
        config.put("debezium.format.header", Connect.class.getSimpleName().toLowerCase());
        return config;
    }

}
