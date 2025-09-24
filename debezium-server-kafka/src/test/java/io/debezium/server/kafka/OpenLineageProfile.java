/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class OpenLineageProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<String, String>();

        config.put("openlineage.integration.enabled", "true");
        config.put("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath());
        config.put("openlineage.integration.job.description", "This connector does cdc for products");
        config.put("openlineage.integration.job.tags", "env=prod,team=cdc");
        config.put("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist");

        return config;
    }
}
