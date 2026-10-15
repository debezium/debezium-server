/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Sets JSSE system properties before Quarkus starts the application. Quarkus test profile overrides are configuration
 * values and do not set JVM system properties.
 */
public class RedisSSLSystemPropertiesTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private final Map<String, String> previousValues = new LinkedHashMap<>();

    @Override
    public Map<String, String> start() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("javax.net.ssl.keyStore", getResourcePath("ssl/client-keystore.p12"));
        properties.put("javax.net.ssl.trustStore", getResourcePath("ssl/client-truststore.p12"));
        properties.put("javax.net.ssl.keyStorePassword", "secret");
        properties.put("javax.net.ssl.trustStorePassword", "secret");

        properties.forEach((name, value) -> previousValues.put(name, System.getProperty(name)));
        try {
            properties.forEach(System::setProperty);
        }
        catch (RuntimeException | Error e) {
            restoreProperties();
            throw e;
        }
        return Map.of();
    }

    @Override
    public void stop() {
        restoreProperties();
    }

    @Override
    public int order() {
        // Set these before other test resources can initialize and cache the JVM default SSL context.
        return -1;
    }

    private void restoreProperties() {
        previousValues.forEach((name, value) -> {
            if (value == null) {
                System.clearProperty(name);
            }
            else {
                System.setProperty(name, value);
            }
        });
        previousValues.clear();
    }

    private static String getResourcePath(String resourceName) {
        URL resource = RedisSSLSystemPropertiesTestResourceLifecycleManager.class.getClassLoader().getResource(resourceName);
        if (resource == null) {
            throw new IllegalStateException("Missing TLS test resource: " + resourceName);
        }

        try {
            return Path.of(resource.toURI()).toString();
        }
        catch (URISyntaxException e) {
            throw new IllegalStateException("Invalid TLS test resource URL: " + resource, e);
        }
    }
}
