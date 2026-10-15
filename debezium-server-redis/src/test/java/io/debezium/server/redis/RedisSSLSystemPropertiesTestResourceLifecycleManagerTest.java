/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class RedisSSLSystemPropertiesTestResourceLifecycleManagerTest {

    private static final String[] TLS_PROPERTIES = {
            "javax.net.ssl.keyStore",
            "javax.net.ssl.trustStore",
            "javax.net.ssl.keyStorePassword",
            "javax.net.ssl.trustStorePassword"
    };

    @Test
    void restoresAbsentAndPreExistingSystemProperties() {
        Map<String, String> originalValues = saveProperties();
        try {
            clearProperties();
            RedisSSLSystemPropertiesTestResourceLifecycleManager absentPropertiesManager = new RedisSSLSystemPropertiesTestResourceLifecycleManager();
            absentPropertiesManager.start();
            for (String name : TLS_PROPERTIES) {
                if (name.endsWith("Password")) {
                    assertEquals("secret", System.getProperty(name));
                }
                else {
                    assertNotNull(System.getProperty(name));
                }
            }
            absentPropertiesManager.stop();
            for (String name : TLS_PROPERTIES) {
                assertNull(System.getProperty(name));
            }

            Map<String, String> expectedValues = new LinkedHashMap<>();
            for (String name : TLS_PROPERTIES) {
                expectedValues.put(name, "previous-" + name);
                System.setProperty(name, "previous-" + name);
            }

            RedisSSLSystemPropertiesTestResourceLifecycleManager existingPropertiesManager = new RedisSSLSystemPropertiesTestResourceLifecycleManager();
            existingPropertiesManager.start();
            existingPropertiesManager.stop();
            expectedValues.forEach((name, value) -> assertEquals(value, System.getProperty(name)));
        }
        finally {
            originalValues.forEach((name, value) -> {
                if (value == null) {
                    System.clearProperty(name);
                }
                else {
                    System.setProperty(name, value);
                }
            });
        }
    }

    private static Map<String, String> saveProperties() {
        Map<String, String> values = new LinkedHashMap<>();
        for (String name : TLS_PROPERTIES) {
            values.put(name, System.getProperty(name));
        }
        return values;
    }

    private static void clearProperties() {
        for (String name : TLS_PROPERTIES) {
            System.clearProperty(name);
        }
    }
}
