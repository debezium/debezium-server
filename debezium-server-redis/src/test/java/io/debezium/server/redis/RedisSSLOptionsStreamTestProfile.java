/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTestProfile;

public class RedisSSLOptionsStreamTestProfile implements QuarkusTestProfile {

    @Override
    public List<TestResourceEntry> testResources() {
        return Arrays.asList(
                new TestResourceEntry(PostgresTestResourceLifecycleManager.class),
                new TestResourceEntry(RedisSSLTestResourceLifecycleManager.class));
    }

    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<String, String>();
        URL keyStoreFile = RedisSSLOptionsStreamTestProfile.class.getClassLoader().getResource("ssl/client-keystore.p12");
        URL trustStoreFile = RedisSSLOptionsStreamTestProfile.class.getClassLoader().getResource("ssl/client-truststore.p12");

        // Instead of using javax.net.ssl properties (used in RedisSSLStreamIT), redis sink specific properties are used
        config.put("debezium.sink.redis.ssl.truststore.path", trustStoreFile.getPath());
        config.put("debezium.sink.redis.ssl.truststore.password", "secret");
        config.put("debezium.sink.redis.ssl.truststore.type", "PKCS12");
        config.put("debezium.sink.redis.ssl.keystore.path", keyStoreFile.getPath());
        config.put("debezium.sink.redis.ssl.keystore.password", "secret");
        config.put("debezium.sink.redis.ssl.keystore.type", "PKCS12");

        config.put("debezium.source.offset.storage", "io.debezium.server.redis.RedisOffsetBackingStore");
        config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        return config;
    }

}
