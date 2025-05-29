/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.*;

/**
 *  Verifies that records can be streamed to Redis using Redis specific SSL configuration options.
 */
@QuarkusIntegrationTest
@TestProfile(RedisSSLOptionsStreamTestProfile.class)
public class RedisSSLOptionsStreamIT {

    @Test
    public void testRedisStream() throws Exception {
        Jedis jedis = getJedisClient();

        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";
        final String HASH_NAME = "metadata:debezium:offsets";

        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT);

        Long streamLength = jedis.xlen(STREAM_NAME);
        assertTrue("Redis Basic Stream Test Failed", streamLength == MESSAGE_COUNT);

        // wait until the offsets are re-written
        TestUtils.awaitHashSizeGte(jedis, HASH_NAME, 1);

        Map<String, String> redisOffsets = jedis.hgetAll(HASH_NAME);
        assertTrue(redisOffsets.size() > 0);

        jedis.close();
    }

    private Jedis getJedisClient() {
        HostAndPort address = HostAndPort.from(RedisSSLTestResourceLifecycleManager.getRedisContainerAddress());
        URL keyStoreFile = RedisSSLOptionsStreamTestProfile.class.getClassLoader().getResource("ssl/client-keystore.p12");
        URL trustStoreFile = RedisSSLOptionsStreamTestProfile.class.getClassLoader().getResource("ssl/client-truststore.p12");

        // Unlike the regular SSL test, the keystore and truststore are not passed as system properties and need to be set explicitly
        SslOptions sslOptions = SslOptions.builder()
                .truststore(new File(trustStoreFile.getPath()), "secret".toCharArray())
                .trustStoreType("PKCS12")
                .keystore(new File(keyStoreFile.getPath()), "secret".toCharArray())
                .keyStoreType("PKCS12")
                .sslVerifyMode(SslVerifyMode.CA)
                .build();
        DefaultJedisClientConfig config = DefaultJedisClientConfig.builder()
                .ssl(true)
                .sslOptions(sslOptions)
                .build();

        return new Jedis(address, config);
    }
}
