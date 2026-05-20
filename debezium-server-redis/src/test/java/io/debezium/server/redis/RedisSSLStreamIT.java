/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URL;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.SslOptions;
import redis.clients.jedis.SslVerifyMode;

/**
 * Integration tests for secured Redis
 *
 * @author Oren Elias
 */
@QuarkusIntegrationTest
@TestProfile(RedisSSLStreamTestProfile.class)
public class RedisSSLStreamIT {

    /**
    *  Verifies that all the records of a PostgreSQL table are streamed to Redis
    */
    @Test
    public void testRedisStream() throws Exception {
        HostAndPort address = HostAndPort.from(RedisSSLTestResourceLifecycleManager.getRedisContainerAddress());

        // Configure SSL with both truststore and keystore for mutual TLS
        URL keyStoreFile = RedisSSLStreamTestProfile.class.getClassLoader().getResource("ssl/client-keystore.p12");
        URL trustStoreFile = RedisSSLStreamTestProfile.class.getClassLoader().getResource("ssl/client-truststore.p12");
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

        Jedis jedis = new Jedis(address, config);
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";
        final String HASH_NAME = "metadata:debezium:offsets";

        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT);

        Long streamLength = jedis.xlen(STREAM_NAME);
        assertTrue(streamLength == MESSAGE_COUNT, "Redis Basic Stream Test Failed");

        // wait until the offsets are re-written
        TestUtils.awaitHashSizeGte(jedis, HASH_NAME, 1);

        Map<String, String> redisOffsets = jedis.hgetAll(HASH_NAME);
        assertTrue(redisOffsets.size() > 0);

        jedis.close();
    }
}
