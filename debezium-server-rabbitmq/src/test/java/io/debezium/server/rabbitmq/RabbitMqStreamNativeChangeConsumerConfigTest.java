/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;

/**
 * Unit tests for {@link RabbitMqStreamNativeChangeConsumerConfig}, covering the TLS/mTLS
 * key store and trust store properties and their cross-field validation.
 */
public class RabbitMqStreamNativeChangeConsumerConfigTest {

    @Test
    public void testTlsVerifyHostnameDefaultsToFalse() {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of()));
        assertFalse(config.isTlsVerifyHostname());
    }

    @Test
    public void testKeyStorePemPropertiesResolved() {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                "tls.keyStore.type", "PEM",
                "tls.keyStore.certificateFilePath", "/path/client.crt",
                "tls.keyStore.keyFilePath", "/path/client.key")));
        assertEquals("PEM", config.getKeyStoreType());
        assertEquals("/path/client.crt", config.getKeyStoreCertificateFilePath());
        assertEquals("/path/client.key", config.getKeyStoreKeyFilePath());
    }

    @Test
    public void testKeyStorePkcs12PropertiesResolved() {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                "tls.keyStore.type", "PKCS12",
                "tls.keyStore.filePath", "/path/client-keystore.p12",
                "tls.keyStore.password", "changeit")));
        assertEquals("PKCS12", config.getKeyStoreType());
        assertEquals("/path/client-keystore.p12", config.getKeyStoreFilePath());
        assertEquals("changeit", config.getKeyStorePassword());
    }

    @Test
    public void testTrustStoreJksPropertiesResolved() {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                "tls.trustStore.type", "JKS",
                "tls.trustStore.filePath", "/path/client-truststore.jks",
                "tls.trustStore.password", "changeit")));
        assertEquals("JKS", config.getTrustStoreType());
        assertEquals("/path/client-truststore.jks", config.getTrustStoreFilePath());
        assertEquals("changeit", config.getTrustStorePassword());
    }

    @Test
    public void testUnsupportedKeyStoreTypeThrows() {
        assertThrows(DebeziumException.class,
                () -> new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                        "tls.keyStore.type", "BOGUS",
                        "tls.keyStore.filePath", "/path/client-keystore.p12"))));
    }

    @Test
    public void testUnsupportedTrustStoreTypeThrows() {
        assertThrows(DebeziumException.class,
                () -> new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                        "tls.trustStore.type", "BOGUS",
                        "tls.trustStore.filePath", "/path/ca.crt"))));
    }

    @Test
    public void testKeyStorePemWithOnlyCertificatePathThrows() {
        assertThrows(DebeziumException.class,
                () -> new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                        "tls.keyStore.type", "PEM",
                        "tls.keyStore.certificateFilePath", "/path/client.crt"))));
    }

    @Test
    public void testKeyStorePemWithOnlyKeyPathThrows() {
        assertThrows(DebeziumException.class,
                () -> new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                        "tls.keyStore.type", "PEM",
                        "tls.keyStore.keyFilePath", "/path/client.key"))));
    }

    @Test
    public void testKeyStorePkcs12WithoutFilePathThrows() {
        assertThrows(DebeziumException.class,
                () -> new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                        "tls.keyStore.type", "PKCS12"))));
    }

    @Test
    public void testTrustStorePemWithoutFilePathThrows() {
        assertThrows(DebeziumException.class,
                () -> new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                        "tls.trustStore.type", "PEM"))));
    }

    @Test
    public void testNoTlsStoreConfigurationSucceeds() {
        // Absence of any keyStore/trustStore configuration must remain valid, preserving
        // backward compatibility for existing tls.enable=true users.
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                "tls.enable", "true")));
        assertEquals(null, config.getKeyStoreType());
        assertEquals(null, config.getTrustStoreType());
    }
}
