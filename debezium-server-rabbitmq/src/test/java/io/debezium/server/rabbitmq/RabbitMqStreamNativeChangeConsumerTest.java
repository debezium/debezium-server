/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.netty.handler.ssl.SslContext;

/**
 * Unit tests for {@link RabbitMqStreamNativeChangeConsumer#buildSslContext(RabbitMqStreamNativeChangeConsumerConfig)}.
 *
 * <p>Shape validation (e.g. a PEM key store missing one of its two required files) is covered by
 * {@link RabbitMqStreamNativeChangeConsumerConfigTest}, since that validation happens at config
 * construction time. These tests only cover building the {@link SslContext} itself from an
 * already-valid configuration, plus genuine I/O/crypto failures that can only surface once the
 * referenced files are actually read.
 */
class RabbitMqStreamNativeChangeConsumerTest {

    private static final String SSL_RESOURCES = "src/test/resources/ssl/";

    private final RabbitMqStreamNativeChangeConsumer consumer = new RabbitMqStreamNativeChangeConsumer();

    @Test
    void testNoTlsStoreConfigurationBuildsPlainContext() throws Exception {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(
                Configuration.from(Map.of("tls.enable", "true")));

        SslContext sslContext = consumer.buildSslContext(config);

        assertThat(sslContext).isNotNull();
        assertThat(sslContext.isClient()).isTrue();
    }

    @Test
    void testKeyStorePemBuildsSuccessfully() throws Exception {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                "tls.enable", "true",
                "tls.keyStore.type", "PEM",
                "tls.keyStore.certificateFilePath", SSL_RESOURCES + "client.crt",
                "tls.keyStore.keyFilePath", SSL_RESOURCES + "client.key")));

        assertThat(consumer.buildSslContext(config)).isNotNull();
    }

    @Test
    void testKeyStorePkcs12BuildsSuccessfully() throws Exception {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                "tls.enable", "true",
                "tls.keyStore.type", "PKCS12",
                "tls.keyStore.filePath", SSL_RESOURCES + "client-keystore.p12",
                "tls.keyStore.password", "changeit")));

        assertThat(consumer.buildSslContext(config)).isNotNull();
    }

    @Test
    void testTrustStorePemBuildsSuccessfully() throws Exception {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                "tls.enable", "true",
                "tls.trustStore.type", "PEM",
                "tls.trustStore.filePath", SSL_RESOURCES + "client.crt")));

        assertThat(consumer.buildSslContext(config)).isNotNull();
    }

    @Test
    void testTrustStoreJksBuildsSuccessfully() throws Exception {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                "tls.enable", "true",
                "tls.trustStore.type", "JKS",
                "tls.trustStore.filePath", SSL_RESOURCES + "client-truststore.jks",
                "tls.trustStore.password", "changeit")));

        assertThat(consumer.buildSslContext(config)).isNotNull();
    }

    @Test
    void testVerifyHostnameEnabledBuildsSuccessfully() throws Exception {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                "tls.enable", "true",
                "tls.verify.hostname", "true")));

        assertThat(consumer.buildSslContext(config)).isNotNull();
    }

    @Test
    void testKeyStorePemWithMissingFileThrows() {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                "tls.enable", "true",
                "tls.keyStore.type", "PEM",
                "tls.keyStore.certificateFilePath", SSL_RESOURCES + "does-not-exist.crt",
                "tls.keyStore.keyFilePath", SSL_RESOURCES + "client.key")));

        assertThatThrownBy(() -> consumer.buildSslContext(config)).isInstanceOf(Exception.class);
    }

    @Test
    void testKeyStorePkcs12WithWrongPasswordThrows() {
        RabbitMqStreamNativeChangeConsumerConfig config = new RabbitMqStreamNativeChangeConsumerConfig(Configuration.from(Map.of(
                "tls.enable", "true",
                "tls.keyStore.type", "PKCS12",
                "tls.keyStore.filePath", SSL_RESOURCES + "client-keystore.p12",
                "tls.keyStore.password", "wrong-password")));

        assertThatThrownBy(() -> consumer.buildSslContext(config)).isInstanceOf(Exception.class);
    }
}
