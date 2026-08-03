/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

import tech.ydb.topic.description.Codec;

public class YdbChangeConsumerTest {

    @Test
    public void parseCodecMapsKnownValuesCaseAndWhitespaceInsensitive() {
        assertThat(YdbChangeConsumerConfig.parseProducerCodec("RAW")).isEqualTo(Codec.RAW);
        assertThat(YdbChangeConsumerConfig.parseProducerCodec("raw")).isEqualTo(Codec.RAW);
        assertThat(YdbChangeConsumerConfig.parseProducerCodec(" GZIP ")).isEqualTo(Codec.GZIP);
        assertThat(YdbChangeConsumerConfig.parseProducerCodec("gzip")).isEqualTo(Codec.GZIP);
        assertThat(YdbChangeConsumerConfig.parseProducerCodec("ZSTD")).isEqualTo(Codec.ZSTD);
        assertThat(YdbChangeConsumerConfig.parseProducerCodec("Zstd")).isEqualTo(Codec.ZSTD);
    }

    @Test
    public void parseCodecDefaultsToGzipForNullOrBlank() {
        assertThat(YdbChangeConsumerConfig.parseProducerCodec(null)).isEqualTo(Codec.GZIP);
        assertThat(YdbChangeConsumerConfig.parseProducerCodec("")).isEqualTo(Codec.GZIP);
        assertThat(YdbChangeConsumerConfig.parseProducerCodec("   ")).isEqualTo(Codec.GZIP);
    }

    @Test
    public void parseCodecRejectsUnknownCodec() {
        assertThatThrownBy(() -> YdbChangeConsumerConfig.parseProducerCodec("LZ4"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("LZ4");
    }

    @Test
    public void resolveTopicPathExplicitTopicWinsOverDestination() {
        assertThat(YdbChangeConsumer.resolveTopicPath("/Root/cdc", "/ignored/", "destination"))
                .isEqualTo("/Root/cdc");
    }

    @Test
    public void resolveTopicPathBlankTopicFallsBackToDestination() {
        assertThat(YdbChangeConsumer.resolveTopicPath("   ", "", "/Root/inv/customers"))
                .isEqualTo("/Root/inv/customers");
        assertThat(YdbChangeConsumer.resolveTopicPath("\t", null, "destination"))
                .isEqualTo("destination");
    }

    @Test
    public void resolveTopicPathUsesDestinationWhenNoPrefix() {
        assertThat(YdbChangeConsumer.resolveTopicPath(null, "", "/Root/inv/customers"))
                .isEqualTo("/Root/inv/customers");
        assertThat(YdbChangeConsumer.resolveTopicPath(null, null, "/Root/inv/customers"))
                .isEqualTo("/Root/inv/customers");
    }

    @Test
    public void resolveTopicPathJoinsPrefixAndDestinationWithSingleSlash() {
        assertThat(YdbChangeConsumer.resolveTopicPath(null, "/Root/debezium", "customers"))
                .isEqualTo("/Root/debezium/customers");
        assertThat(YdbChangeConsumer.resolveTopicPath(null, "/Root/debezium/", "customers"))
                .isEqualTo("/Root/debezium/customers");
        assertThat(YdbChangeConsumer.resolveTopicPath(null, "/Root/debezium", "/customers"))
                .isEqualTo("/Root/debezium/customers");
    }

    @Test
    public void resolveTopicPathCollapsesDoubleSlashFromBothSides() {
        assertThat(YdbChangeConsumer.resolveTopicPath(null, "/Root/debezium/", "/customers"))
                .isEqualTo("/Root/debezium/customers");
    }

    @Test
    public void resolveTopicPathThrowsWhenDestinationMissingAndNoTopic() {
        assertThatThrownBy(() -> YdbChangeConsumer.resolveTopicPath(null, "/Root/", null))
                .isInstanceOf(DebeziumException.class);
        assertThatThrownBy(() -> YdbChangeConsumer.resolveTopicPath(null, null, null))
                .isInstanceOf(DebeziumException.class);
    }

    @Test
    public void resolveInstanceIdUsesConfiguredValueWhenSet() {
        assertThat(YdbChangeConsumerConfig.resolveInstanceId("custom-writer", "my-connector", "host1"))
                .isEqualTo("custom-writer");
    }

    @Test
    public void resolveInstanceIdDefaultsToConnectorAndHostname() {
        assertThat(YdbChangeConsumerConfig.resolveInstanceId(null, "orders-cdc", "pod-7"))
                .isEqualTo("orders-cdc::pod-7");
        assertThat(YdbChangeConsumerConfig.resolveInstanceId("  ", "orders-cdc", "pod-7"))
                .isEqualTo("orders-cdc::pod-7");
    }

    @Test
    public void unwrapAsyncFailureUnwrapsCompletionAndExecutionExceptions() {
        InterruptedException interrupted = new InterruptedException("stopped");
        assertThat(YdbChangeConsumer.unwrapAsyncFailure(new CompletionException(interrupted)))
                .isSameAs(interrupted);
        assertThat(YdbChangeConsumer.unwrapAsyncFailure(new ExecutionException(interrupted)))
                .isSameAs(interrupted);

        RuntimeException root = new RuntimeException("root");
        assertThat(YdbChangeConsumer.unwrapAsyncFailure(new CompletionException(new ExecutionException(root))))
                .isSameAs(root);
    }

    @Test
    public void propagateAsyncFailureRethrowsInterruptedException() {
        InterruptedException interrupted = new InterruptedException("stopped");
        assertThatThrownBy(() -> YdbChangeConsumer.propagateAsyncFailure(new CompletionException(interrupted)))
                .isSameAs(interrupted);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        Thread.interrupted(); // clear for other tests
    }

    @Test
    public void propagateAsyncFailureMapsCancellationToInterruptedException() {
        assertThatThrownBy(() -> YdbChangeConsumer.propagateAsyncFailure(new CancellationException()))
                .isInstanceOf(InterruptedException.class)
                .hasMessageContaining("cancelled");
        Thread.interrupted();
    }

    @Test
    public void propagateAsyncFailureWrapsOtherCausesInDebeziumException() {
        RuntimeException root = new RuntimeException("root");
        assertThatThrownBy(() -> YdbChangeConsumer.propagateAsyncFailure(new ExecutionException(root)))
                .isInstanceOf(DebeziumException.class)
                .hasCause(root);
    }
}
