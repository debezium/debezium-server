/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.server.ConnectionValidationResult;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;

class ZeroBusChangeConsumerTest {

    @Test
    void marksProcessedOnlyAfterDurableAck() throws Exception {
        ZeroBusChangeConsumer consumer = new ZeroBusChangeConsumer();
        consumer.initWithConfig(config(), (targetTable, records) -> ZeroBusWriteResult.acknowledged(records.size()));
        ChangeEvent<Object, Object> record = ZeroBusTableRouterTest.record("server.inventory.customers");

        @SuppressWarnings("unchecked")
        DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer = mock(DebeziumEngine.RecordCommitter.class);
        consumer.handleBatch(List.of(record), committer);

        verify(committer, times(1)).markProcessed(record);
        verify(committer, times(1)).markBatchFinished();
    }

    @Test
    void failureDoesNotMarkProcessedOrBatchFinished() throws Exception {
        ZeroBusChangeConsumer consumer = new ZeroBusChangeConsumer();
        consumer.initWithConfig(config(), (targetTable, records) -> {
            throw new ZeroBusRetriableException("retry");
        });
        ChangeEvent<Object, Object> record = ZeroBusTableRouterTest.record("server.inventory.customers");

        @SuppressWarnings("unchecked")
        DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer = mock(DebeziumEngine.RecordCommitter.class);
        try {
            consumer.handleBatch(List.of(record), committer);
        }
        catch (Exception ignored) {
        }

        verify(committer, never()).markProcessed(record);
        verify(committer, never()).markBatchFinished();
    }

    @Test
    void validateConnectionReturnsSuccessfulForValidConfig() {
        ConnectionValidationResult result = new ZeroBusChangeConsumer().validateConnection(Map.of(
                "endpoint", "https://zerobus.example",
                "workspace.url", "https://workspace.example",
                "authentication.type", "oauth2",
                "authentication.oauth2.client-id", "client",
                "authentication.oauth2.client-secret", "secret",
                "table.mapping.mode", "source",
                "table.mapping.default.catalog", "main",
                "table.mapping.default.schema", "bronze"));

        org.assertj.core.api.Assertions.assertThat(result.valid()).isTrue();
    }

    @Test
    void validateConnectionReturnsFailedForInvalidConfig() {
        ConnectionValidationResult result = new ZeroBusChangeConsumer().validateConnection(Map.of(
                "workspace.url", "https://workspace.example",
                "authentication.type", "oauth2",
                "authentication.oauth2.client-id", "client",
                "authentication.oauth2.client-secret", "secret",
                "table.mapping.mode", "source",
                "table.mapping.default.catalog", "main",
                "table.mapping.default.schema", "bronze"));

        org.assertj.core.api.Assertions.assertThat(result.valid()).isFalse();
        org.assertj.core.api.Assertions.assertThat(result.message()).contains("endpoint");
    }

    private static org.eclipse.microprofile.config.Config config() {
        return new SmallRyeConfigBuilder()
                .withSources(new PropertiesConfigSource(Map.of(
                        "debezium.sink.zerobus.endpoint", "https://zerobus.example",
                        "debezium.sink.zerobus.workspace.url", "https://workspace.example",
                        "debezium.sink.zerobus.authentication.type", "oauth2",
                        "debezium.sink.zerobus.authentication.oauth2.client-id", "client",
                        "debezium.sink.zerobus.authentication.oauth2.client-secret", "secret",
                        "debezium.sink.zerobus.table.mapping.mode", "source",
                        "debezium.sink.zerobus.table.mapping.default.catalog", "main",
                        "debezium.sink.zerobus.table.mapping.default.schema", "bronze",
                        "debezium.sink.zerobus.retries", "1",
                        "debezium.sink.zerobus.retry.interval.ms", "0"),
                        "zerobus-test"))
                .build();
    }
}
