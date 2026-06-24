/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;

class ZeroBusSinkConfigTest {

    @Test
    void rejectsMissingEndpoint() {
        assertThatThrownBy(() -> configWithout("endpoint").validate())
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("endpoint");
    }

    @Test
    void rejectsMissingAuthentication() {
        assertThatThrownBy(() -> configWithout("authentication.type").validate())
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("authentication.type");
    }

    @Test
    void rejectsMissingWorkspaceUrl() {
        assertThatThrownBy(() -> configWithout("workspace.url").validate())
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("workspace.url");
    }

    @Test
    void rejectsUnsupportedRecordFormat() {
        assertThatThrownBy(() -> {
            Configuration.Builder builder = baseBuilder();
            builder.with("record.format", "avro");
            new ZeroBusSinkConfig(builder.build()).validate();
        })
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("record format");
    }

    @Test
    void acceptsProtobufRecordFormat() {
        Configuration.Builder builder = baseBuilder();
        builder.with("record.format", "protobuf");
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(builder.build());

        config.validate();

        assertThat(config.getRecordFormat()).isEqualTo("protobuf");
    }

    @Test
    void acceptsArrowRecordFormat() {
        Configuration.Builder builder = baseBuilder();
        builder.with("record.format", "arrow");
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(builder.build());

        config.validate();

        assertThat(config.getRecordFormat()).isEqualTo("arrow");
    }

    @Test
    void rejectsUnsupportedIdempotencyMode() {
        assertThatThrownBy(() -> {
            Configuration.Builder builder = baseBuilder();
            builder.with("idempotency.mode", "service");
            new ZeroBusSinkConfig(builder.build()).validate();
        })
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("idempotency mode");
    }

    @Test
    void rejectsInvalidMaxInflightBatches() {
        assertThatThrownBy(() -> {
            Configuration.Builder builder = baseBuilder();
            builder.with("max.inflight.batches", 0);
            new ZeroBusSinkConfig(builder.build()).validate();
        })
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("max.inflight.batches");
    }

    @Test
    void rejectsUnsupportedTombstoneHandlingMode() {
        assertThatThrownBy(() -> {
            Configuration.Builder builder = baseBuilder();
            builder.with("tombstone.handling.mode", "rewrite");
            new ZeroBusSinkConfig(builder.build()).validate();
        })
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("tombstone handling mode");
    }

    @Test
    void rejectsMissingSourceMappingDefaults() {
        assertThatThrownBy(() -> {
            Configuration.Builder builder = baseBuilder();
            builder.without("table.mapping.default.schema");
            new ZeroBusSinkConfig(builder.build()).validate();
        })
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("table.mapping.default.schema");
    }

    @Test
    void rejectsInvalidRegexMappingPattern() {
        assertThatThrownBy(() -> {
            Configuration.Builder builder = baseBuilder();
            builder.with("table.mapping.mode", "regex");
            builder.with("table.mapping.regex", "(");
            builder.with("table.mapping.replacement", "main.bronze.$1");
            new ZeroBusSinkConfig(builder.build()).validate();
        })
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("table mapping regex");
    }

    @Test
    void acceptsValidSourceMappingConfig() {
        ZeroBusSinkConfig config = baseConfig();

        config.validate();

        assertThat(config.getEndpoint()).isEqualTo("https://zerobus.example");
        assertThat(config.getWorkspaceUrl()).isEqualTo("https://workspace.example");
        assertThat(config.getAuthenticationType()).isEqualTo("oauth2");
        assertThat(config.getRecordFormat()).isEqualTo("json");
        assertThat(config.getBatchSize()).isEqualTo(200);
        assertThat(config.getMaxInflightRecords()).isEqualTo(1_000_000);
        assertThat(config.getMaxInflightBatches()).isEqualTo(1_000);
        assertThat(config.getIdempotencyMode()).isEqualTo("source");
    }

    private ZeroBusSinkConfig configWithout(String key) {
        Configuration.Builder builder = baseBuilder();
        builder.without(key);
        return new ZeroBusSinkConfig(builder.build());
    }

    static ZeroBusSinkConfig baseConfig() {
        return new ZeroBusSinkConfig(baseBuilder().build());
    }

    static Configuration.Builder baseBuilder() {
        return Configuration.create()
                .with("endpoint", "https://zerobus.example")
                .with("workspace.url", "https://workspace.example")
                .with("authentication.type", "oauth2")
                .with("authentication.oauth2.client-id", "client")
                .with("authentication.oauth2.client-secret", "secret")
                .with("table.mapping.mode", "source")
                .with("table.mapping.default.catalog", "main")
                .with("table.mapping.default.schema", "bronze");
    }
}
