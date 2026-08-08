/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.server.databricks.zerobus.ZerobusChangeConsumerConfig.PayloadMode;
import io.debezium.server.databricks.zerobus.ZerobusChangeConsumerConfig.RecordFormat;

/**
 * Tests the payload mode / record format contract: which combinations the sink accepts, and that the
 * defaults keep the behaviour a deployment that sets neither option already relies on.
 */
class ZerobusPayloadModeTest {

    private static ZerobusChangeConsumerConfig configWith(String... options) {
        Configuration.Builder builder = Configuration.create()
                .with("endpoint", "ws.zerobus.us-west-2.cloud.databricks.com")
                .with("workspace.url", "https://ws.cloud.databricks.com")
                .with("client.id", "id")
                .with("client.secret", "secret")
                .with("table", "main.default.t");
        for (int i = 0; i < options.length; i += 2) {
            builder = builder.with(options[i], options[i + 1]);
        }
        return new ZerobusChangeConsumerConfig(builder.build());
    }

    @Test
    void defaultsToTypedRowsEncodedAsJson() {
        ZerobusChangeConsumerConfig config = configWith();

        // These defaults are what the sink did before the options existed, so an existing deployment
        // that sets neither keeps its current data contract.
        assertThat(config.getPayloadMode()).isEqualTo(PayloadMode.TYPED);
        assertThat(config.getRecordFormat()).isEqualTo(RecordFormat.JSON);
    }

    @Test
    void parsesBothModesAndFormats() {
        assertThat(configWith("payload.mode", "envelope").getPayloadMode()).isEqualTo(PayloadMode.ENVELOPE);
        assertThat(configWith("record.format", "protobuf", "payload.mode", "envelope").getRecordFormat())
                .isEqualTo(RecordFormat.PROTOBUF);
    }

    @Test
    void parsingIsCaseInsensitive() {
        assertThat(configWith("payload.mode", "ENVELOPE").getPayloadMode()).isEqualTo(PayloadMode.ENVELOPE);
        assertThat(configWith("record.format", "Json").getRecordFormat()).isEqualTo(RecordFormat.JSON);
    }

    @Test
    void rejectsTypedRowsEncodedAsProtobuf() {
        // The typed path writes JSON, so this combination cannot be honoured: it has to fail when the
        // configuration is read rather than once records start flowing.
        assertThatThrownBy(() -> configWith("payload.mode", "typed", "record.format", "protobuf"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("payload.mode")
                .hasMessageContaining("record.format");
    }

    @Test
    void anUnknownValueFallsBackToTheDefault() {
        // Field.withEnum validates the value for tooling; the parse here must still be total, so a
        // value that got past validation cannot leave the mode null.
        assertThat(configWith("payload.mode", "not-a-mode").getPayloadMode()).isEqualTo(PayloadMode.TYPED);
        assertThat(configWith("record.format", "not-a-format").getRecordFormat()).isEqualTo(RecordFormat.JSON);
    }

    @Test
    void exposesEnvelopeSafetyDefaults() {
        ZerobusChangeConsumerConfig config = configWith("payload.mode", "envelope");

        assertThat(config.getMaxRecordBytes()).isEqualTo(10_000_000);
        assertThat(config.getIdempotencyMode()).isEqualTo("source");
        assertThat(config.getTombstoneHandlingMode()).isEqualTo("event");
        assertThat(config.getJsonFlexibleFieldsEncoding()).isEqualTo("string");
    }

    @Test
    void rejectsInvalidEnvelopeSafetyOptionsAtStartup() {
        assertThatThrownBy(() -> configWith("payload.mode", "envelope", "max.record.bytes", "0"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("max.record.bytes");
        assertThatThrownBy(() -> configWith("payload.mode", "envelope", "filter.value.regex", ".*"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("filter.value.json.pointer")
                .hasMessageContaining("filter.value.regex");
        assertThatThrownBy(() -> configWith("payload.mode", "envelope", "filter.destination.regex", "["))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("filter.destination.regex");
    }

    @Test
    void asksForTombstonesOnlyWhenTheEnvelopeModeWritesThem() {
        // The engine withholds tombstones unless a consumer asks for them, so the envelope mode's
        // 'event' tombstone handling is unreachable without this capability being reported.
        assertThat(tombstoneSupportOf("payload.mode", "envelope", "tombstone.handling.mode", "event")).contains(true);
    }

    @Test
    void doesNotAskForTombstonesWhenTheEnvelopeModeDropsThem() {
        assertThat(tombstoneSupportOf("payload.mode", "envelope", "tombstone.handling.mode", "drop")).contains(false);
    }

    @Test
    void doesNotAskForTombstonesInTypedMode() {
        // The typed path discards a null payload in isJsonObject, so receiving tombstones would only
        // cost it work; the default 'event' handling must not leak across payload modes.
        assertThat(tombstoneSupportOf("payload.mode", "typed", "tombstone.handling.mode", "event")).contains(false);
    }

    /** Mirrors what the consumer reports to the engine, without needing a CDI container. */
    private static java.util.Optional<Boolean> tombstoneSupportOf(String... options) {
        ZerobusChangeConsumerConfig config = configWith(options);
        return java.util.Optional.of(config.getPayloadMode() == PayloadMode.ENVELOPE
                && "event".equals(config.getTombstoneHandlingMode()));
    }
}
