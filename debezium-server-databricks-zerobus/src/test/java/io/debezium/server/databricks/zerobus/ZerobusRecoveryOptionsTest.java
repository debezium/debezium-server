/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.databricks.zerobus.StreamConfigurationOptions;

import io.debezium.config.Configuration;

/**
 * Tests that the stream recovery options are passed to the Zerobus SDK when configured, and that an
 * option left out keeps whatever the SDK defaults to rather than being pinned by this sink.
 */
class ZerobusRecoveryOptionsTest {

    private static ZerobusChangeConsumerConfig configWith(String... keysAndValues) {
        Configuration.Builder builder = Configuration.create()
                .with("endpoint", "ws.zerobus.us-west-2.cloud.databricks.com")
                .with("workspace.url", "https://ws.cloud.databricks.com")
                .with("client.id", "id")
                .with("client.secret", "secret");
        for (int i = 0; i < keysAndValues.length; i += 2) {
            builder = builder.with(keysAndValues[i], keysAndValues[i + 1]);
        }
        return new ZerobusChangeConsumerConfig(builder.build());
    }

    private static StreamConfigurationOptions optionsFrom(ZerobusChangeConsumerConfig config) {
        return ZerobusChangeConsumer.recoveryOptions(StreamConfigurationOptions.builder(), config).build();
    }

    @Test
    void leavesEverySdkDefaultAloneWhenNothingIsConfigured() {
        StreamConfigurationOptions sdkDefaults = StreamConfigurationOptions.builder().build();
        StreamConfigurationOptions applied = optionsFrom(configWith());

        assertThat(applied.recovery()).isEqualTo(sdkDefaults.recovery());
        assertThat(applied.recoveryRetries()).isEqualTo(sdkDefaults.recoveryRetries());
        assertThat(applied.recoveryBackoffMs()).isEqualTo(sdkDefaults.recoveryBackoffMs());
        assertThat(applied.recoveryTimeoutMs()).isEqualTo(sdkDefaults.recoveryTimeoutMs());
        assertThat(applied.flushTimeoutMs()).isEqualTo(sdkDefaults.flushTimeoutMs());
    }

    @Test
    void appliesEveryOptionThatIsConfigured() {
        StreamConfigurationOptions applied = optionsFrom(configWith(
                "recovery", "true",
                "recovery.retries", "7",
                "recovery.backoff.ms", "250",
                "recovery.timeout.ms", "9000",
                "flush.timeout.ms", "12000"));

        assertThat(applied.recovery()).isTrue();
        assertThat(applied.recoveryRetries()).isEqualTo(7);
        assertThat(applied.recoveryBackoffMs()).isEqualTo(250);
        assertThat(applied.recoveryTimeoutMs()).isEqualTo(9000);
        assertThat(applied.flushTimeoutMs()).isEqualTo(12000);
    }

    @Test
    void appliesOnlyTheConfiguredSubset() {
        StreamConfigurationOptions sdkDefaults = StreamConfigurationOptions.builder().build();
        StreamConfigurationOptions applied = optionsFrom(configWith("recovery.retries", "3"));

        assertThat(applied.recoveryRetries()).isEqualTo(3);
        // The options that were not configured keep the SDK values.
        assertThat(applied.recoveryBackoffMs()).isEqualTo(sdkDefaults.recoveryBackoffMs());
        assertThat(applied.recoveryTimeoutMs()).isEqualTo(sdkDefaults.recoveryTimeoutMs());
        assertThat(applied.flushTimeoutMs()).isEqualTo(sdkDefaults.flushTimeoutMs());
    }

    @Test
    void recoveryCanBeTurnedOffExplicitly() {
        assertThat(optionsFrom(configWith("recovery", "false")).recovery()).isFalse();
    }

    @Test
    void exposesTheRecoveryOptionsAsConfigFields() {
        io.debezium.config.Field.Set fields = new ZerobusChangeConsumer().getConfigFields();

        assertThat(fields.asArray()).extracting("name")
                .contains("recovery", "recovery.retries", "recovery.backoff.ms", "recovery.timeout.ms", "flush.timeout.ms");
    }
}
