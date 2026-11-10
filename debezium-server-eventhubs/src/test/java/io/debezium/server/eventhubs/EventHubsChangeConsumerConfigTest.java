/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;

class EventHubsChangeConsumerConfigTest {

    @Test
    void defaultAuthModeIsConnectionString() {
        Configuration config = Configuration.create()
                .with("connectionstring", "Endpoint=sb://example/;SharedAccessKeyName=x;SharedAccessKey=y")
                .with("hubname", "myhub")
                .build();

        EventHubsChangeConsumerConfig consumerConfig = new EventHubsChangeConsumerConfig(config);

        assertThat(consumerConfig.getAuthMode()).isEqualTo(EventHubsChangeConsumerConfig.AuthMode.CONNECTION_STRING);
    }

    @Test
    void readsAzureCredentialProperties() {
        Configuration config = Configuration.create()
                .with("authmode", "default-azure-credential")
                .with("hubname", "myhub")
                .with("fullyqualifiednamespace", "mynamespace.servicebus.windows.net")
                .build();

        EventHubsChangeConsumerConfig consumerConfig = new EventHubsChangeConsumerConfig(config);

        assertThat(consumerConfig.getAuthMode()).isEqualTo(EventHubsChangeConsumerConfig.AuthMode.DEFAULT_AZURE_CREDENTIAL);
        assertThat(consumerConfig.getFullyQualifiedNamespace()).isEqualTo("mynamespace.servicebus.windows.net");
        assertThat(consumerConfig.getEventHubName()).isEqualTo("myhub");
    }

    @Test
    void fullyQualifiedNamespaceIsNullWhenNotConfigured() {
        Configuration config = Configuration.create()
                .with("connectionstring", "Endpoint=sb://example/;SharedAccessKeyName=x;SharedAccessKey=y")
                .with("hubname", "myhub")
                .build();

        EventHubsChangeConsumerConfig consumerConfig = new EventHubsChangeConsumerConfig(config);

        assertThat(consumerConfig.getFullyQualifiedNamespace()).isNull();
    }

    @Test
    void defaultAzureCredentialRequiresNamespace() {
        Configuration config = Configuration.create()
                .with("authmode", "default-azure-credential")
                .with("hubname", "myhub")
                .build();

        assertThatThrownBy(() -> new EventHubsChangeConsumerConfig(config))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("debezium.sink.eventhubs.fullyqualifiednamespace");
    }

    @Test
    void defaultAzureCredentialRequiresHubName() {
        Configuration config = Configuration.create()
                .with("authmode", "default-azure-credential")
                .with("fullyqualifiednamespace", "mynamespace.servicebus.windows.net")
                .build();

        assertThatThrownBy(() -> new EventHubsChangeConsumerConfig(config))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("debezium.sink.eventhubs.hubname");
    }

    @Test
    void connectionStringModeRequiresConnectionString() {
        Configuration config = Configuration.create()
                .with("hubname", "myhub")
                .build();

        assertThatThrownBy(() -> new EventHubsChangeConsumerConfig(config))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("debezium.sink.eventhubs.connectionstring");
    }

    @Test
    void invalidAuthModeThrows() {
        Configuration config = Configuration.create()
                .with("authmode", "bogus")
                .with("hubname", "myhub")
                .build();

        assertThatThrownBy(() -> new EventHubsChangeConsumerConfig(config))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Invalid authmode");
    }
}
