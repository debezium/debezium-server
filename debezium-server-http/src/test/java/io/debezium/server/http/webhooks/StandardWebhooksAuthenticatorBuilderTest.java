/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.webhooks;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;

import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StandardWebhooksAuthenticatorBuilderTest {

    @Test
    public void verifyBuild() throws URISyntaxException {
        StandardWebhooksAuthenticatorBuilder builder = new StandardWebhooksAuthenticatorBuilder();
        builder.setSecret("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw");

        Assertions.assertDoesNotThrow(() -> {
            builder.build();
        });
    }

    @Test
    public void verifyBuildFromConfig() throws URISyntaxException {
        Map<String, Object> configValues = Map.of("debezium.sink.http.authentication.webhook.secret", "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw");

        Config result = mock(Config.class);

        for (Map.Entry<String, Object> entry : configValues.entrySet()) {
            Object value = entry.getValue();
            when(result.getValue(eq(entry.getKey()), any())).thenReturn(value);
            when(result.getOptionalValue(eq(entry.getKey()), any())).thenReturn(Optional.of(value));
        }

        StandardWebhooksAuthenticatorBuilder builder = StandardWebhooksAuthenticatorBuilder.fromConfig(result, "debezium.sink.http.authentication.");

        Assertions.assertDoesNotThrow(() -> {
            builder.build();
        });
    }
}
