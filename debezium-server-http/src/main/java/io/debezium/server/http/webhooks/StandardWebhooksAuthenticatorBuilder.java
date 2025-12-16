/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.webhooks;

import java.time.Clock;

import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

public class StandardWebhooksAuthenticatorBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardWebhooksAuthenticatorBuilder.class);

    private static final String PROP_SECRET = "webhook.secret";

    private String secret;

    public static StandardWebhooksAuthenticatorBuilder fromConfig(Config config, String prop_prefix) {
        StandardWebhooksAuthenticatorBuilder builder = new StandardWebhooksAuthenticatorBuilder();

        builder.setSecret(config.getValue(prop_prefix + PROP_SECRET, String.class));
        return builder;
    }

    public StandardWebhooksAuthenticatorBuilder setSecret(String secret) {
        this.secret = secret;
        return this;
    }

    public StandardWebhooksAuthenticator build() {
        if (secret == null) {
            String msg = "Cannot build StandardWebhooksAuthenticator.  Secret must be set.";
            LOGGER.error(msg);
            throw new DebeziumException(msg);
        }

        return new StandardWebhooksAuthenticator(secret, Clock.systemUTC());
    }
}
