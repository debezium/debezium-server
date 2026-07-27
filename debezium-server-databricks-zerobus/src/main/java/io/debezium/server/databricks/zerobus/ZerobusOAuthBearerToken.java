/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.util.Set;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

/**
 * Immutable {@link OAuthBearerToken} wrapping a Databricks OAuth access token issued for
 * the Zerobus Ingest audience.
 */
public class ZerobusOAuthBearerToken implements OAuthBearerToken {

    private final String value;
    private final long lifetimeMs;
    private final long startTimeMs;
    private final String principalName;

    public ZerobusOAuthBearerToken(String value, long lifetimeMs, long startTimeMs, String principalName) {
        this.value = value;
        this.lifetimeMs = lifetimeMs;
        this.startTimeMs = startTimeMs;
        this.principalName = principalName;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public Set<String> scope() {
        // Scope is not asserted by the client; the token is validated server-side by Zerobus.
        return Set.of();
    }

    @Override
    public long lifetimeMs() {
        return lifetimeMs;
    }

    @Override
    public String principalName() {
        return principalName;
    }

    @Override
    public Long startTimeMs() {
        return startTimeMs;
    }
}
