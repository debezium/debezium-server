/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;

/**
 * Fetches and caches a Databricks OAuth token scoped to the Zerobus audience for the REST route.
 * The token exchange itself is delegated to the shared {@link ZerobusTokenExchange} (used by the
 * Kafka route as well); this class only owns the caching/expiry policy.
 */
class ZerobusTokenProvider {

    private static final long EXPIRY_GUARD_MS = 60_000L;

    private final ZerobusTokenExchange tokenExchange;

    private volatile String cachedToken;
    private volatile long expiryMs;

    ZerobusTokenProvider(String workspaceUrl, String workspaceId, String clientId, String clientSecret, String tablesCsv) {
        this(workspaceUrl, workspaceId, clientId, clientSecret, tablesCsv, new DefaultTokenHttpClient());
    }

    ZerobusTokenProvider(String workspaceUrl, String workspaceId, String clientId, String clientSecret, String tablesCsv,
                         TokenHttpClient httpClient) {
        String base = workspaceUrl.endsWith("/") ? workspaceUrl.substring(0, workspaceUrl.length() - 1) : workspaceUrl;
        String tokenEndpoint = base + "/oidc/v1/token";
        String audience = "api://databricks/workspaces/" + workspaceId + "/zerobusDirectWriteApi";
        String authorizationDetails = ZerobusOAuthBearerLoginCallbackHandler.buildAuthorizationDetails(tablesCsv);
        this.tokenExchange = new ZerobusTokenExchange(tokenEndpoint, audience, authorizationDetails, clientId, clientSecret, httpClient);
    }

    synchronized String currentToken() throws IOException {
        long now = System.currentTimeMillis();
        if (cachedToken != null && now < expiryMs - EXPIRY_GUARD_MS) {
            return cachedToken;
        }
        ZerobusTokenExchange.Token token = tokenExchange.requestToken();
        cachedToken = token.accessToken;
        expiryMs = now + token.expiresInSeconds * 1000L;
        return cachedToken;
    }
}
