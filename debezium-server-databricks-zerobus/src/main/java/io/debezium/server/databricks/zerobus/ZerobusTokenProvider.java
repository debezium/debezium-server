/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Fetches and caches a Databricks OAuth token scoped to the Zerobus audience, using the
 * client-credentials grant with RFC 9396 {@code authorization_details}. Shared by the REST route;
 * the Kafka route has its own copy inside the SASL callback handler.
 */
class ZerobusTokenProvider {

    private static final long EXPIRY_GUARD_MS = 60_000L;
    private static final Pattern EXPIRES_IN_PATTERN = Pattern.compile("\"expires_in\"\\s*:\\s*(\\d+)");
    private static final Pattern ACCESS_TOKEN_PATTERN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");

    private final String tokenEndpoint;
    private final String audience;
    private final String authorizationDetails;
    private final String basicAuth;
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private volatile String cachedToken;
    private volatile long expiryMs;

    ZerobusTokenProvider(String workspaceUrl, String workspaceId, String clientId, String clientSecret, String tablesCsv) {
        String base = workspaceUrl.endsWith("/") ? workspaceUrl.substring(0, workspaceUrl.length() - 1) : workspaceUrl;
        this.tokenEndpoint = base + "/oidc/v1/token";
        this.audience = "api://databricks/workspaces/" + workspaceId + "/zerobusDirectWriteApi";
        this.authorizationDetails = ZerobusOAuthBearerLoginCallbackHandler.buildAuthorizationDetails(tablesCsv);
        this.basicAuth = Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
    }

    synchronized String currentToken() throws IOException {
        long now = System.currentTimeMillis();
        if (cachedToken != null && now < expiryMs - EXPIRY_GUARD_MS) {
            return cachedToken;
        }
        String body = "grant_type=client_credentials"
                + "&scope=all-apis"
                + "&resource=" + URLEncoder.encode(audience, StandardCharsets.UTF_8)
                + "&authorization_details=" + URLEncoder.encode(authorizationDetails, StandardCharsets.UTF_8);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenEndpoint))
                .timeout(Duration.ofSeconds(30))
                .header("Authorization", "Basic " + basicAuth)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();

        String response;
        try {
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                throw new IOException("Zerobus token endpoint returned HTTP " + resp.statusCode() + ": " + resp.body());
            }
            response = resp.body();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while requesting Zerobus OAuth token", e);
        }

        cachedToken = firstMatch(ACCESS_TOKEN_PATTERN, response, "access_token");
        long expiresInSeconds = Long.parseLong(firstMatch(EXPIRES_IN_PATTERN, response, "expires_in"));
        expiryMs = now + expiresInSeconds * 1000L;
        return cachedToken;
    }

    private static String firstMatch(Pattern pattern, String response, String field) throws IOException {
        Matcher matcher = pattern.matcher(response);
        if (!matcher.find()) {
            throw new IOException("Could not find '" + field + "' in Zerobus token endpoint response");
        }
        return matcher.group(1);
    }
}
