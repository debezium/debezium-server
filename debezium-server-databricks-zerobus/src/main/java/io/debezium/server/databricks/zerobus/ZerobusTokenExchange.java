/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Performs the Databricks OAuth 2.0 {@code client_credentials} token exchange scoped to the Zerobus
 * audience, using RFC 9396 {@code authorization_details}. This is the single implementation shared
 * by both routes that authenticate against Zerobus — the REST route ({@link ZerobusTokenProvider})
 * and the Kafka {@code OAUTHBEARER} login handler ({@link ZerobusOAuthBearerLoginCallbackHandler}) —
 * so the request shape and response parsing cannot drift between them.
 * <p>
 * This class is stateless (no token caching); callers own the caching/expiry policy appropriate to
 * their runtime.
 */
final class ZerobusTokenExchange {

    private static final Pattern EXPIRES_IN_PATTERN = Pattern.compile("\"expires_in\"\\s*:\\s*(\\d+)");
    private static final Pattern ACCESS_TOKEN_PATTERN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");

    private final String tokenEndpoint;
    private final String audience;
    private final String authorizationDetails;
    private final String basicAuth;
    private final TokenHttpClient httpClient;

    ZerobusTokenExchange(String tokenEndpoint, String audience, String authorizationDetails,
                         String clientId, String clientSecret, TokenHttpClient httpClient) {
        this.tokenEndpoint = tokenEndpoint;
        this.audience = audience;
        this.authorizationDetails = authorizationDetails;
        this.basicAuth = "Basic " + Base64.getEncoder()
                .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
        this.httpClient = httpClient;
    }

    /** Requests a fresh token and returns the parsed access token and its lifetime. */
    Token requestToken() throws IOException {
        String body = "grant_type=client_credentials"
                + "&scope=all-apis"
                + "&resource=" + URLEncoder.encode(audience, StandardCharsets.UTF_8)
                + "&authorization_details=" + URLEncoder.encode(authorizationDetails, StandardCharsets.UTF_8);

        String response = httpClient.postForm(tokenEndpoint, basicAuth, body);

        String accessToken = firstMatch(ACCESS_TOKEN_PATTERN, response, "access_token");
        long expiresInSeconds = Long.parseLong(firstMatch(EXPIRES_IN_PATTERN, response, "expires_in"));
        return new Token(accessToken, expiresInSeconds);
    }

    private static String firstMatch(Pattern pattern, String response, String field) throws IOException {
        Matcher matcher = pattern.matcher(response);
        if (!matcher.find()) {
            throw new IOException("Could not find '" + field + "' in Zerobus token endpoint response");
        }
        return matcher.group(1);
    }

    /** The result of a token exchange: the access token and its {@code expires_in} in seconds. */
    static final class Token {
        final String accessToken;
        final long expiresInSeconds;

        Token(String accessToken, long expiresInSeconds) {
            this.accessToken = accessToken;
            this.expiresInSeconds = expiresInSeconds;
        }
    }
}
