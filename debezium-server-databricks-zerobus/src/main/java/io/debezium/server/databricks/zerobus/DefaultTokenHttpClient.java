/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Default {@link TokenHttpClient} backed by the JDK {@link HttpClient}. Posts an
 * {@code application/x-www-form-urlencoded} body to the Databricks OIDC token endpoint.
 * <p>
 * A caller that already owns an {@link HttpClient} should pass it in, so that a single client (and
 * therefore a single thread pool) serves both the token exchange and the caller's own requests.
 */
class DefaultTokenHttpClient implements TokenHttpClient {

    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final HttpClient httpClient;

    DefaultTokenHttpClient() {
        this(HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build());
    }

    DefaultTokenHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public String postForm(String url, String authorizationHeader, String formBody) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(REQUEST_TIMEOUT)
                .header("Authorization", authorizationHeader)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(formBody, StandardCharsets.UTF_8))
                .build();

        try {
            HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            String body = new String(response.body().readAllBytes(), StandardCharsets.UTF_8);
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IOException("Zerobus token endpoint returned HTTP " + response.statusCode() + ": " + body);
            }
            return body;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while requesting Zerobus OAuth token", e);
        }
    }
}
