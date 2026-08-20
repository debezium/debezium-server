/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends a single record to the Zerobus REST ingest endpoint. Encapsulates the record-insert HTTP
 * concern of the REST route, the counterpart to {@link DefaultTokenHttpClient} for the token
 * exchange, so that the consumer holds the batch loop and this class holds the request wiring.
 * <p>
 * A caller that already owns an {@link HttpClient} should pass it in, so that a single client (and
 * therefore a single thread pool) serves both the record POSTs and the OAuth token exchange.
 */
class ZerobusRestClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZerobusRestClient.class);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final HttpClient httpClient;
    private final String baseUri;
    private final ZerobusTokenProvider tokenProvider;
    private final String workspaceUrl;

    ZerobusRestClient(HttpClient httpClient, String baseUri, ZerobusTokenProvider tokenProvider, String workspaceUrl) {
        this.httpClient = httpClient;
        this.baseUri = baseUri;
        this.tokenProvider = tokenProvider;
        this.workspaceUrl = workspaceUrl;
    }

    /**
     * Posts one serialized record to {@code <baseUri>/zerobus/v1/tables/<table>/insert}.
     *
     * @throws IOException if the request fails or the endpoint returns a non-2xx status
     */
    void insert(String table, String json) throws IOException, InterruptedException {
        String url = baseUri + "/zerobus/v1/tables/" + table + "/insert";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + tokenProvider.currentToken())
                .header("unity-catalog-endpoint", workspaceUrl)
                .header("x-databricks-zerobus-table-name", table)
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IOException("Zerobus REST returned HTTP " + resp.statusCode() + " for table '" + table + "': " + resp.body());
        }
        LOGGER.trace("Ingested record to {} (HTTP {})", table, resp.statusCode());
    }
}
