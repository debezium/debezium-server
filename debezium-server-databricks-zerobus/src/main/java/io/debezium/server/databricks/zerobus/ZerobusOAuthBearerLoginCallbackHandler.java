/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka {@link AuthenticateCallbackHandler} that authenticates the native Kafka sink against the
 * Databricks Zerobus Ingest Kafka-compatible endpoint.
 * <p>
 * Zerobus exposes a Kafka listener that requires {@code SASL_SSL} with the {@code OAUTHBEARER}
 * mechanism. This handler performs the OAuth 2.0 {@code client_credentials} flow against the
 * workspace OIDC token endpoint, requesting a token scoped to the Zerobus audience
 * ({@code api://databricks/workspaces/<workspace-id>/zerobusDirectWriteApi}). The resulting
 * access token is cached until shortly before it expires and refreshed on demand.
 * <p>
 * Configure it on the sink producer, e.g. in {@code application.properties}:
 *
 * <pre>
 * debezium.sink.type=kafka
 * debezium.sink.kafka.producer.bootstrap.servers=&lt;workspace-id&gt;.zerobus.&lt;region&gt;.cloud.databricks.com:9092
 * debezium.sink.kafka.producer.security.protocol=SASL_SSL
 * debezium.sink.kafka.producer.sasl.mechanism=OAUTHBEARER
 * debezium.sink.kafka.producer.sasl.login.callback.handler.class=io.debezium.server.databricks.zerobus.ZerobusOAuthBearerLoginCallbackHandler
 * debezium.sink.kafka.producer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
 *   workspaceUrl="https://&lt;workspace-host&gt;" workspaceId="&lt;workspace-id&gt;" \
 *   clientId="&lt;sp-client-id&gt;" clientSecret="&lt;sp-client-secret&gt;" ;
 * </pre>
 */
public class ZerobusOAuthBearerLoginCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZerobusOAuthBearerLoginCallbackHandler.class);

    static final String OPTION_WORKSPACE_URL = "workspaceUrl";
    static final String OPTION_WORKSPACE_ID = "workspaceId";
    static final String OPTION_CLIENT_ID = "clientId";
    static final String OPTION_CLIENT_SECRET = "clientSecret";
    static final String OPTION_AUDIENCE = "audience";
    static final String OPTION_TOKEN_ENDPOINT = "tokenEndpoint";
    static final String OPTION_TOKEN = "token";
    static final String OPTION_TABLES = "tables";

    /** Refresh the token this many milliseconds before its declared expiry. */
    private static final long EXPIRY_GUARD_MS = 60_000L;

    /** Assumed lifetime for a statically-supplied token (e.g. a PAT) when no expiry is known. */
    private static final long STATIC_TOKEN_LIFETIME_MS = 24L * 60 * 60 * 1000;

    private String tokenEndpoint;
    private String audience;
    private String clientId;
    private String clientSecret;
    private String principalName;

    /** OAuth Rich Authorization Requests (RFC 9396) details for the Zerobus target tables. */
    private String authorizationDetails;

    /** When set, this token is used directly (PAT / pre-issued bearer) and no token exchange happens. */
    private String staticToken;

    private TokenHttpClient httpClient = new DefaultTokenHttpClient();

    private volatile ZerobusOAuthBearerToken cachedToken;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!"OAUTHBEARER".equals(saslMechanism)) {
            throw new IllegalArgumentException("Unexpected SASL mechanism: " + saslMechanism + ". Zerobus requires OAUTHBEARER.");
        }
        if (jaasConfigEntries.size() != 1) {
            throw new IllegalArgumentException("Exactly one JAAS entry expected for the Zerobus OAUTHBEARER handler, got " + jaasConfigEntries.size());
        }

        Map<String, ?> options = jaasConfigEntries.get(0).getOptions();

        // Direct-token mode (e.g. a PAT): use it verbatim as the bearer token, no exchange.
        this.staticToken = optionalOption(options, OPTION_TOKEN);
        if (staticToken != null) {
            this.principalName = optionalOption(options, OPTION_CLIENT_ID) != null ? options.get(OPTION_CLIENT_ID).toString() : "token";
            LOGGER.info("Zerobus OAUTHBEARER handler configured in direct-token mode (no token exchange)");
            return;
        }

        this.clientId = requiredOption(options, OPTION_CLIENT_ID);
        this.clientSecret = requiredOption(options, OPTION_CLIENT_SECRET);
        this.principalName = clientId;

        // The token endpoint can be provided directly, or derived from the workspace URL.
        String explicitEndpoint = optionalOption(options, OPTION_TOKEN_ENDPOINT);
        if (explicitEndpoint != null) {
            this.tokenEndpoint = explicitEndpoint;
        }
        else {
            String workspaceUrl = requiredOption(options, OPTION_WORKSPACE_URL);
            this.tokenEndpoint = stripTrailingSlash(workspaceUrl) + "/oidc/v1/token";
        }

        // The audience can be provided directly, or derived from the workspace id.
        String explicitAudience = optionalOption(options, OPTION_AUDIENCE);
        if (explicitAudience != null) {
            this.audience = explicitAudience;
        }
        else {
            String workspaceId = requiredOption(options, OPTION_WORKSPACE_ID);
            this.audience = "api://databricks/workspaces/" + workspaceId + "/zerobusDirectWriteApi";
        }

        // Rich Authorization Requests (RFC 9396): the token must carry the UC privileges for the
        // target tables, otherwise Zerobus rejects it with invalid_authorization_details.
        String tables = requiredOption(options, OPTION_TABLES);
        this.authorizationDetails = buildAuthorizationDetails(tables);

        LOGGER.info("Zerobus OAUTHBEARER handler configured: tokenEndpoint={}, audience={}, clientId={}, tables={}",
                tokenEndpoint, audience, clientId, tables);
    }

    /**
     * Builds the {@code authorization_details} JSON array granting USE CATALOG / USE SCHEMA on the
     * parent catalog/schema and SELECT+MODIFY on each fully-qualified {@code catalog.schema.table}.
     */
    static String buildAuthorizationDetails(String tablesCsv) {
        java.util.LinkedHashSet<String> catalogs = new java.util.LinkedHashSet<>();
        java.util.LinkedHashSet<String> schemas = new java.util.LinkedHashSet<>();
        java.util.LinkedHashSet<String> tables = new java.util.LinkedHashSet<>();

        for (String raw : tablesCsv.split(",")) {
            String table = raw.trim();
            if (table.isEmpty()) {
                continue;
            }
            String[] parts = table.split("\\.");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Zerobus table must be fully qualified 'catalog.schema.table', got: " + table);
            }
            catalogs.add(parts[0]);
            schemas.add(parts[0] + "." + parts[1]);
            tables.add(table);
        }

        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (String catalog : catalogs) {
            first = appendDetail(sb, first, "[\"USE CATALOG\"]", "CATALOG", catalog);
        }
        for (String schema : schemas) {
            first = appendDetail(sb, first, "[\"USE SCHEMA\"]", "SCHEMA", schema);
        }
        for (String table : tables) {
            first = appendDetail(sb, first, "[\"SELECT\",\"MODIFY\"]", "TABLE", table);
        }
        return sb.append("]").toString();
    }

    private static boolean appendDetail(StringBuilder sb, boolean first, String privileges, String objectType, String path) {
        if (!first) {
            sb.append(",");
        }
        sb.append("{\"type\":\"unity_catalog_privileges\",\"privileges\":").append(privileges)
                .append(",\"object_type\":\"").append(objectType)
                .append("\",\"object_full_path\":\"").append(path).append("\"}");
        return false;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                handleTokenCallback((OAuthBearerTokenCallback) callback);
            }
            else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private void handleTokenCallback(OAuthBearerTokenCallback callback) throws IOException {
        ZerobusOAuthBearerToken token = currentToken();
        callback.token(token);
    }

    private synchronized ZerobusOAuthBearerToken currentToken() throws IOException {
        long now = System.currentTimeMillis();
        if (staticToken != null) {
            if (cachedToken == null) {
                cachedToken = new ZerobusOAuthBearerToken(staticToken, now + STATIC_TOKEN_LIFETIME_MS, now, principalName);
            }
            return cachedToken;
        }
        ZerobusOAuthBearerToken existing = cachedToken;
        if (existing != null && now < existing.lifetimeMs() - EXPIRY_GUARD_MS) {
            return existing;
        }
        cachedToken = requestToken(now);
        return cachedToken;
    }

    private ZerobusOAuthBearerToken requestToken(long issuedAtMs) throws IOException {
        ZerobusTokenExchange exchange = new ZerobusTokenExchange(
                tokenEndpoint, audience, authorizationDetails, clientId, clientSecret, httpClient);
        ZerobusTokenExchange.Token token = exchange.requestToken();
        long expiryMs = issuedAtMs + token.expiresInSeconds * 1000L;

        LOGGER.debug("Obtained Zerobus OAuth token, expires in {}s", token.expiresInSeconds);
        return new ZerobusOAuthBearerToken(token.accessToken, expiryMs, issuedAtMs, principalName);
    }

    @Override
    public void close() {
        // no resources to release
    }

    // ---- helpers -------------------------------------------------------------------------------

    void setHttpClient(TokenHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    private static String requiredOption(Map<String, ?> options, String key) {
        Object value = options.get(key);
        if (value == null || value.toString().isBlank()) {
            throw new IllegalArgumentException("Missing required JAAS option '" + key + "' for the Zerobus OAUTHBEARER handler");
        }
        return value.toString();
    }

    private static String optionalOption(Map<String, ?> options, String key) {
        Object value = options.get(key);
        return (value == null || value.toString().isBlank()) ? null : value.toString();
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

}
