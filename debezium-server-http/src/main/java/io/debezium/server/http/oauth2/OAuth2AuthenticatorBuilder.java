/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.oauth2;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

public class OAuth2AuthenticatorBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2AuthenticatorBuilder.class);
    private static final Duration DEFAULT_HTTP_TIMEOUT = Duration.ofSeconds(30);

    private static final String PROP_CLIENT_ID = "oauth2.client_id";
    private static final String PROP_CLIENT_SECRET = "oauth2.client_secret";
    private static final String PROP_TOKEN_URL = "oauth2.token_url";
    private static final String PROP_SCOPE = "oauth2.scope";
    private static final String PROP_CLIENT_AUTH_METHOD = "oauth2.client_auth_method";
    private static final String PROP_HTTP_METHOD = "oauth2.token_url.http_method";
    private static final String PROP_ADDITIONAL_PARAMS_PREFIX = "oauth2.params.";

    private URI tokenUri;
    private String clientId;
    private String clientSecret;
    private String scope;
    private OAuth2Authenticator.ClientAuthMethod clientAuthMethod = OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC;
    private OAuth2Authenticator.TokenHttpMethod tokenHttpMethod = OAuth2Authenticator.TokenHttpMethod.POST;
    private Map<String, String> additionalParams = new HashMap<>();
    private Duration httpTimeout = DEFAULT_HTTP_TIMEOUT;
    private HttpClient client;
    private Clock clock = Clock.systemUTC();

    public static OAuth2AuthenticatorBuilder fromConfig(Config config, String propPrefix) {
        OAuth2AuthenticatorBuilder builder = new OAuth2AuthenticatorBuilder();

        builder.setClientId(config.getValue(propPrefix + PROP_CLIENT_ID, String.class));
        builder.setClientSecret(config.getValue(propPrefix + PROP_CLIENT_SECRET, String.class));

        String tokenUrl = config.getValue(propPrefix + PROP_TOKEN_URL, String.class);
        try {
            URI parsedUri = new URI(tokenUrl);
            builder.setTokenUri(parsedUri);
        }
        catch (URISyntaxException e) {
            throw new DebeziumException("Could not parse OAuth2 token URL: " + tokenUrl, e);
        }

        config.getOptionalValue(propPrefix + PROP_SCOPE, String.class)
                .ifPresent(builder::setScope);

        config.getOptionalValue(propPrefix + PROP_CLIENT_AUTH_METHOD, String.class)
                .ifPresent(method -> {
                    switch (method.toLowerCase()) {
                        case "client_secret_basic" -> builder.setClientAuthMethod(OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC);
                        case "client_secret_post" -> builder.setClientAuthMethod(OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_POST);
                        default -> throw new DebeziumException("Unknown OAuth2 client auth method: '" + method
                                + "'. Supported values: client_secret_basic, client_secret_post");
                    }
                });

        config.getOptionalValue(propPrefix + PROP_HTTP_METHOD, String.class)
                .ifPresent(method -> {
                    switch (method.toUpperCase()) {
                        case "POST" -> builder.setTokenHttpMethod(OAuth2Authenticator.TokenHttpMethod.POST);
                        case "GET" -> builder.setTokenHttpMethod(OAuth2Authenticator.TokenHttpMethod.GET);
                        default -> throw new DebeziumException("Unknown OAuth2 token HTTP method: '" + method
                                + "'. Supported values: POST, GET");
                    }
                });

        // Collect any additional params with the oauth2.params. prefix
        String fullParamsPrefix = propPrefix + PROP_ADDITIONAL_PARAMS_PREFIX;
        for (String propertyName : config.getPropertyNames()) {
            if (propertyName.startsWith(fullParamsPrefix)) {
                String paramName = propertyName.substring(fullParamsPrefix.length());
                config.getOptionalValue(propertyName, String.class)
                        .ifPresent(value -> builder.addAdditionalParam(paramName, value));
            }
        }

        return builder;
    }

    public OAuth2AuthenticatorBuilder setTokenUri(URI tokenUri) {
        this.tokenUri = tokenUri;
        return this;
    }

    public OAuth2AuthenticatorBuilder setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public OAuth2AuthenticatorBuilder setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
        return this;
    }

    public OAuth2AuthenticatorBuilder setScope(String scope) {
        this.scope = scope;
        return this;
    }

    public OAuth2AuthenticatorBuilder setClientAuthMethod(OAuth2Authenticator.ClientAuthMethod clientAuthMethod) {
        this.clientAuthMethod = clientAuthMethod;
        return this;
    }

    public OAuth2AuthenticatorBuilder setTokenHttpMethod(OAuth2Authenticator.TokenHttpMethod tokenHttpMethod) {
        this.tokenHttpMethod = tokenHttpMethod;
        return this;
    }

    public OAuth2AuthenticatorBuilder addAdditionalParam(String key, String value) {
        this.additionalParams.put(key, value);
        return this;
    }

    public OAuth2AuthenticatorBuilder setHttpTimeout(Duration httpTimeout) {
        this.httpTimeout = httpTimeout;
        return this;
    }

    public OAuth2AuthenticatorBuilder setHttpClient(HttpClient client) {
        this.client = client;
        return this;
    }

    public OAuth2AuthenticatorBuilder setClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    public OAuth2Authenticator build() {
        if (tokenUri == null) {
            String msg = "Cannot build OAuth2Authenticator. Token URL must be set.";
            LOGGER.error(msg);
            throw new IllegalStateException(msg);
        }
        if (clientId == null) {
            String msg = "Cannot build OAuth2Authenticator. Client ID must be set.";
            LOGGER.error(msg);
            throw new IllegalStateException(msg);
        }
        if (clientSecret == null) {
            String msg = "Cannot build OAuth2Authenticator. Client secret must be set.";
            LOGGER.error(msg);
            throw new IllegalStateException(msg);
        }
        if (client == null) {
            client = HttpClient.newHttpClient();
        }

        return new OAuth2Authenticator(client, clock, tokenUri, clientId, clientSecret, httpTimeout,
                scope, additionalParams, clientAuthMethod, tokenHttpMethod);
    }
}
