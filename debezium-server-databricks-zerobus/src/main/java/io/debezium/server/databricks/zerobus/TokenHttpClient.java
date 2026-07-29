/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;

/**
 * Minimal HTTP abstraction for the Databricks OIDC token exchange, so the exchange can be
 * unit-tested without a live workspace. Shared by both routes that authenticate against Zerobus:
 * the REST route ({@link ZerobusTokenProvider}) and the Kafka {@code OAUTHBEARER} login handler
 * ({@link ZerobusOAuthBearerLoginCallbackHandler}).
 */
interface TokenHttpClient {
    String postForm(String url, String authorizationHeader, String formBody) throws IOException;
}
