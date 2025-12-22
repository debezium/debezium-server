/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import java.net.http.HttpRequest;
import java.util.UUID;

public interface Authenticator {
    void setAuthorizationHeader(HttpRequest.Builder httpRequestBuilder, String bodyContent, UUID messageId);

    void authenticate() throws InterruptedException;
}
