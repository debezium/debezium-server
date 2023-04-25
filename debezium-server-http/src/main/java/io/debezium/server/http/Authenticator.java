/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import java.io.IOException;
import java.net.http.HttpRequest;

public interface Authenticator {
    void addAuthorizationHeader(HttpRequest.Builder httpRequestBuilder);

    boolean authenticate() throws InterruptedException, IOException;
}
