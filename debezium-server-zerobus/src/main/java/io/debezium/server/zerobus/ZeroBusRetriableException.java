/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

public class ZeroBusRetriableException extends Exception {

    public ZeroBusRetriableException(String message) {
        super(message);
    }

    public ZeroBusRetriableException(String message, Throwable cause) {
        super(message, cause);
    }
}
