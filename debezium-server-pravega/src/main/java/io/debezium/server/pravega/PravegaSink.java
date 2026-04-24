/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pravega;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;

public interface PravegaSink extends AutoCloseable {

    void handle(CapturingEvents<BatchEvent> events) throws InterruptedException;

}
