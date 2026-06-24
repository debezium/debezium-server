/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.util.List;

public interface ZeroBusClient extends AutoCloseable {

    ZeroBusWriteResult write(String targetTable, List<ZeroBusRecord> records) throws Exception;

    @Override
    default void close() throws Exception {
    }
}
