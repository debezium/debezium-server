/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusJsonStream;

/**
 * Adapts the SDK's {@link ZerobusJsonStream} to {@link ZerobusStreamHandle}, which is the only thing
 * the sink's batch handling needs from a stream. A second implementation can wrap
 * {@code ZerobusProtoStream} without the batch handling knowing which one it drives.
 */
final class ZerobusJsonStreamHandle implements ZerobusStreamHandle<String> {

    private final ZerobusJsonStream stream;

    ZerobusJsonStreamHandle(ZerobusJsonStream stream) {
        this.stream = stream;
    }

    @Override
    public long ingest(String json) throws ZerobusException {
        return stream.ingestRecordOffset(json);
    }

    @Override
    public void flush() throws ZerobusException {
        stream.flush();
    }

    @Override
    public void close() throws ZerobusException {
        stream.close();
    }
}
