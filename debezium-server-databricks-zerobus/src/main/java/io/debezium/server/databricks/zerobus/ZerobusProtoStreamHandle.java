/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusProtoStream;

/** Adapts an SDK Protobuf stream to the sink's encoding-independent stream contract. */
final class ZerobusProtoStreamHandle implements ZerobusStreamHandle<byte[]> {

    private final ZerobusProtoStream stream;

    ZerobusProtoStreamHandle(ZerobusProtoStream stream) {
        this.stream = stream;
    }

    @Override
    public long ingest(byte[] payload) throws ZerobusException {
        return stream.ingestRecordOffset(payload);
    }

    @Override
    public void waitForOffset(long offset) throws ZerobusException {
        stream.waitForOffset(offset);
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
