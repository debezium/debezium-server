/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import com.databricks.zerobus.ZerobusException;

/**
 * The part of a Zerobus stream that the sink drives: enqueue a record, flush to durability, close.
 * <p>
 * The Zerobus SDK exposes one stream class per record encoding ({@code ZerobusJsonStream},
 * {@code ZerobusProtoStream}), and they share only a package-private base class, so no SDK type names
 * both. This interface is that common type, which keeps the batch handling in
 * {@link ZerobusChangeConsumer} independent of how a record is encoded.
 * <p>
 * The payload type parameter ties an encoding to the stream that accepts it: a JSON stream takes the
 * serialized object as a {@code String}, so a record cannot be handed to a stream that does not
 * speak its encoding.
 *
 * @param <P> the encoded record type this stream accepts
 */
interface ZerobusStreamHandle<P> extends AutoCloseable {

    /**
     * Enqueues one encoded record, returning the offset the stream assigned to it. The record is not
     * durable until a subsequent {@link #flush()} succeeds.
     */
    long ingest(P payload) throws ZerobusException;

    /** Waits for every record enqueued on this stream to be acknowledged. This is the durability barrier. */
    void flush() throws ZerobusException;

    @Override
    void close() throws ZerobusException;
}
