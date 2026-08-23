/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.ZerobusSdk;

/**
 * Strategy for one Zerobus envelope encoding: it turns the stable CDC envelope into the encoded
 * payload and opens a stream that accepts that encoding. The two collaborate through the payload type
 * {@code P}, which is why they live behind one interface rather than as separate concerns &mdash; a
 * JSON encoding produces a {@code String} and needs a JSON stream, a Protobuf encoding produces
 * {@code byte[]} and needs a Protobuf stream, and neither payload can be handed to the other's stream.
 * <p>
 * Binding both to {@code P} keeps the batch write path in {@link ZerobusChangeConsumer} generic: it
 * serializes, size-checks and ingests without ever branching on the configured format. Exactly one
 * implementation is created per consumer, chosen by {@link #create(ZerobusChangeConsumerConfig)}.
 *
 * @param <P> the encoded record type this strategy produces and its stream accepts
 */
interface ZerobusEnvelopeSerializer<P> {

    /** Encodes the CDC envelope into this strategy's payload type. */
    P serialize(ZerobusEnvelope record);

    /** The encoded size of {@code payload} in bytes, used to enforce {@code max.record.bytes}. */
    int byteSize(P payload);

    /** Opens a Zerobus stream bound to {@code table} that accepts this strategy's encoding. */
    ZerobusStreamHandle<P> openStream(ZerobusSdk sdk, String table, StreamConfigurationOptions options,
                                      String clientId, String clientSecret);

    /**
     * Creates the single serializer the configured {@code record.format} selects, so only the chosen
     * encoding is initialized rather than one of each.
     */
    static ZerobusEnvelopeSerializer<?> create(ZerobusChangeConsumerConfig config) {
        return switch (config.getRecordFormat()) {
            case PROTOBUF -> new ZerobusProtobufEnvelopeSerializer();
            case JSON -> new ZerobusJsonEnvelopeSerializer(config);
        };
    }
}
