/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import com.databricks.zerobus.ArrowStreamConfigurationOptions;
import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.ZerobusArrowStream;
import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusProtoStream;
import com.databricks.zerobus.ZerobusSdk;
import com.google.protobuf.DescriptorProtos.DescriptorProto;

interface ZeroBusSdkAdapter extends AutoCloseable {

    CompletableFuture<ZeroBusJsonStreamAdapter> createJsonStream(String targetTable,
                                                                 String clientId,
                                                                 String clientSecret,
                                                                 StreamConfigurationOptions options);

    CompletableFuture<ZeroBusProtoStreamAdapter> createProtoStream(String targetTable,
                                                                   DescriptorProto descriptor,
                                                                   String clientId,
                                                                   String clientSecret,
                                                                   StreamConfigurationOptions options);

    CompletableFuture<ZeroBusArrowStreamAdapter> createArrowStream(String targetTable,
                                                                   Schema schema,
                                                                   String clientId,
                                                                   String clientSecret,
                                                                   ArrowStreamConfigurationOptions options);

    @Override
    void close();
}

interface ZeroBusJsonStreamAdapter extends AutoCloseable {

    Optional<Long> ingestRecordsOffset(List<String> records) throws Exception;

    void waitForOffset(long offset) throws Exception;

    boolean isClosed();

    @Override
    void close() throws Exception;
}

interface ZeroBusProtoStreamAdapter extends AutoCloseable {

    Optional<Long> ingestRecordsOffset(List<byte[]> records) throws Exception;

    void waitForOffset(long offset) throws Exception;

    boolean isClosed();

    @Override
    void close() throws Exception;
}

interface ZeroBusArrowStreamAdapter extends AutoCloseable {

    Optional<Long> ingestBatch(VectorSchemaRoot root) throws Exception;

    void waitForOffset(long offset) throws Exception;

    boolean isClosed();

    @Override
    void close() throws Exception;
}

final class DatabricksZeroBusSdkAdapter implements ZeroBusSdkAdapter {

    private final ZerobusSdk sdk;

    DatabricksZeroBusSdkAdapter(ZeroBusSinkConfig config) {
        this.sdk = new ZerobusSdk(config.getEndpoint(), config.getWorkspaceUrl());
    }

    @Override
    public CompletableFuture<ZeroBusJsonStreamAdapter> createJsonStream(String targetTable,
                                                                        String clientId,
                                                                        String clientSecret,
                                                                        StreamConfigurationOptions options) {
        return sdk.createJsonStream(targetTable, clientId, clientSecret, options)
                .thenApply(DatabricksZeroBusJsonStreamAdapter::new);
    }

    @Override
    public CompletableFuture<ZeroBusProtoStreamAdapter> createProtoStream(String targetTable,
                                                                          DescriptorProto descriptor,
                                                                          String clientId,
                                                                          String clientSecret,
                                                                          StreamConfigurationOptions options) {
        return sdk.createProtoStream(targetTable, descriptor, clientId, clientSecret, options)
                .thenApply(DatabricksZeroBusProtoStreamAdapter::new);
    }

    @Override
    public CompletableFuture<ZeroBusArrowStreamAdapter> createArrowStream(String targetTable,
                                                                          Schema schema,
                                                                          String clientId,
                                                                          String clientSecret,
                                                                          ArrowStreamConfigurationOptions options) {
        return sdk.createArrowStream(targetTable, schema, clientId, clientSecret, options)
                .thenApply(DatabricksZeroBusArrowStreamAdapter::new);
    }

    @Override
    public void close() {
        sdk.close();
    }
}

final class DatabricksZeroBusJsonStreamAdapter implements ZeroBusJsonStreamAdapter {

    private final ZerobusJsonStream stream;

    DatabricksZeroBusJsonStreamAdapter(ZerobusJsonStream stream) {
        this.stream = stream;
    }

    @Override
    public Optional<Long> ingestRecordsOffset(List<String> records) throws Exception {
        return stream.ingestRecordsOffset(records);
    }

    @Override
    public void waitForOffset(long offset) throws Exception {
        stream.waitForOffset(offset);
    }

    @Override
    public boolean isClosed() {
        return stream.isClosed();
    }

    @Override
    public void close() throws Exception {
        stream.close();
    }
}

final class DatabricksZeroBusArrowStreamAdapter implements ZeroBusArrowStreamAdapter {

    private final ZerobusArrowStream stream;

    DatabricksZeroBusArrowStreamAdapter(ZerobusArrowStream stream) {
        this.stream = stream;
    }

    @Override
    public Optional<Long> ingestBatch(VectorSchemaRoot root) throws Exception {
        return stream.ingestBatch(root);
    }

    @Override
    public void waitForOffset(long offset) throws Exception {
        stream.waitForOffset(offset);
    }

    @Override
    public boolean isClosed() {
        return stream.isClosed();
    }

    @Override
    public void close() throws Exception {
        stream.close();
    }
}

final class DatabricksZeroBusProtoStreamAdapter implements ZeroBusProtoStreamAdapter {

    private final ZerobusProtoStream stream;

    DatabricksZeroBusProtoStreamAdapter(ZerobusProtoStream stream) {
        this.stream = stream;
    }

    @Override
    public Optional<Long> ingestRecordsOffset(List<byte[]> records) throws Exception {
        return stream.ingestRecordsOffset(records);
    }

    @Override
    public void waitForOffset(long offset) throws Exception {
        stream.waitForOffset(offset);
    }

    @Override
    public boolean isClosed() {
        return stream.isClosed();
    }

    @Override
    public void close() throws Exception {
        stream.close();
    }
}
