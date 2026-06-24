/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.arrow.vector.VectorSchemaRoot;

import com.databricks.zerobus.ArrowStreamConfigurationOptions;
import com.databricks.zerobus.IPCCompressionType;
import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.ZerobusException;

import io.debezium.DebeziumException;

/**
 * ZeroBus client backed by the Databricks ZeroBus Java SDK.
 */
public class DatabricksZeroBusClient implements ZeroBusClient {

    private final ZeroBusSinkConfig config;
    private final ZeroBusSdkAdapter sdk;
    private final StreamConfigurationOptions streamOptions;
    private final ArrowStreamConfigurationOptions arrowStreamOptions;
    private final ZeroBusJsonSerializer jsonSerializer;
    private final ZeroBusProtobufSerializer protobufSerializer;
    private ZeroBusArrowSerializer arrowSerializer;
    private final Map<String, ZeroBusJsonStreamAdapter> jsonStreams = new HashMap<>();
    private final Map<String, ZeroBusProtoStreamAdapter> protobufStreams = new HashMap<>();
    private final Map<String, ZeroBusArrowStreamAdapter> arrowStreams = new HashMap<>();

    public DatabricksZeroBusClient(ZeroBusSinkConfig config) {
        this(config, new DatabricksZeroBusSdkAdapter(config), new ZeroBusJsonSerializer(), new ZeroBusProtobufSerializer(), null);
    }

    DatabricksZeroBusClient(ZeroBusSinkConfig config, ZeroBusSdkAdapter sdk, ZeroBusJsonSerializer jsonSerializer, ZeroBusProtobufSerializer protobufSerializer, ZeroBusArrowSerializer arrowSerializer) {
        this.config = config;
        this.sdk = sdk;
        this.jsonSerializer = jsonSerializer;
        this.protobufSerializer = protobufSerializer;
        this.arrowSerializer = arrowSerializer;
        this.streamOptions = StreamConfigurationOptions.builder()
                .setMaxInflightRecords(config.getMaxInflightRecords())
                .setRecovery(true)
                .setRecoveryRetries(config.getRetries())
                .setServerLackOfAckTimeoutMs(toIntMillis(config.getTimeout().toMillis(), ZeroBusSinkConfig.TIMEOUT_MS.name()))
                .setFlushTimeoutMs(toIntMillis(config.getTimeout().toMillis(), ZeroBusSinkConfig.TIMEOUT_MS.name()))
                .build();
        this.arrowStreamOptions = ArrowStreamConfigurationOptions.builder()
                .setMaxInflightBatches(config.getMaxInflightBatches())
                .setRecovery(true)
                .setRecoveryRetries(config.getRetries())
                .setServerLackOfAckTimeoutMs(config.getTimeout().toMillis())
                .setFlushTimeoutMs(config.getTimeout().toMillis())
                .setConnectionTimeoutMs(config.getTimeout().toMillis())
                .setIpcCompression(IPCCompressionType.NONE)
                .build();
    }

    @Override
    public synchronized ZeroBusWriteResult write(String targetTable, List<ZeroBusRecord> records) throws Exception {
        if (records.isEmpty()) {
            return ZeroBusWriteResult.acknowledged(0);
        }

        try {
            if (ZeroBusSinkConfig.RECORD_FORMAT_PROTOBUF.equals(config.getRecordFormat())) {
                return writeProtobuf(targetTable, records);
            }
            if (ZeroBusSinkConfig.RECORD_FORMAT_ARROW.equals(config.getRecordFormat())) {
                return writeArrow(targetTable, records);
            }
            return writeJson(targetTable, records);
        }
        catch (NonRetriableException e) {
            throw new DebeziumException("Non-retriable ZeroBus write failure for table " + targetTable, e);
        }
        catch (ZerobusException e) {
            throw new ZeroBusRetriableException("Retryable ZeroBus write failure for table " + targetTable, e);
        }
        catch (CompletionException e) {
            throw translateCompletionException(targetTable, e);
        }
    }

    private ZeroBusWriteResult writeJson(String targetTable, List<ZeroBusRecord> records) throws Exception {
        ZeroBusJsonStreamAdapter stream = jsonStreamFor(targetTable);
        List<String> payloads = new ArrayList<>(records.size());
        for (ZeroBusRecord record : records) {
            payloads.add(jsonSerializer.serialize(record));
        }

        Optional<Long> offset = stream.ingestRecordsOffset(payloads);
        if (offset.isPresent()) {
            stream.waitForOffset(offset.get());
        }
        return ZeroBusWriteResult.acknowledged(records.size());
    }

    private ZeroBusWriteResult writeArrow(String targetTable, List<ZeroBusRecord> records) throws Exception {
        ZeroBusArrowStreamAdapter stream = arrowStreamFor(targetTable);
        try (VectorSchemaRoot root = arrowSerializer().serialize(records)) {
            Optional<Long> offset = stream.ingestBatch(root);
            if (offset.isPresent()) {
                stream.waitForOffset(offset.get());
            }
        }
        return ZeroBusWriteResult.acknowledged(records.size());
    }

    private ZeroBusWriteResult writeProtobuf(String targetTable, List<ZeroBusRecord> records) throws Exception {
        ZeroBusProtoStreamAdapter stream = protobufStreamFor(targetTable);
        List<byte[]> payloads = new ArrayList<>(records.size());
        for (ZeroBusRecord record : records) {
            payloads.add(protobufSerializer.serialize(record));
        }

        Optional<Long> offset = stream.ingestRecordsOffset(payloads);
        if (offset.isPresent()) {
            stream.waitForOffset(offset.get());
        }
        return ZeroBusWriteResult.acknowledged(records.size());
    }

    private ZeroBusJsonStreamAdapter jsonStreamFor(String targetTable) throws Exception {
        ZeroBusJsonStreamAdapter existing = jsonStreams.get(targetTable);
        if (existing != null && !existing.isClosed()) {
            return existing;
        }

        try {
            ZeroBusJsonStreamAdapter stream = sdk.createJsonStream(
                    targetTable,
                    config.getOauth2ClientId(),
                    config.getOauth2ClientSecret(),
                    streamOptions).get(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            jsonStreams.put(targetTable, stream);
            return stream;
        }
        catch (CompletionException e) {
            throw translateCompletionException(targetTable, e);
        }
        catch (ExecutionException e) {
            throw translateThrowable(targetTable, e.getCause() == null ? e : e.getCause());
        }
        catch (TimeoutException e) {
            throw new ZeroBusRetriableException("Timed out creating ZeroBus stream for table " + targetTable, e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private ZeroBusProtoStreamAdapter protobufStreamFor(String targetTable) throws Exception {
        ZeroBusProtoStreamAdapter existing = protobufStreams.get(targetTable);
        if (existing != null && !existing.isClosed()) {
            return existing;
        }

        try {
            ZeroBusProtoStreamAdapter stream = sdk.createProtoStream(
                    targetTable,
                    protobufSerializer.descriptorProto(),
                    config.getOauth2ClientId(),
                    config.getOauth2ClientSecret(),
                    streamOptions).get(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            protobufStreams.put(targetTable, stream);
            return stream;
        }
        catch (CompletionException e) {
            throw translateCompletionException(targetTable, e);
        }
        catch (ExecutionException e) {
            throw translateThrowable(targetTable, e.getCause() == null ? e : e.getCause());
        }
        catch (TimeoutException e) {
            throw new ZeroBusRetriableException("Timed out creating ZeroBus stream for table " + targetTable, e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private ZeroBusArrowStreamAdapter arrowStreamFor(String targetTable) throws Exception {
        ZeroBusArrowStreamAdapter existing = arrowStreams.get(targetTable);
        if (existing != null && !existing.isClosed()) {
            return existing;
        }

        try {
            ZeroBusArrowStreamAdapter stream = sdk.createArrowStream(
                    targetTable,
                    arrowSerializer().schema(),
                    config.getOauth2ClientId(),
                    config.getOauth2ClientSecret(),
                    arrowStreamOptions).get(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            arrowStreams.put(targetTable, stream);
            return stream;
        }
        catch (CompletionException e) {
            throw translateCompletionException(targetTable, e);
        }
        catch (ExecutionException e) {
            throw translateThrowable(targetTable, e.getCause() == null ? e : e.getCause());
        }
        catch (TimeoutException e) {
            throw new ZeroBusRetriableException("Timed out creating ZeroBus stream for table " + targetTable, e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private Exception translateCompletionException(String targetTable, CompletionException e) {
        Throwable cause = e.getCause() == null ? e : e.getCause();
        return translateThrowable(targetTable, cause);
    }

    private Exception translateThrowable(String targetTable, Throwable cause) {
        if (cause instanceof NonRetriableException) {
            return new DebeziumException("Non-retriable ZeroBus stream failure for table " + targetTable, cause);
        }
        if (cause instanceof ZerobusException) {
            return new ZeroBusRetriableException("Retryable ZeroBus stream failure for table " + targetTable, cause);
        }
        return new DebeziumException("Failed to create ZeroBus stream for table " + targetTable, cause);
    }

    private static int toIntMillis(long millis, String fieldName) {
        if (millis > Integer.MAX_VALUE) {
            throw new DebeziumException("ZeroBus " + fieldName + " must not exceed " + Integer.MAX_VALUE);
        }
        return (int) millis;
    }

    private ZeroBusArrowSerializer arrowSerializer() {
        if (arrowSerializer == null) {
            arrowSerializer = new ZeroBusArrowSerializer();
        }
        return arrowSerializer;
    }

    @Override
    public synchronized void close() throws Exception {
        Exception failure = null;
        for (ZeroBusJsonStreamAdapter stream : jsonStreams.values()) {
            try {
                stream.close();
            }
            catch (Exception e) {
                if (failure == null) {
                    failure = e;
                }
            }
        }
        for (ZeroBusProtoStreamAdapter stream : protobufStreams.values()) {
            try {
                stream.close();
            }
            catch (Exception e) {
                if (failure == null) {
                    failure = e;
                }
            }
        }
        for (ZeroBusArrowStreamAdapter stream : arrowStreams.values()) {
            try {
                stream.close();
            }
            catch (Exception e) {
                if (failure == null) {
                    failure = e;
                }
            }
        }
        jsonStreams.clear();
        protobufStreams.clear();
        arrowStreams.clear();
        try {
            sdk.close();
        }
        catch (Exception e) {
            if (failure == null) {
                failure = e;
            }
        }
        if (arrowSerializer != null) {
            try {
                arrowSerializer.close();
            }
            catch (Exception e) {
                if (failure == null) {
                    failure = e;
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
