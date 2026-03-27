/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link PubSubChangeConsumer}.
 */
public class PubSubChangeConsumerConfig {

    public static final Field PROJECT_ID = Field.create("project.id")
            .withDisplayName("GCP Project ID")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Google Cloud project ID. If not specified, uses the default from the environment.");

    public static final Field ORDERING_ENABLED = Field.create("ordering.enabled")
            .withDisplayName("Message Ordering Enabled")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Enable message ordering to preserve order within a single ordering key.");

    public static final Field ORDERING_KEY = Field.create("ordering.key")
            .withDisplayName("Ordering Key")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Custom ordering key to use for all messages. If not specified, uses the record key.");

    public static final Field NULL_KEY = Field.create("null.key")
            .withDisplayName("Null Key Value")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Ordering key to use when the record key is null.");

    public static final Field BATCH_DELAY_THRESHOLD_MS = Field.create("batch.delay.threshold.ms")
            .withDisplayName("Batch Delay Threshold (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(100)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum delay threshold in milliseconds for batching messages.");

    public static final Field BATCH_ELEMENT_COUNT_THRESHOLD = Field.create("batch.element.count.threshold")
            .withDisplayName("Batch Element Count Threshold")
            .withType(ConfigDef.Type.LONG)
            .withDefault(100L)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of messages to include in a single batch.");

    public static final Field BATCH_REQUEST_BYTE_THRESHOLD = Field.create("batch.request.byte.threshold")
            .withDisplayName("Batch Request Byte Threshold")
            .withType(ConfigDef.Type.LONG)
            .withDefault(9500000L)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum total size in bytes of messages in a single batch.");

    public static final Field FLOWCONTROL_ENABLED = Field.create("flowcontrol.enabled")
            .withDisplayName("Flow Control Enabled")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Enable flow control to limit outstanding messages and bytes.");

    public static final Field FLOWCONTROL_MAX_OUTSTANDING_MESSAGES = Field.create("flowcontrol.max.outstanding.messages")
            .withDisplayName("Max Outstanding Messages")
            .withType(ConfigDef.Type.LONG)
            .withDefault(Long.MAX_VALUE)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of outstanding messages allowed when flow control is enabled.");

    public static final Field FLOWCONTROL_MAX_OUTSTANDING_BYTES = Field.create("flowcontrol.max.outstanding.bytes")
            .withDisplayName("Max Outstanding Bytes")
            .withType(ConfigDef.Type.LONG)
            .withDefault(Long.MAX_VALUE)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum total size in bytes of outstanding messages when flow control is enabled.");

    public static final Field RETRY_TOTAL_TIMEOUT_MS = Field.create("retry.total.timeout.ms")
            .withDisplayName("Retry Total Timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(60000)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Total timeout in milliseconds for retry attempts.");

    public static final Field RETRY_MAX_RPC_TIMEOUT_MS = Field.create("retry.max.rpc.timeout.ms")
            .withDisplayName("Retry Max RPC Timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(10000)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum RPC timeout in milliseconds for retry attempts.");

    public static final Field RETRY_INITIAL_DELAY_MS = Field.create("retry.initial.delay.ms")
            .withDisplayName("Retry Initial Delay (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(5)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Initial delay in milliseconds before first retry attempt.");

    public static final Field RETRY_DELAY_MULTIPLIER = Field.create("retry.delay.multiplier")
            .withDisplayName("Retry Delay Multiplier")
            .withType(ConfigDef.Type.DOUBLE)
            .withDefault("2.0")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Multiplier for exponential backoff between retry attempts.");

    public static final Field RETRY_MAX_DELAY_MS = Field.create("retry.max.delay.ms")
            .withDisplayName("Retry Max Delay (ms)")
            .withType(ConfigDef.Type.LONG)
            .withDefault(Long.MAX_VALUE)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum delay in milliseconds between retry attempts.");

    public static final Field RETRY_INITIAL_RPC_TIMEOUT_MS = Field.create("retry.initial.rpc.timeout.ms")
            .withDisplayName("Retry Initial RPC Timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(10000)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Initial RPC timeout in milliseconds for retry attempts.");

    public static final Field RETRY_RPC_TIMEOUT_MULTIPLIER = Field.create("retry.rpc.timeout.multiplier")
            .withDisplayName("Retry RPC Timeout Multiplier")
            .withType(ConfigDef.Type.DOUBLE)
            .withDefault("2.0")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Multiplier for RPC timeout between retry attempts.");

    public static final Field WAIT_MESSAGE_DELIVERY_TIMEOUT_MS = Field.create("wait.message.delivery.timeout.ms")
            .withDisplayName("Wait Message Delivery Timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(30000)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Timeout in milliseconds to wait for message delivery confirmation.");

    public static final Field CONCURRENCY_THREADS = Field.create("concurrency.threads")
            .withDisplayName("Concurrency Threads")
            .withType(ConfigDef.Type.INT)
            .withDefault(0)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Number of threads for concurrent publishing. 0 means use default.");

    public static final Field COMPRESSION_THRESHOLD_BYTES = Field.create("compression.threshold.bytes")
            .withDisplayName("Compression Threshold (bytes)")
            .withType(ConfigDef.Type.LONG)
            .withDefault(-1L)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Minimum message size in bytes to enable compression. -1 disables compression.");

    public static final Field CHANNEL_SHUTDOWN_TIMEOUT_MS = Field.create("channel.shutdown.timeout.ms")
            .withDisplayName("Channel Shutdown Timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(30000)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Timeout in milliseconds for channel shutdown.");

    public static final Field ADDRESS = Field.create("address")
            .withDisplayName("PubSub Endpoint Address")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Optional PubSub endpoint address (for testing with emulator).");

    public static final Field REGION = Field.create("region")
            .withDisplayName("GCP Region")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("GCP region for regional endpoint (e.g., 'us-east1').");

    // Instance fields
    private String projectId;
    private boolean orderingEnabled;
    private String orderingKey;
    private String nullKey;
    private int maxDelayThresholdMs;
    private long maxBufferSize;
    private long maxBufferBytes;
    private boolean flowControlEnabled;
    private long maxOutstandingMessages;
    private long maxOutstandingRequestBytes;
    private int maxTotalTimeoutMs;
    private int maxRequestTimeoutMs;
    private int initialRetryDelay;
    private double retryDelayMultiplier;
    private long maxRetryDelay;
    private int initialRpcTimeout;
    private double rpcTimeoutMultiplier;
    private int waitMessageDeliveryTimeout;
    private int concurrencyThreads;
    private long compressionBytesThreshold;
    private int channelShutdownTimeout;
    private String address;
    private String region;

    public PubSubChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        projectId = config.getString(PROJECT_ID);
        orderingEnabled = config.getBoolean(ORDERING_ENABLED);
        orderingKey = config.getString(ORDERING_KEY);
        nullKey = config.getString(NULL_KEY);
        maxDelayThresholdMs = config.getInteger(BATCH_DELAY_THRESHOLD_MS);
        maxBufferSize = config.getLong(BATCH_ELEMENT_COUNT_THRESHOLD);
        maxBufferBytes = config.getLong(BATCH_REQUEST_BYTE_THRESHOLD);
        flowControlEnabled = config.getBoolean(FLOWCONTROL_ENABLED);
        maxOutstandingMessages = config.getLong(FLOWCONTROL_MAX_OUTSTANDING_MESSAGES);
        maxOutstandingRequestBytes = config.getLong(FLOWCONTROL_MAX_OUTSTANDING_BYTES);
        maxTotalTimeoutMs = config.getInteger(RETRY_TOTAL_TIMEOUT_MS);
        maxRequestTimeoutMs = config.getInteger(RETRY_MAX_RPC_TIMEOUT_MS);
        initialRetryDelay = config.getInteger(RETRY_INITIAL_DELAY_MS);
        retryDelayMultiplier = Double.parseDouble(config.getString(RETRY_DELAY_MULTIPLIER));
        maxRetryDelay = config.getLong(RETRY_MAX_DELAY_MS);
        initialRpcTimeout = config.getInteger(RETRY_INITIAL_RPC_TIMEOUT_MS);
        rpcTimeoutMultiplier = Double.parseDouble(config.getString(RETRY_RPC_TIMEOUT_MULTIPLIER));
        waitMessageDeliveryTimeout = config.getInteger(WAIT_MESSAGE_DELIVERY_TIMEOUT_MS);
        concurrencyThreads = config.getInteger(CONCURRENCY_THREADS);
        compressionBytesThreshold = config.getLong(COMPRESSION_THRESHOLD_BYTES);
        channelShutdownTimeout = config.getInteger(CHANNEL_SHUTDOWN_TIMEOUT_MS);
        address = config.getString(ADDRESS);
        region = config.getString(REGION);
    }

    public String getProjectId() {
        return projectId;
    }

    public boolean isOrderingEnabled() {
        return orderingEnabled;
    }

    public String getOrderingKey() {
        return orderingKey;
    }

    public String getNullKey() {
        return nullKey;
    }

    public int getMaxDelayThresholdMs() {
        return maxDelayThresholdMs;
    }

    public long getMaxBufferSize() {
        return maxBufferSize;
    }

    public long getMaxBufferBytes() {
        return maxBufferBytes;
    }

    public boolean isFlowControlEnabled() {
        return flowControlEnabled;
    }

    public long getMaxOutstandingMessages() {
        return maxOutstandingMessages;
    }

    public long getMaxOutstandingRequestBytes() {
        return maxOutstandingRequestBytes;
    }

    public int getMaxTotalTimeoutMs() {
        return maxTotalTimeoutMs;
    }

    public int getMaxRequestTimeoutMs() {
        return maxRequestTimeoutMs;
    }

    public int getInitialRetryDelay() {
        return initialRetryDelay;
    }

    public double getRetryDelayMultiplier() {
        return retryDelayMultiplier;
    }

    public long getMaxRetryDelay() {
        return maxRetryDelay;
    }

    public int getInitialRpcTimeout() {
        return initialRpcTimeout;
    }

    public double getRpcTimeoutMultiplier() {
        return rpcTimeoutMultiplier;
    }

    public int getWaitMessageDeliveryTimeout() {
        return waitMessageDeliveryTimeout;
    }

    public int getConcurrencyThreads() {
        return concurrencyThreads;
    }

    public long getCompressionBytesThreshold() {
        return compressionBytesThreshold;
    }

    public int getChannelShutdownTimeout() {
        return channelShutdownTimeout;
    }

    public String getAddress() {
        return address;
    }

    public String getRegion() {
        return region;
    }
}
