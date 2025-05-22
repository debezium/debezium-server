/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

/**
 * The list of container images used in testing
 */
public class Images {

    private static final String PRAVEGA_VERSION = "0.13.0";
    private static final String PULSAR_VERSION = "2.5.2";
    private static final String MILVUS_VERSION = "v2.5.4";
    private static final String QDRANT_VERSION = "v1.14.0";

    public static final String PRAVEGA_IMAGE = "mirror.gcr.io/pravega/pravega:" + PRAVEGA_VERSION;
    public static final String REDIS_IMAGE = "mirror.gcr.io/library/redis";
    public static final String PUB_SUB_EMULATOR_IMAGE = "gcr.io/google.com/cloudsdktool/cloud-sdk:380.0.0-emulators";
    public static final String KAFKA_IMAGE = "mirror.gcr.io/confluentinc/cp-kafka:5.4.3";
    public static final String PULSAR_IMAGE = "mirror.gcr.io/apachepulsar/pulsar:" + PULSAR_VERSION;
    public static final String MILVUS_IMAGE = "mirror.gcr.io/milvusdb/milvus:" + MILVUS_VERSION;
    public static final String QDRANT_IMAGE = "qdrant/qdrant:" + QDRANT_VERSION;
    public static final String INFINISPAN_IMAGE = "quay.io/infinispan/server:" + System.getProperty("tag.infinispan", "latest");
    public static final String LOCALSTACK_IMAGE = "mirror.gcr.io/localstack/localstack";
    public static final String RABBITMQ_IMAGE = "mirror.gcr.io/library/rabbitmq:3.12.9-management";
    public static final String ROCKETMQ_IMAGE = "mirror.gcr.io/apache/rocketmq";
    public static final String WIREMOCK_IMAGE = "mirror.gcr.io/wiremock/wiremock:3.2.0";
    public static final String NATS_IMAGE = "mirror.gcr.io/library/nats:latest";
    public static final String NATS_STREAMING_IMAGE = "mirror.gcr.io/library/nats-streaming:latest";
}
