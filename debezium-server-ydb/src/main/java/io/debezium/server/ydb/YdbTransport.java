/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import tech.ydb.auth.NopAuthProvider;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.topic.TopicClient;

final class YdbTransport {

    record Clients(GrpcTransport transport, TopicClient topicClient) {
    }

    private YdbTransport() {
    }

    static Clients open(YdbChangeConsumerConfig cfg) {
        GrpcTransport transport = openTransport(cfg);
        try {
            return new Clients(transport, TopicClient.newClient(transport).build());
        }
        catch (Exception e) {
            transport.close();
            throw e;
        }
    }

    private static GrpcTransport openTransport(YdbChangeConsumerConfig cfg) {
        GrpcTransportBuilder builder = GrpcTransport.forEndpoint(cfg.getEndpoint(), cfg.getDatabase());
        if (cfg.getAuthUser() != null && !cfg.getAuthUser().isBlank()) {
            String pwd = cfg.getAuthPassword() == null ? "" : cfg.getAuthPassword();
            builder.withAuthProvider(new StaticCredentials(cfg.getAuthUser(), pwd));
        }
        else {
            builder.withAuthProvider(NopAuthProvider.INSTANCE);
        }
        return builder.build();
    }
}
