/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.elasticsearch;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.server.Images;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/**
 * Manages the lifecycle of the Elasticsearch container used as the sink target, with basic
 * authentication enabled and TLS disabled so the connector's credential path is exercised without
 * certificate provisioning.
 *
 * @author Chris Cranford
 */
public class ElasticsearchTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchTestResourceLifecycleManager.class);

    public static final String USERNAME = "elastic";
    public static final String PASSWORD = "debezium";

    public static ElasticsearchContainer container;

    private static RestClient restClient;
    private static ElasticsearchClient client;

    @Override
    public Map<String, String> start() {
        try {
            container = new ElasticsearchContainer(DockerImageName.parse(Images.ELASTICSEARCH_IMAGE)
                    .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch"))
                    .withPassword(PASSWORD)
                    .withEnv("xpack.security.http.ssl.enabled", "false")
                    .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");

            container.start();

            final String url = getUrl();
            LOGGER.info("Elasticsearch sink target container started at: {}", url);

            final Map<String, String> config = new ConcurrentHashMap<>();
            config.put("debezium.sink.elasticsearch.connection.url", url);
            config.put("debezium.sink.elasticsearch.connection.auth.mode", "basic");
            config.put("debezium.sink.elasticsearch.connection.username", USERNAME);
            config.put("debezium.sink.elasticsearch.connection.password", PASSWORD);

            return config;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start Elasticsearch target container", e);
        }
    }

    @Override
    public void stop() {
        try {
            if (restClient != null) {
                restClient.close();
                restClient = null;
                client = null;
            }
        }
        catch (IOException e) {
            LOGGER.warn("Error closing Elasticsearch test client", e);
        }
        try {
            if (container != null) {
                container.stop();
                LOGGER.info("Elasticsearch sink target container stopped");
            }
        }
        catch (Exception e) {
            LOGGER.warn("Error stopping Elasticsearch target container", e);
        }
    }

    public static String getUrl() {
        return "http://" + container.getHttpHostAddress();
    }

    /**
     * A client for assertions, independent of the one the connector builds.
     */
    public static synchronized ElasticsearchClient getClient() {
        if (client == null) {
            final BasicCredentialsProvider credentials = new BasicCredentialsProvider();
            credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USERNAME, PASSWORD));
            restClient = RestClient.builder(HttpHost.create(getUrl()))
                    .setHttpClientConfigCallback(b -> b.setDefaultCredentialsProvider(credentials))
                    .build();
            client = new ElasticsearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
        }
        return client;
    }
}
