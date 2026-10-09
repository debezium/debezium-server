/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.elasticsearch.quarkus;

import io.debezium.connector.elasticsearch.naming.ElasticsearchCollectionNamingStrategy;
import io.debezium.sink.naming.DefaultCollectionNamingStrategy;
import io.debezium.sink.naming.PassthroughCollectionNamingStrategy;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Registers the classes the Elasticsearch sink connector instantiates by name from its
 * configuration so they remain reachable in a native image.
 *
 * @author Chris Cranford
 */
@RegisterForReflection(targets = {
        ElasticsearchCollectionNamingStrategy.class,
        DefaultCollectionNamingStrategy.class,
        PassthroughCollectionNamingStrategy.class
})
public class ElasticsearchSinkReflectionConfiguration {
}
