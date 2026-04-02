/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link InstructLabSinkConsumer}.
 */
public class InstructLabSinkConsumerConfig {

    public static final Field TAXONOMY_BASE_PATH = Field.create("taxonomy.base.path")
            .withDisplayName("Taxonomy Base Path")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Base path for the InstructLab taxonomy directory.");

    public static final Field TAXONOMIES = Field.create("taxonomies")
            .withDisplayName("Taxonomies")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Comma-separated list of taxonomy names to configure.");

    public static final Field TAXONOMY_QUESTION = Field.create("taxonomy.<name>.question")
            .withDisplayName("Taxonomy Question Mapping")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Mapping for the question field. Use 'value:<field>' for a value field, 'header:<name>' for a header, or a constant string.");

    public static final Field TAXONOMY_ANSWER = Field.create("taxonomy.<name>.answer")
            .withDisplayName("Taxonomy Answer Mapping")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Mapping for the answer field. Use 'value:<field>' for a value field, 'header:<name>' for a header, or a constant string.");

    public static final Field TAXONOMY_CONTEXT = Field.create("taxonomy.<name>.context")
            .withDisplayName("Taxonomy Context Mapping")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Optional mapping for the context field. Use 'value:<field>' for a value field, 'header:<name>' for a header, or a constant string.");

    public static final Field TAXONOMY_TOPIC = Field.create("taxonomy.<name>.topic")
            .withDisplayName("Taxonomy Topic Pattern")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(".*")
            .withDescription("Regular expression pattern to filter events by topic for this taxonomy.");

    public static final Field TAXONOMY_DOMAIN = Field.create("taxonomy.<name>.domain")
            .withDisplayName("Taxonomy Domain")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Domain path for the taxonomy (e.g., 'knowledge/database/postgresql').");

    private final String taxonomyBasePath;
    private final String taxonomies;

    public InstructLabSinkConsumerConfig(Configuration config) {
        this.taxonomyBasePath = config.getString(TAXONOMY_BASE_PATH);
        this.taxonomies = config.getString(TAXONOMIES);
    }

    public String getTaxonomyBasePath() {
        return taxonomyBasePath;
    }

    public String getTaxonomies() {
        return taxonomies;
    }
}
