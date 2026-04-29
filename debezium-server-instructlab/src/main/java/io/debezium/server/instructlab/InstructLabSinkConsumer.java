/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.DebeziumServerConsumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.annotation.Immutable;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerSink;
import io.debezium.util.Strings;

/**
 * An implementation of the {@link ChangeConsumer} interface that appends change event messages
 * to the InstructLab's {@code qna.yml} file, to improve training of models.
 *
 * @author Chris Cranford
 */
@Named("instructlab")
@Dependent
public class InstructLabSinkConsumer extends BaseChangeConsumer implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstructLabSinkConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.instructlab.";
    private static final String TAXONOMY_PREFIX = PROP_PREFIX + "taxonomy.";

    private final List<TaxonomyMapping> mappings = new ArrayList<>();
    private InstructLabSinkConsumerConfig config;

    @PostConstruct
    void configure() {
        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new InstructLabSinkConsumerConfig(configuration);

        final String taxonomyBasePath = config.getTaxonomyBasePath();
        final String[] taxonomyNames = config.getTaxonomies().split(",");
        for (String taxonomyName : taxonomyNames) {
            // Read mapping question, answer, and optional context configs
            final MappingValue question = MappingValue.from(mpConfig.getValue(TAXONOMY_PREFIX + taxonomyName + ".question", String.class));
            final MappingValue answer = MappingValue.from(mpConfig.getValue(TAXONOMY_PREFIX + taxonomyName + ".answer", String.class));
            final MappingValue context = mpConfig.getOptionalValue(TAXONOMY_PREFIX + taxonomyName + ".context", String.class)
                    .map(MappingValue::from)
                    .orElse(null);

            final String topicRegEx = mpConfig.getOptionalValue(TAXONOMY_PREFIX + taxonomyName + ".topic", String.class).orElse(".*");

            // Compute mapping qna.yml filename from taxonomy domain and base paths
            final String fileName = createTaxonomyQnAPath(taxonomyBasePath,
                    mpConfig.getValue(TAXONOMY_PREFIX + taxonomyName + ".domain", String.class));

            LOGGER.info("Configured taxonomy mapping '{}' to taxonomy {}", taxonomyName, fileName);
            mappings.add(new TaxonomyMapping(taxonomyName, question, answer, context, Pattern.compile(topicRegEx), fileName));
        }

        if (mappings.isEmpty()) {
            throw new DebeziumException("No taxonomy mappings configured.");
        }
    }

    @PreDestroy
    @Override
    public void close() {
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events)
            throws InterruptedException {
        LOGGER.trace("Processing batch of {} events.", events.records().size());
        // Read each event and build file batch data
        final Map<String, QnaFile> batchFiles = new HashMap<>();
        for (BatchEvent record : events.records()) {
            if (record.value() != null) {
                for (TaxonomyMapping mapping : mappings) {
                    // Filter by topic
                    final Matcher matcher = mapping.topicPattern().matcher(events.destination());
                    if (!matcher.matches()) {
                        LOGGER.trace("Skipped event for taxonomy {}, topic {} did not match.", mapping.name(), events.destination());
                        continue;
                    }

                    LOGGER.trace("Processing taxonomy {} for topic {}", mapping.name(), events.destination());

                    final String question = getChangeMappingValue(record, mapping.question());
                    final String answer = getChangeMappingValue(record, mapping.answer());
                    if (!Strings.isNullOrEmpty(question) && !Strings.isNullOrEmpty(answer)) {
                        final QnaFile file = batchFiles.computeIfAbsent(mapping.fileName(), QnaFile::new);
                        final String context = getChangeMappingValue(record, mapping.context());
                        LOGGER.trace("Adding seed example to taxonomy file '{}'", mapping.fileName());
                        file.addSeedExample(question, answer, context);
                    }
                    else {
                        LOGGER.trace("Cannot add seed example for taxonomy file '{}', question or answer is empty.", mapping.fileName());
                    }
                }
            }
            record.commit();
        }

        // Flush files
        for (QnaFile file : batchFiles.values()) {
            try {
                file.flush();
            }
            catch (IOException e) {
                throw new DebeziumException("Failed to flush file: " + file.getFileName(), e);
            }
        }
    }

    /**
     * Reads a specific mapping value reference as a String from the change event.
     *
     * @param changeEvent the change event, should not be {@code null}
     * @param mappingValue the mapping value, may be {@code null}
     * @return the mapping value reference or {@code null} if not found
     */
    private String getChangeMappingValue(BatchEvent changeEvent, MappingValue mappingValue) {
        if (mappingValue != null) {
            if (mappingValue.isHeader()) {
                for (Header header : changeEvent.record().headers()) {
                    if (header.key().equals(mappingValue.getValue())) {
                        return String.valueOf(header.value());
                    }
                }
            }
            else if (mappingValue.isField()) {
                if (changeEvent.record().valueSchema() != null && Envelope.isEnvelopeSchema(changeEvent.record().valueSchema())) {
                    // Debezium event
                    final Struct after = ((Struct) changeEvent.record().value()).getStruct(Envelope.FieldName.AFTER);
                    if (after != null && after.schema().field(mappingValue.getValue()) != null) {
                        return String.valueOf(after.get(mappingValue.getValue()));
                    }
                }
                else if (changeEvent.record().valueSchema() != null) {
                    // Flattened event
                    final Struct struct = (Struct) changeEvent.record().value();
                    if (struct != null && struct.schema().field(mappingValue.getValue()) != null) {
                        return String.valueOf(struct.get(mappingValue.getValue()));
                    }
                }
            }
            else if (mappingValue.isConstant()) {
                return mappingValue.getValue();
            }
        }
        return null;
    }

    private String createTaxonomyQnAPath(String basePath, String domain) {
        return Stream.concat(Stream.concat(Stream.of(basePath), Arrays.stream(domain.split("/"))), Stream.of("qna.yml")).collect(Collectors.joining("/"));
    }

    /**
     * Defines a taxonomy configuration-based mapping
     */
    @Immutable
    private record TaxonomyMapping(String name, MappingValue question, MappingValue answer, MappingValue context, Pattern topicPattern, String fileName) {

    }

    /**
     * A mapping value represents an encoded representation for a taxonomy mapping attribute.
     */
    @Immutable
    @VisibleForTesting
    public static class MappingValue {

        private final static String FIELD_PREFIX = "value:";
        private final static String HEADER_PREFIX = "header:";

        private final boolean header;
        private final boolean field;
        private final boolean constant;
        private final String value;

        private MappingValue(boolean header, boolean field, boolean constant, String value) {
            this.header = header;
            this.field = field;
            this.constant = constant;
            this.value = value;
        }

        public boolean isHeader() {
            return header;
        }

        public boolean isField() {
            return field;
        }

        public boolean isConstant() {
            return constant;
        }

        public String getValue() {
            return value;
        }

        static MappingValue from(String mapping) {
            Objects.requireNonNull(mapping, "The mapping must not be null or empty");
            if (mapping.startsWith(FIELD_PREFIX)) {
                return new MappingValue(false, true, false, mapping.substring(mapping.indexOf(":") + 1));
            }
            else if (mapping.startsWith(HEADER_PREFIX)) {
                return new MappingValue(true, false, false, mapping.substring(mapping.indexOf(":") + 1));
            }
            return new MappingValue(false, false, true, mapping);
        }
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                InstructLabSinkConsumerConfig.TAXONOMY_BASE_PATH,
                InstructLabSinkConsumerConfig.TAXONOMIES,
                InstructLabSinkConsumerConfig.TAXONOMY_QUESTION,
                InstructLabSinkConsumerConfig.TAXONOMY_ANSWER,
                InstructLabSinkConsumerConfig.TAXONOMY_CONTEXT,
                InstructLabSinkConsumerConfig.TAXONOMY_TOPIC,
                InstructLabSinkConsumerConfig.TAXONOMY_DOMAIN);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
