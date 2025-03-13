/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab.transforms;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.SmtManager;
import io.debezium.util.Strings;

/**
 * An abstract implementation of the InstructLab sink {@code AddHeader} transformation that specifically
 * is designed to add heads for consumption by the Debezium Server InstructLab sink.
 *
 * A specific transform implementation will require specifying the filename and logic to generate the
 * specific question, answer, and optional context values to append to the InstructLab QNA file.
 *
 * <pre>
 *     "transforms": "fraud",
 *     "transforms.fraud.type": "<fully-qualified-class-name-for-custom-fraud-transform>",
 *     "transforms.fraud.name": "fraud",
 *     "transforms.fraud.filename": "/<path-to-qna-in-taxonomy-domain>/qna.yml"
 * </pre>
 *
 * These transforms will amend the in-flight event and append the following headers:
 *
 * <ul>
 *     <li>qna.[name].filename - which describes which qna.yml file to change</li>
 *     <li>qna.[name].question - the question to append to qna.yml</li>
 *     <li>qna.[name].answer - the answer to append to qna.yml</li>
 *     <li>qna.[name].context - an optional context to append to qna.yml</li>
 * </ul>
 *
 * @author Chris Cranford
 */
public abstract class AbstractAddQnaHeader<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAddQnaHeader.class);

    public static final String QNA_PREFIX = "qna.";
    public static final String ATTRIBUTE_FILENAME = "filename";
    public static final String ATTRIBUTE_QUESTION = "question";
    public static final String ATTRIBUTE_ANSWER = "answer";
    public static final String ATTRIBUTE_CONTEXT = "context";

    private static final Field NAME = Field.create("name")
            .withDisplayName("Header key name to differentiate different header transformation values")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Unique name that should be applied to each transformation instance in the chain");

    private static final Field FILENAME = Field.create("filename")
            .withDisplayName("The full file path to the InstructLab QNA file to modify")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("The full file path to the InstructLab QNA filename to append the question/answers.");

    private static final Field.Set ALL_FIELDS = Field.setOf(NAME, FILENAME);

    private SmtManager<R> smtManager;
    private String name;
    private String fileName;

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);

        boolean failed = false;
        for (Map.Entry<String, ConfigValue> entry : config.validate(ALL_FIELDS).entrySet()) {
            for (String message : entry.getValue().errorMessages()) {
                LOGGER.error("{}", message);
                failed = true;
            }
        }
        if (failed) {
            throw new ConnectException("Failed to configure transformation, see logs for details");
        }

        this.smtManager = new SmtManager<>(config);

        this.name = config.getString(NAME);
        this.fileName = config.getString(FILENAME);
    }

    @Override
    public R apply(R record) {
        if (record.value() != null && smtManager.isValidEnvelope(record)) {
            final QnaEntry entry = createQnaEntry(record).orElse(null);
            if (entry != null) {
                final String question = entry.getQuestion();
                final String answer = entry.getAnswer();
                if (Strings.isNullOrEmpty(question)) {
                    LOGGER.debug("Skipped event because transform did not generate a question");
                }
                else if (Strings.isNullOrEmpty(answer)) {
                    LOGGER.debug("Skipped event because transform did not generate an answer");
                }
                else {
                    LOGGER.debug("Adding header for QNA file '{}'", fileName);
                    record.headers().add(createHeaderName(ATTRIBUTE_FILENAME), fileName, Schema.OPTIONAL_STRING_SCHEMA);
                    record.headers().add(createHeaderName(ATTRIBUTE_QUESTION), question, Schema.OPTIONAL_STRING_SCHEMA);
                    record.headers().add(createHeaderName(ATTRIBUTE_ANSWER), answer, Schema.OPTIONAL_STRING_SCHEMA);

                    final String context = entry.getContext();
                    if (!Strings.isNullOrEmpty(context)) {
                        record.headers().add(createHeaderName(ATTRIBUTE_CONTEXT), context, Schema.OPTIONAL_STRING_SCHEMA);
                    }
                }
            }
        }
        return record;
    }

    /**
     * Creates the unique header key for the given attribute.
     *
     * @param attributeName the attribute name, should not be null or empty
     * @return the header key name
     */
    protected String createHeaderName(String attributeName) {
        return QNA_PREFIX + name + "." + attributeName;
    }

    protected Struct getAfter(R record) {
        if (smtManager.isValidEnvelope(record) && record.valueSchema().type() == Schema.Type.STRUCT) {
            return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        }
        return null;
    }

    /**
     * Creates the QNA entry.
     *
     * @param record the in-flight event record
     * @return optional InstructLab QNA file entry
     */
    protected abstract Optional<QnaEntry> createQnaEntry(R record);

    /**
     * Defines a specific set of InstructLab question-and-answer pairs.
     */
    @Immutable
    protected static class QnaEntry {

        private final String question;
        private final String answer;
        private final String context;

        /**
         * Creates an entry without a context value.
         *
         * @param question the question value
         * @param answer the answer value
         */
        public QnaEntry(String question, String answer) {
            this(question, answer, null);
        }

        /**
         * Creates an entry with a context value.
         *
         * @param question the question value
         * @param answer the answer value
         * @param context the context value
         */
        public QnaEntry(String question, String answer, String context) {
            this.question = question;
            this.answer = answer;
            this.context = context;
        }

        public String getQuestion() {
            return question;
        }

        public String getAnswer() {
            return answer;
        }

        public String getContext() {
            return context;
        }

        @Override
        public String toString() {
            return "QnaEntry{" +
                    "question='" + question + '\'' +
                    ", answer='" + answer + '\'' +
                    ", context='" + context + '\'' +
                    '}';
        }
    }
}
