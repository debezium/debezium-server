/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.bridge.deployment;

import io.debezium.quarkus.bridge.QuarkusChangeConsumer;
import io.debezium.quarkus.bridge.QuarkusChangeConsumerHolderProducer;
import io.debezium.quarkus.bridge.QuarkusSerializationProducer;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class DebeziumQuarkusBridgeProcessor {

    private static final String FEATURE = "debezium-quarkus-bridge";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    public void bridge(BuildProducer<AdditionalBeanBuildItem> additionalBeanProducer) {
        additionalBeanProducer.produce(AdditionalBeanBuildItem
                .builder()
                .addBeanClasses(QuarkusChangeConsumer.class)
                .setDefaultScope(DotNames.APPLICATION_SCOPED)
                .setUnremovable()
                .build());

        additionalBeanProducer.produce(AdditionalBeanBuildItem
                .builder()
                .addBeanClasses(QuarkusSerializationProducer.class)
                .setDefaultScope(DotNames.APPLICATION_SCOPED)
                .setUnremovable()
                .build());

        additionalBeanProducer.produce(AdditionalBeanBuildItem
                .builder()
                .addBeanClasses(QuarkusChangeConsumerHolderProducer.class)
                .setDefaultScope(DotNames.APPLICATION_SCOPED)
                .setUnremovable()
                .build());

    }
}
