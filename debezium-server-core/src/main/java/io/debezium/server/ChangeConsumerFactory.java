/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static io.debezium.server.DebeziumServer.PROP_SINK_TYPE;

import java.util.Set;
import java.util.stream.Collectors;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;

import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

@ApplicationScoped
public class ChangeConsumerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeConsumerFactory.class);

    private final Config config;
    private final BeanManager beanManager;

    private Bean<DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>> consumerBean;
    private CreationalContext<DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>> consumerBeanCreationalContext;
    private DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> consumer;

    public ChangeConsumerFactory(Config config, BeanManager beanManager) {
        this.config = config;
        this.beanManager = beanManager;
    }

    DefaultChangeConsumer create() {

        final String name = config.getValue(PROP_SINK_TYPE, String.class);

        final Set<Bean<?>> beans = beanManager.getBeans(name).stream()
                .filter(x -> DebeziumEngine.ChangeConsumer.class.isAssignableFrom(x.getBeanClass()))
                .collect(Collectors.toSet());
        LOGGER.debug("Found {} candidate consumer(s)", beans.size());

        if (beans.isEmpty()) {
            throw new DebeziumException("No Debezium consumer named '" + name + "' is available");
        }
        else if (beans.size() > 1) {
            throw new DebeziumException("Multiple Debezium consumers named '" + name + "' were found");
        }

        consumerBean = (Bean<DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>>) beans.iterator().next();
        consumerBeanCreationalContext = beanManager.createCreationalContext(consumerBean);
        consumer = consumerBean.create(consumerBeanCreationalContext);
        LOGGER.info("Consumer '{}' instantiated", consumer.getClass().getName());

        return new DefaultChangeConsumer(consumer, config);
    }

    @PreDestroy
    void cleanup() {
        consumerBean.destroy(consumer, consumerBeanCreationalContext);
    }
}
