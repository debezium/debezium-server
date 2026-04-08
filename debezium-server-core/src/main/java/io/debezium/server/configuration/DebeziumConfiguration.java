package io.debezium.server.configuration;

import static io.debezium.server.configuration.DebeziumProperties.PROP_SINK_TYPE;

import java.nio.file.Paths;
import java.util.NoSuchElementException;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.Quarkus;

public class DebeziumConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumConfiguration.class);

    public static Config get() {
        final Config config = ConfigProvider.getConfig();
        // Check config and exit if we cannot load mandatory option.
        try {
            config.getValue(PROP_SINK_TYPE, String.class);
        }
        catch (NoSuchElementException e) {
            final String configFile = Paths.get(System.getProperty("user.dir"), "config", "application.properties").toString();
            final String errorMessage = String
                    .format("Failed to load mandatory config value '%s'. Please check you have a correct Debezium server config in %s or required "
                            + "properties are defined via system or environment variables.", PROP_SINK_TYPE, configFile);

            // Print to stderr in case of logging misconfiguration.
            // CHECKSTYLE IGNORE check FOR NEXT 1 LINES
            System.err.println(errorMessage);
            LOGGER.error(errorMessage);

            Quarkus.asyncExit();
        }
        return config;
    }
}
