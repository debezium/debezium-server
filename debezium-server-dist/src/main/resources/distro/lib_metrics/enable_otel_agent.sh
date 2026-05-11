#!/bin/bash
# To attach the OpenTelemetry Java Agent, set OTEL_ENABLED=true
# The SDK is disabled by default even when attached. To activate telemetry
# collection, also set OTEL_SDK_DISABLED=false

if [ "${OTEL_ENABLED}" = "true" ]; then
  export OTEL_SDK_DISABLED=${OTEL_SDK_DISABLED:-true}
  export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-"http://localhost:4318"}
  export OTEL_JMX_CONFIG=${OTEL_JMX_CONFIG:-"config/debezium-jmx-config.yaml"}

  OTEL_AGENT_JAR=$(find lib_metrics -name "opentelemetry-javaagent-*.jar")
  export JAVA_OPTS="-javaagent:${OTEL_AGENT_JAR} ${JAVA_OPTS}"
fi
