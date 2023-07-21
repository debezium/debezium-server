#!/bin/bash
#
# In order to run Debezium for the Cassandra database, export the CASSANDRA_VERSION environment variable
# with one of the following values: v3, v4, dse.
#
# Copyright Debezium Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
LIB_PATH="lib/*"

if [ "$OSTYPE" = "msys" ] || [ "$OSTYPE" = "cygwin" ]; then
  PATH_SEP=";"
else
  PATH_SEP=":"
fi

if [ -n "$EXTRA_CONNECTOR" ]; then
  EXTRA_CONNECTOR=${EXTRA_CONNECTOR,,}
  export EXTRA_CONNECTOR_DIR="connectors/debezium-connector-${EXTRA_CONNECTOR}"

  echo "Connector - ${EXTRA_CONNECTOR} loaded from ${EXTRA_CONNECTOR_DIR}"

  if [ -f "${EXTRA_CONNECTOR_DIR}/jdk_java_options.sh" ]; then
    source "${EXTRA_CONNECTOR_DIR}/jdk_java_options.sh"
  fi

  EXTRA_CLASS_PATH=""
  if [ -f "${EXTRA_CONNECTOR_DIR}/extra_class_path.sh" ]; then
    source "${EXTRA_CONNECTOR_DIR}/extra_class_path.sh"
    LIB_PATH=$EXTRA_CLASS_PATH$LIB_PATH
  fi
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA_BINARY="java"
else
  JAVA_BINARY="$JAVA_HOME/bin/java"
fi

RUNNER=$(ls debezium-server-*runner.jar)

ENABLE_DEBEZIUM_SCRIPTING=${ENABLE_DEBEZIUM_SCRIPTING:-false}
if [[ "${ENABLE_DEBEZIUM_SCRIPTING}" == "true" ]]; then
  LIB_PATH=$LIB_PATH$PATH_SEP"lib_opt/*"
fi

source ./jmx/enable_jmx.sh

exec "$JAVA_BINARY" $DEBEZIUM_OPTS $JAVA_OPTS -cp \
    $RUNNER$PATH_SEP"conf"$PATH_SEP$LIB_PATH io.debezium.server.Main