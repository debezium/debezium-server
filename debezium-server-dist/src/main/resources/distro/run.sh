#!/bin/bash
#
# Copyright Debezium Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

if [ -z "$JAVA_HOME" ]; then
  JAVA_BINARY="java"
else
  JAVA_BINARY="$JAVA_HOME/bin/java"
fi

if [ "$OSTYPE" = "msys" ] || [ "$OSTYPE" = "cygwin" ]; then
  PATH_SEP=";"
else
  PATH_SEP=":"
fi

RUNNER=$(ls debezium-server-*runner.jar)

ENABLE_DEBEZIUM_SCRIPTING=${ENABLE_DEBEZIUM_SCRIPTING:-false}
LIB_PATH="lib/*"
if [[ "${ENABLE_DEBEZIUM_SCRIPTING}" == "true" ]]; then
    LIB_PATH=$LIB_PATH$PATH_SEP"lib_opt/*"
fi

if [[ -z "${CASSANDRA_VERSION}"]]; then
  CASSANDRA_VERSION="${CASSANDRA_VERSION^^}"
  if [[ "${CASSANDRA_VERSION}" == "V3" ]]; then
    CONNECTOR_LIB="./connectors/cassandra/v3/*"
  elsif [[ "${CASSANDRA_VERSION}" == "V4" ]]; then
    CONNECTOR_LIB="./connectors/cassandra/ver4/debezium-connector-cassandra-4/*"
  elsif [[ "${CASSANDRA_VERSION}" == "DSE" ]]; then
    if [[ ! -z "${$DSE_HOME}"]]; then
      echo "Error: DSE_HOME variable is not defined."
      exit -1
    fi
    CONNECTOR_LIB="./connectors/cassandra/dse/debezium-connector-dse/*"
  else
  fi

  export JDK_JAVA_OPTIONS="--add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED \
  --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED \
  --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED \
  --add-exports java.sql/java.sql=ALL-UNNAMED  \
  --add-opens java.base/java.lang.module=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.loader=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.math=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.module=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED \
  --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED"

  exec "$JAVA_BINARY" $DEBEZIUM_OPTS $JAVA_OPTS -cp \
  "$RUNNER"$PATH_SEP"conf"$PATH_SEP"lib/slf4j-jboss-logmanager-2.0.0.Final.jar"$PATH_SEP"$DSE_HOME/resources/cassandra/lib/*"$PATH_SEP"$CONNECTOR_LIB"$PATH_SEP"$DSE_HOME/resources/cassandra/conf"$PATH_SEP"$LIB_PATH"$PATH_SEP"$DSE_HOME/lib/*"$PATH_SEP"$DSE_HOME/resources/solr/lib/*" io.debezium.server.Main
else
  exec "$JAVA_BINARY" $DEBEZIUM_OPTS $JAVA_OPTS -cp "$RUNNER"$PATH_SEP"conf"$PATH_SEP$LIB_PATH io.debezium.server.Main
fi