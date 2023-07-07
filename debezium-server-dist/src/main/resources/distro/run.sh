#!/bin/bash -vx
#
# To enable JMX functionality, export the JMX_HOST and JMX_PORT environment variables.
# Modify the jmxremote.access and jmxremote.password files accordingly.
#
# In order to run Debezium for the Cassandra database, export the CASSANDRA_VERSION environment variable
# with one of the following values: v3, v4, dse.
#
# Copyright Debezium Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set_jmx() {
  if [ -n "${JMX_HOST}" -a -n "${JMX_PORT}" ]; then
    chmod 600 ./jmxremote.password
    export JAVA_OPTS="-Dcom.sun.management.jmxremote.ssl=false \
       -Dcom.sun.management.jmxremote.authenticate=true \
       -Dcom.sun.management.jmxremote.port=${JMX_PORT} \
       -Dcom.sun.management.jmxremote.rmi.port=${JMX_PORT} \
       -Dcom.sun.management.jmxremote.local.only=false \
       -Djava.rmi.server.hostname=${JMX_HOST} \
       -Dcom.sun.management.jmxremote.access.file=./jmxremote.access \
       -Dcom.sun.management.jmxremote.password.file=./jmxremote.password \
       -Dcom.sun.management.jmxremote.verbose=true"
  fi
}

set_java_options() {
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
}

handleCassandra() {
  CASSANDRA_PATH="./connectors/cassandra"
  version="${CASSANDRA_VERSION^^}"
  if [[ "${version}" == "V3" ]]; then
    CONNECTOR_LIB="${CASSANDRA_PATH}/v3/*"
  elif [[ "${version}" == "V4" ]]; then
    CONNECTOR_LIB="${CASSANDRA_PATH}/ver4/debezium-connector-cassandra-4/*"
  elif [[ "${version}" == "DSE" ]]; then
    if [[ ! -n "${DSE_HOME}" ]]; then
      echo "Error: DSE_HOME variable is not defined."
      exit -1
    fi
    CONNECTOR_LIB="${CASSANDRA_PATH}/dse/debezium-connector-dse/*"
  else
    echo "Invalid Cassandra version '$CASSANDRA_VERSION' defined"
    exit -1
  fi
  set_java_options
  echo "Executing Debezium for Cassandra ${CASSANDRA_VERSION}"
  exec "$JAVA_BINARY" $DEBEZIUM_OPTS $JAVA_OPTS -cp \
    "$RUNNER"$PATH_SEP"conf"$PATH_SEP"lib/slf4j-jboss-logmanager-2.0.0.Final.jar"$PATH_SEP"$DSE_HOME/resources/cassandra/lib/*"$PATH_SEP"$CONNECTOR_LIB"$PATH_SEP"$DSE_HOME/resources/cassandra/conf"$PATH_SEP"$LIB_PATH"$PATH_SEP"$DSE_HOME/lib/*"$PATH_SEP"$DSE_HOME/resources/solr/lib/*" \
    io.debezium.server.Main
}

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

set_jmx

if [ -n "${CASSANDRA_VERSION}" ]; then
  handleCassandra
else
  exec "$JAVA_BINARY" $DEBEZIUM_OPTS $JAVA_OPTS -cp "$RUNNER"$PATH_SEP"conf"$PATH_SEP$LIB_PATH io.debezium.server.Main
fi
