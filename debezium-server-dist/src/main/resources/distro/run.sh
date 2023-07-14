#!/bin/bash
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
    export JAVA_OPTS="-Dcom.sun.management.jmxremote.ssl=false \
       -Dcom.sun.management.jmxremote.authenticate=true \
       -Dcom.sun.management.jmxremote.port=${JMX_PORT} \
       -Dcom.sun.management.jmxremote.rmi.port=${JMX_PORT} \
       -Dcom.sun.management.jmxremote.local.only=false \
       -Djava.rmi.server.hostname=${JMX_HOST} \
       -Dcom.sun.management.jmxremote.verbose=true"

    if [ -f "./jmxremote.access" -a -f "./jmxremote.password" ]; then
      chmod 600 ./jmxremote.password
      export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote.access.file=./jmxremote.access \
         -Dcom.sun.management.jmxremote.password.file=./jmxremote.password"
    fi
  fi
}

if [ -f "${EXTRA_CONNECTOR}/jdk_java_options.sh" ]; then
    source "${EXTRA_CONNECTOR}/jdk_java_options.sh"
fi

export EXTRA_CLASS_PATH=""
if [ -f "${EXTRA_CONNECTOR}/extra_class_path.sh" ]; then
    source "${EXTRA_CONNECTOR}/extra_class_path.sh"
fi

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

exec "$JAVA_BINARY" $DEBEZIUM_OPTS $JAVA_OPTS -cp \
    "$RUNNER"$PATH_SEP"conf"$PATH_SEP$LIB_PATH$PATH_SEP$EXTRA_CLASS_PATH io.debezium.server.Main
