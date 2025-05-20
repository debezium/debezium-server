#!/bin/bash

export JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 -javaagent:jolokia.jar -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=9012 -Dcom.sun.management.jmxremote.rmi.port=9012 -Djava.rmi.server.hostname=$POD_IP"

if [[ -v PULSAR_SERVICE_ACCOUNT_JSON ]]; then
  echo "$PULSAR_SERVICE_ACCOUNT_JSON" > /tmp/pulsar_creds.json
else
  echo "Pulsar service account environment variable (PULSAR_SERVICE_ACCOUNT_JSON) was not found."
fi

/debezium/run.sh