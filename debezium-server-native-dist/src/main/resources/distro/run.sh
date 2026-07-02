#!/bin/bash
#
#
# Copyright Debezium Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

RUNNER=$(ls "$SCRIPT_DIR"/debezium-server-*runner 2>/dev/null | head -1)

if [ -z "$RUNNER" ]; then
    echo "ERROR: Could not find the native executable (debezium-server-*runner) in $SCRIPT_DIR"
    exit 1
fi

NATIVE_JAVA_PROPS=(
    "-Ddebezium.deployment.server=true"
)

exec "$RUNNER" "${NATIVE_JAVA_PROPS[@]}" "$@"