#!/bin/bash

FLINK_VERSION=1.20
FLINK_VERSION_FULL=1.20.1
# ICEBERG_VERSION=1.9.2
# ICEBERG_FLINK_VERSION=${FLINK_VERSION}-${ICEBERG_VERSION}
# HADOOP_VERSION=3.3.1

if [ -z "$1" ]; then
  SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
  LIB_DIR=$SCRIPT_DIR/../lib
else
  LIB_DIR="$1"
fi

mkdir -p $LIB_DIR && cd $LIB_DIR

# Flink SQL JDBC Driver
curl -OL https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-jdbc-driver-bundle/${FLINK_VERSION_FULL}/flink-sql-jdbc-driver-bundle-${FLINK_VERSION_FULL}.jar
