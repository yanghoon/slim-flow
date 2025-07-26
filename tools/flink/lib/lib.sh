#!/bin/bash

FLINK_VERSION=1.20
FLINK_VERSION_FULL=1.20.1
ICEBERG_VERSION=1.9.2
ICEBERG_FLINK_VERSION=${FLINK_VERSION}-${ICEBERG_VERSION}
HADOOP_VERSION=3.3.1

# mkdir -p lib && cd lib

# Iceberg Flink Dependencies
curl -OL https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${ICEBERG_FLINK_VERSION}.jar
curl -OL https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Hadoop Dependencies
curl -OL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/${HADOOP_VERSION}/hadoop-hdfs-client-${HADOOP_VERSION}.jar
curl -OL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/${HADOOP_VERSION}/hadoop-mapreduce-client-core-${HADOOP_VERSION}.jar

# Flink Plugins
curl -OL https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION_FULL}/flink-s3-fs-hadoop-${FLINK_VERSION_FULL}.jar

# Flink SQL JDBC Driver
curl -OL https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-jdbc-driver-bundle/${FLINK_VERSION_FULL}/flink-sql-jdbc-driver-bundle-${FLINK_VERSION_FULL}.jar
