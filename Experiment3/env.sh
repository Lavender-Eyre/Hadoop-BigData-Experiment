#!/usr/bin/env bash
# Source this file from Ubuntu Bash: source ./env.sh
export LAB3_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop}"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME"
export HADOOP_YARN_HOME="$HADOOP_HOME"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export HBASE_HOME="$LAB3_ROOT/software/hbase"
export SPARK_HOME="$LAB3_ROOT/software/spark"
export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HBASE_HOME/bin:$SPARK_HOME/bin:$PATH"
