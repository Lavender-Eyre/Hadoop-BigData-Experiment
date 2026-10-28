#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
mkdir -p logs results
start_if_missing() {
  local class="$1"
  shift
  if jps | awk '{print $2}' | grep -qx "$class"; then
    echo "$class already running"
  else
    "$@"
  fi
}
# Individual daemons on one WSL instance: pseudo-distributed mode, without SSH.
start_if_missing NameNode hdfs --daemon start namenode
start_if_missing DataNode hdfs --daemon start datanode
start_if_missing SecondaryNameNode hdfs --daemon start secondarynamenode
start_if_missing ResourceManager yarn --daemon start resourcemanager
start_if_missing NodeManager yarn --daemon start nodemanager
timeout 90 hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p "/user/$(whoami)/lab3"
jps | tee logs/jps.txt
hdfs dfsadmin -report | tee logs/hdfs-report.txt
yarn node -list | tee logs/yarn-nodes.txt
echo 'Check Live datanodes = 1 and RUNNING NodeManagers = 1 before proceeding.'
