#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p build/hbase logs
javac -encoding UTF-8 -cp "$(hbase classpath)" -d build/hbase hbase/HBaseLab.java
jar cf build/hbase-lab.jar -C build/hbase .
java -cp "build/hbase-lab.jar:$(hbase classpath)" HBaseLab "$@" 2>&1 | tee logs/hbase-java.log
