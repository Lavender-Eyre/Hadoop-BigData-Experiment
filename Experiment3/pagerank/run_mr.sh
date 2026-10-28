#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 3 ]]; then
  echo "Usage: bash run_mr.sh HDFS_INITIAL_FILE HDFS_OUTPUT_ROOT NODE_COUNT" >&2
  exit 2
fi
INPUT=$1
OUTPUT=${2%/}
N=$3
[[ "$N" =~ ^[1-9][0-9]*$ ]] || { echo "NODE_COUNT must be positive" >&2; exit 2; }
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if [[ -z "${HADOOP_HOME:-}" ]]; then
  HADOOP_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v hadoop)")")")
fi
shopt -s nullglob
JARS=("$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar)
[[ ${#JARS[@]} -eq 1 ]] || { echo "Expected one Hadoop Streaming jar in $HADOOP_HOME/share/hadoop/tools/lib" >&2; exit 1; }
hdfs dfs -test -e "$INPUT" || { echo "Input not found: $INPUT" >&2; exit 1; }
if hdfs dfs -test -e "$OUTPUT"; then
  echo "Output already exists: $OUTPUT. Choose a NEW run directory." >&2
  exit 1
fi
hdfs dfs -mkdir -p "$OUTPUT"
CURRENT=$INPUT
for iteration in $(seq 1 10); do
  suffix=$(printf '%02d' "$iteration")
  STATS="$OUTPUT/dangling$suffix"
  NEXT="$OUTPUT/iter$suffix"
  echo "===== PageRank iteration $iteration/10: sum dangling mass ====="
  hadoop jar "${JARS[0]}" \
    -D "mapreduce.job.name=Lab3-PR-$suffix-dangling" \
    -D mapreduce.job.reduces=1 \
    -files "$SCRIPT_DIR/mr.py" \
    -input "$CURRENT" -output "$STATS" \
    -mapper 'python3 mr.py stats-map' \
    -combiner 'python3 mr.py stats-reduce' \
    -reducer 'python3 mr.py stats-reduce'
  DANGLING=$(hdfs dfs -cat "$STATS/part-*" | awk -F '\t' '$1=="D" {print $2}')
  [[ -n "$DANGLING" ]] || { echo "Missing dangling mass" >&2; exit 1; }
  REDUCER="python3 mr.py reduce --nodes $N --dangling $DANGLING"
  FINAL_OPTIONS=()
  if [[ "$iteration" -eq 10 ]]; then
    REDUCER="$REDUCER --final"
    # Use Text + NullWritable for final tuples so TextOutputFormat adds no TAB.
    FINAL_OPTIONS=(-D stream.reduce.output=keyonlytext)
  fi
  echo "===== PageRank iteration $iteration/10: update ranks (dangling=$DANGLING) ====="
  hadoop jar "${JARS[0]}" \
    -D "mapreduce.job.name=Lab3-PR-$suffix-update" \
    -D "mapreduce.job.reduces=${MR_REDUCERS:-2}" \
    "${FINAL_OPTIONS[@]}" \
    -files "$SCRIPT_DIR/mr.py" \
    -input "$CURRENT" -output "$NEXT" \
    -mapper 'python3 mr.py map' -reducer "$REDUCER"
  CURRENT=$NEXT
done
echo "Completed exactly 10 PageRank iterations (20 MapReduce jobs)."
echo "Final HDFS output: $CURRENT"
hdfs dfs -cat "$CURRENT/part-*"
