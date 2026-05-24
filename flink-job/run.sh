#!/bin/bash
set -e

MAX_RETRIES=5
RETRY_DELAY=10

for ((i=1; i<=MAX_RETRIES; i++)); do
  echo "Attempt $i to connect to flink-jobmanager:8081"
  if nc -z flink-jobmanager 8081; then
    echo "JobManager is ready, submitting job"
    /opt/flink/bin/flink run -py /opt/flink/aggregate.py
    exit 0
  fi
  echo "JobManager not ready, retrying in $RETRY_DELAY seconds..."
  sleep $RETRY_DELAY
done

echo "Failed to connect to JobManager after $MAX_RETRIES attempts"
exit 1