#!/bin/sh
set -eu

until curl -fsS http://jobmanager:8081/overview >/dev/null 2>&1; do
  echo "Waiting for Flink JobManager..."
  sleep 5
done

/opt/flink/bin/flink run -d -py /opt/flink/usrlib/app/job.py
