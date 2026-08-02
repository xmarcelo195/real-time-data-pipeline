#!/bin/bash
set -e

until kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
  echo "Waiting for Kafka..."
  sleep 5
done

kafka-topics --create --if-not-exists --topic btc.transactions --bootstrap-server kafka:9092 --partitions 6 --replication-factor 1
kafka-topics --create --if-not-exists --topic btc.alerts --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
kafka-topics --create --if-not-exists --topic btc.metrics --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
