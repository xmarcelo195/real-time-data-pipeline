#!/bin/bash

# Aguardar o Kafka estar pronto
until nc -z kafka 9092; do
  echo "Waiting for Kafka..."
  sleep 5
done

# Criar o tópico test-topic
kafka-topics --create --bootstrap-server kafka:9092 --topic test-topic --partitions 16 --replication-factor 1

# Criar o tópico aggregated-topic
kafka-topics --create --bootstrap-server kafka:9092 --topic aggregated-topic --partitions 8 --replication-factor 1
