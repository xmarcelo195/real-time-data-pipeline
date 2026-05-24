#!/bin/bash

<<<<<<< HEAD
# Aguardar o Kafka estar pronto
until nc -z kafka 9092; do
  echo "Waiting for Kafka..."
  sleep 5
done

# Criar o tópico test-topic
kafka-topics --create --bootstrap-server kafka:9092 --topic test-topic --partitions 16 --replication-factor 1

# Criar o tópico aggregated-topic
kafka-topics --create --bootstrap-server kafka:9092 --topic aggregated-topic --partitions 8 --replication-factor 1
=======
# Wait for Kafka to be ready
until nc -z kafka 9092; do
  echo "Waiting for Kafka..."
  sleep 20
done

# Create Topic
kafka-topics --create -if-not-exists --topic btc --bootstrap-server kafka:9092 --partitions 10 --replication-factor 1
>>>>>>> 24ad629615bea8d6d4b314b0e4f29b205c974579
