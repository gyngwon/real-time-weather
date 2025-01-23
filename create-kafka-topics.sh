#!/bin/bash

# Create Kafka topics
docker exec test-kafka-1 kafka-topics --create --topic weatherDetails --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
docker exec test-kafka-1 kafka-topics --create --topic aggregatedWeather --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
docker exec test-kafka-1 kafka-topics --create --topic metricsWeather --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1

# Verify the created topics
docker exec test-kafka-1 kafka-topics --list --bootstrap-server kafka:9092
