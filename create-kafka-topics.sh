#!/bin/bash
# Script to create Kafka topics for system monitoring
# Author: pes2ug23cs707

KAFKA_HOME="/opt/kafka"
BOOTSTRAP_SERVER="172.24.174.69:9092"

# Define topics
TOPICS=(
  "topic-cpu"
  "topic-mem"
  "topic-net"
  "topic-disk"
)

echo " Creating Kafka topics on broker $BOOTSTRAP_SERVER ..."

# Create each topic
for topic in "${TOPICS[@]}"; do
  echo " Creating topic: $topic"
  sudo $KAFKA_HOME/bin/kafka-topics.sh --create \
    --topic "$topic" \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists
done

echo " All topics created successfully!"

# Verify created topics
echo " Listing all topics:"
sudo $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP_SERVER
