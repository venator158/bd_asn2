#!/bin/bash
# Script to start ZooKeeper and Kafka Broker

# Start ZooKeeper
echo " Starting ZooKeeper..."
sudo /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties

# Wait a few seconds for ZooKeeper to initialize
sleep 5

# Start Kafka Broker
echo " Starting Kafka Broker..."
sudo /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties

echo " ZooKeeper and Kafka Broker started successfully!"
