#!/bin/bash
#
# Starts the bundled ZooKeeper and a single Kafka 0.8.2.2 broker in one container.
#
set -e

cd /opt/kafka

BROKER_PORT=${BROKER_PORT:-9092}
ADVERTISED_HOST=${ADVERTISED_HOST:-127.0.0.1}
ADVERTISED_PORT=${ADVERTISED_PORT:-$BROKER_PORT}
BROKER_ID=${BROKER_ID:-0}
NUM_PARTITIONS=${NUM_PARTITIONS:-3}

bin/zookeeper-server-start.sh config/zookeeper.properties &

# 0.8.2.2 has no wait-for-quorum option, so give ZooKeeper a moment to bind 2181
for _ in $(seq 1 30); do
    if bash -c "</dev/tcp/127.0.0.1/2181" 2>/dev/null; then
        break
    fi
    sleep 1
done

sed -i "s/^broker.id=.*/broker.id=${BROKER_ID}/" config/server.properties
sed -i "s/^num.partitions=.*/num.partitions=${NUM_PARTITIONS}/" config/server.properties

{
    # The controller reaches every broker through its ADVERTISED address, so it has to resolve to this
    # container from the inside as well - otherwise UpdateMetadata never arrives and the metadata cache
    # of the broker stays empty forever.
    echo "port=${BROKER_PORT}"
    echo "advertised.host.name=${ADVERTISED_HOST}"
    echo "advertised.port=${ADVERTISED_PORT}"
    echo "auto.create.topics.enable=true"
    echo "delete.topic.enable=true"
    # A single-broker cluster can not replicate the __consumer_offsets topic
    echo "offsets.topic.replication.factor=1"
    echo "offsets.topic.num.partitions=5"
} >> config/server.properties

exec bin/kafka-server-start.sh config/server.properties
