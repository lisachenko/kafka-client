#!/bin/bash
#
# Starts the bundled ZooKeeper and a single Kafka 0.9.0.1 broker in one container.
#
set -e

cd /opt/kafka

BROKER_PORT=${BROKER_PORT:-9092}
BROKER_SSL_PORT=${BROKER_SSL_PORT:-9093}
ADVERTISED_HOST=${ADVERTISED_HOST:-127.0.0.1}
ADVERTISED_PORT=${ADVERTISED_PORT:-$BROKER_PORT}
ADVERTISED_SSL_PORT=${ADVERTISED_SSL_PORT:-$BROKER_SSL_PORT}
BROKER_ID=${BROKER_ID:-0}
NUM_PARTITIONS=${NUM_PARTITIONS:-3}

bin/zookeeper-server-start.sh config/zookeeper.properties &

# Give ZooKeeper a moment to bind 2181 before the broker registers itself
for _ in $(seq 1 30); do
    if bash -c "</dev/tcp/127.0.0.1/2181" 2>/dev/null; then
        break
    fi
    sleep 1
done

sed -i "s/^broker.id=.*/broker.id=${BROKER_ID}/" config/server.properties
sed -i "s/^num.partitions=.*/num.partitions=${NUM_PARTITIONS}/" config/server.properties
sed -i "s/^#\?listeners=.*//; s/^#\?port=.*//" config/server.properties

{
    # 0.9 binds by `listeners`; the controller reaches every broker through its ADVERTISED address, so it has to
    # resolve to this container from the inside as well - otherwise UpdateMetadata never arrives and the metadata
    # cache of the broker stays empty forever.
    echo "listeners=PLAINTEXT://0.0.0.0:${BROKER_PORT},SSL://0.0.0.0:${BROKER_SSL_PORT}"
    echo "advertised.listeners=PLAINTEXT://${ADVERTISED_HOST}:${ADVERTISED_PORT},SSL://${ADVERTISED_HOST}:${ADVERTISED_SSL_PORT}"
    echo "ssl.keystore.location=/opt/kafka/ssl/broker.keystore.jks"
    echo "ssl.keystore.password=kafkatest"
    echo "ssl.key.password=kafkatest"
    echo "ssl.truststore.location=/opt/kafka/ssl/broker.truststore.jks"
    echo "ssl.truststore.password=kafkatest"
    echo "ssl.client.auth=none"
    echo "auto.create.topics.enable=true"
    echo "delete.topic.enable=true"
    # A single-broker cluster can not replicate the __consumer_offsets topic
    echo "offsets.topic.replication.factor=1"
    echo "offsets.topic.num.partitions=5"
    # Group membership: let integration tests use short session timeouts
    echo "group.min.session.timeout.ms=1000"
    echo "group.max.session.timeout.ms=60000"
} >> config/server.properties

exec bin/kafka-server-start.sh config/server.properties
