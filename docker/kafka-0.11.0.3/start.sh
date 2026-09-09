#!/bin/bash
#
# Starts the bundled ZooKeeper and a single Kafka 0.11.0.3 broker in one container.
#
set -e

cd /opt/kafka

BROKER_PORT=${BROKER_PORT:-9092}
BROKER_SSL_PORT=${BROKER_SSL_PORT:-9093}
BROKER_SASL_PORT=${BROKER_SASL_PORT:-9094}
BROKER_SASL_SSL_PORT=${BROKER_SASL_SSL_PORT:-9095}
ADVERTISED_HOST=${ADVERTISED_HOST:-127.0.0.1}
ADVERTISED_PORT=${ADVERTISED_PORT:-$BROKER_PORT}
ADVERTISED_SSL_PORT=${ADVERTISED_SSL_PORT:-$BROKER_SSL_PORT}
ADVERTISED_SASL_PORT=${ADVERTISED_SASL_PORT:-$BROKER_SASL_PORT}
ADVERTISED_SASL_SSL_PORT=${ADVERTISED_SASL_SSL_PORT:-$BROKER_SASL_SSL_PORT}
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

# The server.properties shipped with 0.11 ends without a trailing newline (its last line is
# `group.initial.rebalance.delay.ms=0`), so anything appended to it would glue onto that line
echo >> config/server.properties

{
    # 0.9 binds by `listeners`; the controller reaches every broker through its ADVERTISED address, so it has to
    # resolve to this container from the inside as well - otherwise UpdateMetadata never arrives and the metadata
    # cache of the broker stays empty forever.
    echo "listeners=PLAINTEXT://0.0.0.0:${BROKER_PORT},SSL://0.0.0.0:${BROKER_SSL_PORT},SASL_PLAINTEXT://0.0.0.0:${BROKER_SASL_PORT},SASL_SSL://0.0.0.0:${BROKER_SASL_SSL_PORT}"
    echo "advertised.listeners=PLAINTEXT://${ADVERTISED_HOST}:${ADVERTISED_PORT},SSL://${ADVERTISED_HOST}:${ADVERTISED_SSL_PORT},SASL_PLAINTEXT://${ADVERTISED_HOST}:${ADVERTISED_SASL_PORT},SASL_SSL://${ADVERTISED_HOST}:${ADVERTISED_SASL_SSL_PORT}"
    echo "security.inter.broker.protocol=PLAINTEXT"
    # SASL/PLAIN on the SASL listeners; the users live in config/kafka_server_jaas.conf
    echo "sasl.enabled.mechanisms=PLAIN"
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
    # 0.11: the transaction coordinator's __transaction_state topic can not replicate either; without these two
    # settings a one-broker cluster answers every transactional request with 15 (COORDINATOR_NOT_AVAILABLE)
    echo "transaction.state.log.replication.factor=1"
    echo "transaction.state.log.min.isr=1"
    # Group membership: let integration tests use short session timeouts
    echo "group.min.session.timeout.ms=1000"
    echo "group.max.session.timeout.ms=60000"
} >> config/server.properties

# The 0.11 broker reads its JAAS configuration from the JVM system property (no listener-scoped
# sasl.jaas.config before 0.10.2 / KIP-85 on the broker side).
export KAFKA_OPTS="-Djava.security.auth.login.config=/opt/kafka/config/kafka_server_jaas.conf ${KAFKA_OPTS:-}"

exec bin/kafka-server-start.sh config/server.properties
