#!/bin/bash
#
# Formats (once) and starts a single Apache Kafka 3.9.2 node in KRaft mode: broker and controller in one process,
# no ZooKeeper. The configuration is written from scratch on top of the shipped config/kraft/server.properties.
#
set -e

cd /opt/kafka

BROKER_PORT=${BROKER_PORT:-9092}
BROKER_SSL_PORT=${BROKER_SSL_PORT:-9093}
BROKER_SASL_PORT=${BROKER_SASL_PORT:-9094}
BROKER_SASL_SSL_PORT=${BROKER_SASL_SSL_PORT:-9095}
CONTROLLER_PORT=${CONTROLLER_PORT:-9096}
ADVERTISED_HOST=${ADVERTISED_HOST:-127.0.0.1}
ADVERTISED_PORT=${ADVERTISED_PORT:-$BROKER_PORT}
ADVERTISED_SSL_PORT=${ADVERTISED_SSL_PORT:-$BROKER_SSL_PORT}
ADVERTISED_SASL_PORT=${ADVERTISED_SASL_PORT:-$BROKER_SASL_PORT}
ADVERTISED_SASL_SSL_PORT=${ADVERTISED_SASL_SSL_PORT:-$BROKER_SASL_SSL_PORT}
NODE_ID=${NODE_ID:-1}
NUM_PARTITIONS=${NUM_PARTITIONS:-3}

CONFIG=config/kraft/server.properties

# The shipped KRaft properties name a one-listener node; everything below is written anew
sed -i "s/^node.id=.*/node.id=${NODE_ID}/" "$CONFIG"
sed -i "s/^num.partitions=.*/num.partitions=${NUM_PARTITIONS}/" "$CONFIG"
sed -i "s/^controller.quorum.voters=.*//; s/^listeners=.*//; s/^advertised.listeners=.*//; s/^log.dirs=.*//" "$CONFIG"
sed -i "s/^inter.broker.listener.name=.*//; s/^controller.listener.names=.*//; s/^listener.security.protocol.map=.*//" "$CONFIG"
echo >> "$CONFIG"

{
    # One combined node: the controller quorum is the node itself, on a listener of its own (never one of the four
    # client listeners), reachable inside the container only
    echo "process.roles=broker,controller"
    echo "controller.quorum.voters=${NODE_ID}@localhost:${CONTROLLER_PORT}"
    echo "controller.listener.names=CONTROLLER"
    echo "listeners=PLAINTEXT://0.0.0.0:${BROKER_PORT},SSL://0.0.0.0:${BROKER_SSL_PORT},SASL_PLAINTEXT://0.0.0.0:${BROKER_SASL_PORT},SASL_SSL://0.0.0.0:${BROKER_SASL_SSL_PORT},CONTROLLER://0.0.0.0:${CONTROLLER_PORT}"
    echo "advertised.listeners=PLAINTEXT://${ADVERTISED_HOST}:${ADVERTISED_PORT},SSL://${ADVERTISED_HOST}:${ADVERTISED_SSL_PORT},SASL_PLAINTEXT://${ADVERTISED_HOST}:${ADVERTISED_SASL_PORT},SASL_SSL://${ADVERTISED_HOST}:${ADVERTISED_SASL_SSL_PORT}"
    echo "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL"
    echo "inter.broker.listener.name=PLAINTEXT"
    # Two log directories (KIP-113, and KIP-858 for KRaft): AlterReplicaLogDirs (34) can move a replica between them
    # and DescribeLogDirs (35) reports both. The metadata log lives in the first one. Nothing in the suite depends on
    # the directory a partition lands in.
    echo "log.dirs=/tmp/kafka-logs,/tmp/kafka-logs-2"
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
    # A single-node cluster can not replicate the __consumer_offsets and __transaction_state topics
    echo "offsets.topic.replication.factor=1"
    echo "offsets.topic.num.partitions=5"
    echo "transaction.state.log.replication.factor=1"
    echo "transaction.state.log.min.isr=1"
    # KIP-48 / KIP-900 (delegation tokens on KRaft, Kafka 3.6): the token apis 38-41 are disabled (error code 61)
    # without a secret key. Test value only.
    echo "delegation.token.secret.key=kafkatest-delegation-token-master-key"
    # Group membership: let integration tests use short session timeouts
    echo "group.min.session.timeout.ms=1000"
    echo "group.max.session.timeout.ms=60000"
    # KIP-848: the new group coordinator with the `consumer` rebalance protocol next to the classic one, so that
    # ConsumerGroupHeartbeat (68) and ConsumerGroupDescribe (69) are served (they answer 35 on the old coordinator)
    echo "group.coordinator.rebalance.protocols=classic,consumer"
    # The ACL apis 29-31 need an authorizer to answer anything but 54. The StandardAuthorizer of KRaft keeps the
    # ACLs in the metadata log; every principal the suite uses is a super user (ANONYMOUS on the PLAINTEXT and SSL
    # listeners, the two SASL users), except `acltest`, whose SASL login is what an ACL test denies or allows.
    echo "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer"
    echo "super.users=User:ANONYMOUS;User:admin;User:kafkatest"
    echo "allow.everyone.if.no.acl.found=false"
} >> "$CONFIG"

# The broker reads its JAAS configuration from the JVM system property
export KAFKA_OPTS="-Djava.security.auth.login.config=/opt/kafka/config/kafka_server_jaas.conf ${KAFKA_OPTS:-}"

# KRaft: the log directories carry a meta.properties with the cluster id, written once by the storage tool at the
# first start (a recreated container - `docker compose down -v` - starts over with a new cluster id)
if [ ! -f /tmp/kafka-logs/meta.properties ]; then
    CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
    bin/kafka-storage.sh format --ignore-formatted -t "$CLUSTER_ID" -c "$CONFIG"
fi

exec bin/kafka-server-start.sh "$CONFIG"
