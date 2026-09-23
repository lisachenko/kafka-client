#!/bin/bash
#
# Formats (once) and starts a single Apache Kafka 4.3.1 node in KRaft mode: broker and controller in one process,
# no ZooKeeper (Kafka 4.0 removed it). The configuration is written from scratch into config/server.properties (the
# 4.x distributions ship it there; the config/kraft/ directory of the 3.x distributions is gone).
#
# The node is formatted the way the release formats a combined node by default (the shipped server.properties and
# the quickstart of 4.3.1): a *dynamic* quorum - `controller.quorum.bootstrap.servers` instead of the static
# `controller.quorum.voters` of the 3.9.2 image, and `kafka-storage.sh format --standalone` - which finalizes
# `kraft.version` 1 (KIP-853), so that AddRaftVoter (80), RemoveRaftVoter (81) and UpdateRaftVoter (82) are answered
# instead of refused. Every other feature is left at the default level of the release: `metadata.version`
# 4.3-IV0, `transaction.version` 2 (KIP-890 part 2), `group.version` 1, `share.version` 1 (KIP-932 share groups),
# `streams.version` 1 (KIP-1071) and `eligible.leader.replicas.version` 1 (KIP-966).
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

CONFIG=config/server.properties

{
    # One combined node: the controller quorum is the node itself, on a listener of its own (never one of the four
    # client listeners), reachable inside the container only. A dynamic quorum (KIP-853): the voter set lives in
    # the metadata log, seeded by `format --standalone` below, and the node finds it through the bootstrap server.
    echo "process.roles=broker,controller"
    echo "node.id=${NODE_ID}"
    echo "controller.quorum.bootstrap.servers=localhost:${CONTROLLER_PORT}"
    echo "controller.listener.names=CONTROLLER"
    echo "listeners=PLAINTEXT://0.0.0.0:${BROKER_PORT},SSL://0.0.0.0:${BROKER_SSL_PORT},SASL_PLAINTEXT://0.0.0.0:${BROKER_SASL_PORT},SASL_SSL://0.0.0.0:${BROKER_SASL_SSL_PORT},CONTROLLER://0.0.0.0:${CONTROLLER_PORT}"
    # The controller listener is advertised as well: a dynamic voter registers the endpoint it advertises, and
    # DescribeQuorum v2 / DescribeCluster v1 report it. It stays reachable inside the container only.
    echo "advertised.listeners=PLAINTEXT://${ADVERTISED_HOST}:${ADVERTISED_PORT},SSL://${ADVERTISED_HOST}:${ADVERTISED_SSL_PORT},SASL_PLAINTEXT://${ADVERTISED_HOST}:${ADVERTISED_SASL_PORT},SASL_SSL://${ADVERTISED_HOST}:${ADVERTISED_SASL_SSL_PORT},CONTROLLER://localhost:${CONTROLLER_PORT}"
    echo "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL"
    echo "inter.broker.listener.name=PLAINTEXT"
    echo "num.network.threads=3"
    echo "num.io.threads=8"
    echo "socket.send.buffer.bytes=102400"
    echo "socket.receive.buffer.bytes=102400"
    echo "socket.request.max.bytes=104857600"
    echo "num.partitions=${NUM_PARTITIONS}"
    echo "num.recovery.threads.per.data.dir=2"
    echo "log.retention.hours=168"
    echo "log.segment.bytes=1073741824"
    echo "log.retention.check.interval.ms=300000"
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
    # A single-node cluster can not replicate the __consumer_offsets, __transaction_state and __share_group_state
    # topics (the last one is the state topic of the share coordinator of KIP-932)
    echo "offsets.topic.replication.factor=1"
    echo "offsets.topic.num.partitions=5"
    echo "transaction.state.log.replication.factor=1"
    echo "transaction.state.log.min.isr=1"
    echo "share.coordinator.state.topic.replication.factor=1"
    echo "share.coordinator.state.topic.min.isr=1"
    # KIP-48 / KIP-900 (delegation tokens on KRaft, Kafka 3.6): the token apis 38-41 are disabled (error code 61)
    # without a secret key. Test value only.
    echo "delegation.token.secret.key=kafkatest-delegation-token-master-key"
    # Group membership: let integration tests use short session timeouts
    echo "group.min.session.timeout.ms=1000"
    echo "group.max.session.timeout.ms=60000"
    # The group coordinator serves the classic, consumer (KIP-848) and streams (KIP-1071) rebalance protocols by
    # default in 4.3.1 (`group.coordinator.rebalance.protocols`); share groups (KIP-932) are enabled by the
    # finalized `share.version` feature. Nothing to set here.
    #
    # The ACL apis 29-31 need an authorizer to answer anything but 54. The StandardAuthorizer of KRaft keeps the
    # ACLs in the metadata log; every principal the suite uses is a super user (ANONYMOUS on the PLAINTEXT and SSL
    # listeners, the two SASL users), except `acltest`, whose SASL login is what an ACL test denies or allows.
    echo "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer"
    echo "super.users=User:ANONYMOUS;User:admin;User:kafkatest"
    echo "allow.everyone.if.no.acl.found=false"
} > "$CONFIG"

# The broker reads its JAAS configuration from the JVM system property
export KAFKA_OPTS="-Djava.security.auth.login.config=/opt/kafka/config/kafka_server_jaas.conf ${KAFKA_OPTS:-}"

# KRaft: the log directories carry a meta.properties with the cluster id, written once by the storage tool at the
# first start (a recreated container - `docker compose down -v` - starts over with a new cluster id). `--standalone`
# makes this node the one voter of a dynamic quorum and finalizes `kraft.version` 1; every other feature takes the
# default level of the release.
if [ ! -f /tmp/kafka-logs/meta.properties ]; then
    CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
    bin/kafka-storage.sh format --ignore-formatted --standalone -t "$CLUSTER_ID" -c "$CONFIG"
fi

exec bin/kafka-server-start.sh "$CONFIG"
