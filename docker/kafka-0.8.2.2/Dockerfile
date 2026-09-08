# Apache Kafka 0.8.2.2 (Scala 2.11) with the bundled ZooKeeper, for wire-protocol testing.
#
# 0.8.2.2 only runs on Java 7/8, hence the ancient JRE base image.
FROM eclipse-temurin:8-jre

ARG KAFKA_VERSION=0.8.2.2
ARG SCALA_VERSION=2.11
ARG KAFKA_DIST=kafka_2.11-0.8.2.2

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_DIST}.tgz" -o /tmp/kafka.tgz; \
    tar -xzf /tmp/kafka.tgz -C /opt; \
    rm /tmp/kafka.tgz; \
    ln -s "/opt/${KAFKA_DIST}" /opt/kafka

COPY start.sh /opt/start.sh
RUN chmod +x /opt/start.sh

ENV ADVERTISED_HOST=127.0.0.1 \
    ADVERTISED_PORT=9092 \
    BROKER_ID=0 \
    NUM_PARTITIONS=3

EXPOSE 2181 9092

CMD ["/opt/start.sh"]
