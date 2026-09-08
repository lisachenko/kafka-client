# Apache Kafka 0.9.0.1 (Scala 2.11) with the bundled ZooKeeper, for wire-protocol testing.
#
# The broker listens on PLAINTEXT (9092) and SSL (9093). The SSL listener uses the self-signed test
# certificate from ssl/ (CN=localhost, SAN=localhost,127.0.0.1, valid for 100 years); the same broker.crt
# is the CA file that the SSL integration tests hand to the client. Do not use these keys anywhere else.
FROM eclipse-temurin:8-jre

ARG KAFKA_VERSION=0.9.0.1
ARG SCALA_VERSION=2.11
ARG KAFKA_DIST=kafka_2.11-0.9.0.1

# Extra CA certificates of a sandbox proxy, see ca/README.md (the directory is normally empty)
COPY ca/ /usr/local/share/ca-certificates/extra/

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates openssl; \
    rm -rf /var/lib/apt/lists/*; \
    update-ca-certificates; \
    curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_DIST}.tgz" -o /tmp/kafka.tgz; \
    tar -xzf /tmp/kafka.tgz -C /opt; \
    rm /tmp/kafka.tgz; \
    ln -s "/opt/${KAFKA_DIST}" /opt/kafka

COPY ssl/broker.crt ssl/broker.key /opt/kafka/ssl/
RUN set -eux; \
    cd /opt/kafka/ssl; \
    openssl pkcs12 -export -in broker.crt -inkey broker.key -name broker -out broker.p12 -password pass:kafkatest; \
    keytool -importkeystore -noprompt \
        -srckeystore broker.p12 -srcstoretype PKCS12 -srcstorepass kafkatest \
        -destkeystore broker.keystore.jks -deststoretype JKS -deststorepass kafkatest -destkeypass kafkatest; \
    keytool -importcert -noprompt -alias broker -file broker.crt \
        -keystore broker.truststore.jks -storepass kafkatest; \
    rm broker.p12

COPY start.sh /opt/start.sh
RUN chmod +x /opt/start.sh

ENV ADVERTISED_HOST=127.0.0.1 \
    ADVERTISED_PORT=9092 \
    ADVERTISED_SSL_PORT=9093 \
    BROKER_ID=0 \
    NUM_PARTITIONS=3

EXPOSE 2181 9092 9093

CMD ["/opt/start.sh"]
