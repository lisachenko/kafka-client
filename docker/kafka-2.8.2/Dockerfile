# Apache Kafka 2.8.2 (Scala 2.13) with the bundled ZooKeeper, for wire-protocol testing.
#
# The broker listens on PLAINTEXT (9092), SSL (9093), SASL_PLAINTEXT (9094) and SASL_SSL (9095).
# SASL offers the PLAIN mechanism only (users in jaas.conf: kafkatest/kafkatest-secret, admin/admin-secret). The SSL listener uses the self-signed test
# certificate from ssl/ (CN=localhost, SAN=localhost,127.0.0.1, valid for 100 years); the same broker.crt
# is the CA file that the SSL integration tests hand to the client. Do not use these keys anywhere else.
FROM eclipse-temurin:11-jre

ARG KAFKA_VERSION=2.8.2
ARG SCALA_VERSION=2.13
ARG KAFKA_DIST=kafka_2.13-2.8.2

# Extra CA certificates of a sandbox proxy, see ca/README.md (the directory is normally empty)
COPY ca/ /usr/local/share/ca-certificates/extra/

# The temurin image ships curl, openssl and ca-certificates already (the 1.1.1 recipe installed them with apt-get,
# which a sandbox proxy that throttles the package lists can stall for good), so only the trust store is refreshed
RUN set -eux; \
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

COPY jaas.conf /opt/kafka/config/kafka_server_jaas.conf
COPY start.sh /opt/start.sh
RUN chmod +x /opt/start.sh

ENV ADVERTISED_HOST=127.0.0.1 \
    ADVERTISED_PORT=9092 \
    ADVERTISED_SSL_PORT=9093 \
    ADVERTISED_SASL_PORT=9094 \
    ADVERTISED_SASL_SSL_PORT=9095 \
    BROKER_ID=0 \
    NUM_PARTITIONS=3

EXPOSE 2181 9092 9093 9094 9095

CMD ["/opt/start.sh"]
