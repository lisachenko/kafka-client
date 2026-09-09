Extra CA certificates for the image build
=========================================

Every `*.crt` file dropped into this directory is installed into the system trust store of the image
before the Kafka distribution is downloaded. Nothing is needed here on a normal network; a sandbox
whose egress proxy re-signs TLS (e.g. Claude Code remote sessions) copies its CA bundle in first:

    cp /root/.ccr/ca-bundle.crt docker/kafka-0.11.0.3/ca/proxy-ca.crt
    docker compose build

The certificates are ignored by git.
