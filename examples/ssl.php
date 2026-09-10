<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

/**
 * Talks to the SSL listener of a Kafka 1.1.1 cluster.
 *
 * Kafka 0.9 is the release that added transport security: a broker binds one listener per security protocol
 * (`listeners=PLAINTEXT://...,SSL://...`) and every listener serves the identical request set, so encryption changes
 * the transport and never a byte of a request. Point `bootstrap.servers` at the SSL listener, set
 * `security.protocol`, and the producer, the consumer and the admin client of this package work exactly as they do
 * over plaintext.
 *
 * The broker of this repository advertises PLAINTEXT on 9092, SSL on 9093, SASL_PLAINTEXT on 9094 and SASL_SSL on
 * 9095, with the self-signed test certificate of `docker/kafka-1.1.1/ssl/broker.crt`, which is also the CA file
 * the client verifies it against:
 *
 *   docker compose up -d
 *   php examples/ssl.php
 *   php examples/ssl.php my-topic                                   # another topic
 *   KAFKA_SSL_BOOTSTRAP_SERVERS=127.0.0.1:9093 php examples/ssl.php
 *
 * Note what the Metadata answer below reports: every version of the api carries exactly one host/port per broker,
 * and the broker fills it with the endpoint of the listener the request arrived on. A client that bootstraps over
 * TLS therefore discovers the TLS endpoints of the whole cluster and keeps talking TLS to every broker it learns
 * about; one that bootstraps in plaintext learns the plaintext ones. They never mix.
 *
 * Authentication is a separate option and lives in {@see examples/sasl.php}: Kafka 0.10.0 added the SaslHandshake
 * request and the PLAIN mechanism, so `security.protocol = SASL_SSL` is this example plus three options.
 */

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Common\Serialization\StringDeserializer;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;

require dirname(__DIR__) . '/vendor/autoload.php';

$sslBootstrapServer = getenv('KAFKA_SSL_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9093';
$brokerAddress      = 'tcp://' . trim(explode(',', $sslBootstrapServer)[0]);
$topic              = $argv[1] ?? 'kafka-client-example-ssl';
$certificate        = dirname(__DIR__) . '/docker/kafka-1.1.1/ssl/broker.crt';

if (!extension_loaded('openssl')) {
    echo "The openssl extension is required for security.protocol = SSL\n";

    exit(1);
}

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => [$brokerAddress],
    ClientConfig::CLIENT_ID         => 'kafka-client-example-ssl',

    // The transport: everything else in this file is the ordinary API of the client
    ClientConfig::SECURITY_PROTOCOL => SecurityProtocol::SSL,
    // The certificates the broker certificate is verified against - the `ssl.truststore.location` of the Java
    // client. Without it the certificate stores of the system are used, which do not know this self-signed one.
    ClientConfig::SSL_CA_CERT_LOCATION => $certificate,
    // For a broker running `ssl.client.auth=required`, add the client certificate and its key:
    // ClientConfig::SSL_CLIENT_CERT_LOCATION => '/etc/kafka/client.pem',
    // ClientConfig::SSL_KEY_LOCATION         => '/etc/kafka/client.key',
    // ClientConfig::SSL_KEY_PASSWORD         => 'secret',
    // `ssl.protocol` offers a single TLS version instead of "any" (SslProtocol::TLSv1_2 and friends); note that
    // OpenSSL 3 refuses TLSv1_1 outright, even though the broker still offers it.
    ClientConfig::REQUEST_TIMEOUT_MS => 10000,
];

if (!is_readable($certificate)) {
    echo "The certificate of the test broker is missing: {$certificate}\n";
    echo "Point ClientConfig::SSL_CA_CERT_LOCATION at the CA of your own broker instead.\n";

    exit(1);
}

// The handshake happens inside this call: a broker speaks TLS from the first byte of a connection to its SSL
// listener, there is no in-protocol upgrade, and a certificate that does not verify fails here with a
// NetworkException carrying the OpenSSL message.
$cluster = Cluster::bootstrap($configuration);
$admin   = new AdminClient($cluster, $configuration);

echo "Brokers, as the SSL listener advertises them\n";
foreach ($admin->findAllBrokers() as $broker) {
    echo "  {$broker->nodeId}: {$broker->host}:{$broker->port}\n";
}

// The admin client asks with `allow_auto_topic_creation = false` since Metadata v4 (Kafka 0.11, KIP-4), so
// describing a topic no longer creates it: CreateTopics is the explicit way, and 36 (TopicAlreadyExists) simply
// means the topic is already around. The controller elects the leaders of its partitions a moment later.
$created = $admin->createTopics([new NewTopic($topic, 1, 1)])[$topic] ?? null;
if ($created !== null && $created->getCode() !== KafkaException::TOPIC_ALREADY_EXISTS) {
    echo 'The topic could not be created: ' . $created->getMessage() . "\n";

    exit(1);
}

$deadline = microtime(true) + 30.0;
do {
    $cluster->reload();
    $metadata   = $admin->describeTopics([$topic])[$topic] ?? null;
    $partitions = $metadata?->partitions ?? [];
    $leaderless = array_filter($partitions, static fn($partition): bool => $partition->leader < 0);
    if ($partitions !== [] && $leaderless === []) {
        break;
    }
    usleep(250000);
} while (microtime(true) < $deadline);

if ($partitions === [] || $leaderless !== []) {
    echo "No leader was elected for the partitions of {$topic}\n";

    exit(1);
}

echo "\nProducing over TLS\n";
$producer = new KafkaProducer($configuration + [ProducerConfig::ACKS => ProducerConfig::ACKS_ALL]);
for ($index = 0; $index < 3; $index++) {
    $producer
        ->send($topic, Record::fromValue("Encrypted message #{$index} at " . date(DATE_ATOM)), 0)
        ->then(
            static fn(RecordMetadata $metadata) => print("  stored {$metadata}\n"),
            static fn(Throwable $error) => print('  failed: ' . $error->getMessage() . "\n")
        );
}

try {
    $producer->flush();
} catch (KafkaException $exception) {
    echo 'The cluster refused the batch: ' . $exception->getMessage() . "\n";

    exit(1);
}

echo "\nConsuming over TLS\n";
$consumer = new KafkaConsumer($configuration + [
    ConsumerConfig::GROUP_ID           => 'kafka-client-example-ssl-group',
    ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
    ConsumerConfig::ENABLE_AUTO_COMMIT => false,
    ConsumerConfig::VALUE_DESERIALIZER => StringDeserializer::class,
]);

$consumer->assign([$topic => [0]]);
$consumer->seekToBeginning([$topic => [0]]);

$received   = 0;
$emptyPolls = 0;
while ($emptyPolls < 3) {
    $isEmpty = true;
    foreach ($consumer->poll(1000) as $polledTopic => $polledPartitions) {
        foreach ($polledPartitions as $partitionId => $records) {
            foreach ($records as $record) {
                $isEmpty = false;
                $received++;

                $value = $record instanceof ConsumerRecord ? $record->deserializedValue : $record->value;
                printf(
                    '  %s:%d@%d %s%s',
                    $polledTopic,
                    $partitionId,
                    (int) $record->offset,
                    is_string($value) ? $value : var_export($value, true),
                    PHP_EOL
                );
            }
        }
    }
    $emptyPolls = $isEmpty ? $emptyPolls + 1 : 0;
}

printf('%sRead %d records over an encrypted connection to %s%s', PHP_EOL, $received, $sslBootstrapServer, PHP_EOL);
