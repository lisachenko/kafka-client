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
 * Authenticates against the SASL listeners of a Kafka 0.11.0.3 cluster with SASL/PLAIN.
 *
 * Kafka 0.10.0 (KIP-43) made SASL part of the protocol: a connection to a `SASL_PLAINTEXT`/`SASL_SSL` listener
 * starts with a `SaslHandshake` request (api key 17) that names the mechanism, and the tokens of that mechanism
 * follow as bare size-prefixed frames - for PLAIN a single `\0<username>\0<password>`, answered with an empty
 * token. Everything after it is an ordinary Kafka connection, so the producer, the consumer and the admin client
 * of this package work exactly as they do over plaintext; only these three options are added:
 *
 *   security.protocol = SASL_PLAINTEXT | SASL_SSL
 *   sasl.mechanism    = PLAIN                        (the only implemented mechanism)
 *   sasl.username / sasl.password
 *
 * The JAAS configuration of the Java client is not reproduced - a login module is a Java class - so the user name
 * and the password are plain options here.
 *
 * The broker of this repository advertises PLAINTEXT on 9092, SSL on 9093, SASL_PLAINTEXT on 9094 and SASL_SSL on
 * 9095, with the users of `docker/kafka-0.11.0.3/jaas.conf`:
 *
 *   docker compose up -d
 *   php examples/sasl.php                                             # SASL_PLAINTEXT on 9094
 *   php examples/sasl.php my-topic                                    # another topic
 *   KAFKA_SASL_SSL_BOOTSTRAP_SERVERS=127.0.0.1:9095 php examples/sasl.php   # the same exchange inside TLS
 *
 * PLAIN sends the password in clear text inside the token: on a real cluster use `SASL_SSL`, where the very same
 * exchange happens after the TLS handshake. `SASL_PLAINTEXT` is for a trusted network and for tests.
 *
 * GSSAPI (Kerberos) and the SCRAM mechanisms of 0.10.2 stay out of this client: PHP has no GSS-API binding in core,
 * and SCRAM needs the multi-round exchange of RFC 5802. Both are refused with an InvalidConfigurationException that
 * names the reason, instead of failing somewhere in the middle of a connection.
 *
 * Wrong credentials have no error code before Kafka 1.0 - the broker simply closes the connection during the token
 * exchange - which this client reports as a SaslAuthenticationException.
 */

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\SaslAuthenticationException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Security\SaslMechanism;
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

$saslSslBootstrapServer = getenv('KAFKA_SASL_SSL_BOOTSTRAP_SERVERS') ?: '';
$saslBootstrapServer    = $saslSslBootstrapServer !== ''
    ? $saslSslBootstrapServer
    : (getenv('KAFKA_SASL_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9094');
$securityProtocol       = $saslSslBootstrapServer !== ''
    ? SecurityProtocol::SASL_SSL
    : SecurityProtocol::SASL_PLAINTEXT;
$brokerAddress          = 'tcp://' . trim(explode(',', $saslBootstrapServer)[0]);
$topic                  = $argv[1] ?? 'kafka-client-example-sasl';
$certificate            = dirname(__DIR__) . '/docker/kafka-0.11.0.3/ssl/broker.crt';

if ($securityProtocol === SecurityProtocol::SASL_SSL && !extension_loaded('openssl')) {
    echo "The openssl extension is required for security.protocol = SASL_SSL\n";

    exit(1);
}

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => [$brokerAddress],
    ClientConfig::CLIENT_ID         => 'kafka-client-example-sasl',

    // The transport and the credentials: everything else in this file is the ordinary API of the client
    ClientConfig::SECURITY_PROTOCOL => $securityProtocol,
    ClientConfig::SASL_MECHANISM    => SaslMechanism::PLAIN,
    ClientConfig::SASL_USERNAME     => getenv('KAFKA_SASL_USERNAME') ?: 'kafkatest',
    ClientConfig::SASL_PASSWORD     => getenv('KAFKA_SASL_PASSWORD') ?: 'kafkatest-secret',

    ClientConfig::REQUEST_TIMEOUT_MS => 10000,
];

if ($securityProtocol === SecurityProtocol::SASL_SSL) {
    // SASL_SSL takes every ssl.* option of examples/ssl.php: the TLS handshake happens first, the SASL exchange
    // runs inside the encrypted channel afterwards.
    $configuration[ClientConfig::SSL_CA_CERT_LOCATION] = $certificate;

    if (!is_readable($certificate)) {
        echo "The certificate of the test broker is missing: {$certificate}\n";

        exit(1);
    }
}

// The handshake and the token exchange happen inside this call: wrong credentials are reported here, as a
// SaslAuthenticationException - the broker answers them by closing the connection, without an error code.
try {
    $cluster = Cluster::bootstrap($configuration);
} catch (SaslAuthenticationException $exception) {
    echo 'The broker refused the credentials: ' . $exception->getMessage() . "\n";

    exit(1);
}

$admin = new AdminClient($cluster, $configuration);

echo "Authenticated as {$configuration[ClientConfig::SASL_USERNAME]} over {$securityProtocol}\n";
echo "Brokers, as the SASL listener advertises them\n";
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

echo "\nProducing over an authenticated connection\n";
$producer = new KafkaProducer($configuration + [ProducerConfig::ACKS => ProducerConfig::ACKS_ALL]);
for ($index = 0; $index < 3; $index++) {
    $producer
        ->send($topic, Record::fromValue("Authenticated message #{$index} at " . date(DATE_ATOM)), 0)
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

echo "\nConsuming over an authenticated connection\n";
$consumer = new KafkaConsumer($configuration + [
    ConsumerConfig::GROUP_ID           => 'kafka-client-example-sasl-group',
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

printf('%sRead %d records over an authenticated connection to %s%s', PHP_EOL, $received, $saslBootstrapServer, PHP_EOL);
