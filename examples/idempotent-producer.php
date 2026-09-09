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
 * Produces with the **exactly once** guarantee of Kafka 0.11 (KIP-98): `enable.idempotence`.
 *
 * With the option on, the producer asks a broker for a **producer id** before its first batch (InitProducerId, api
 * key 22) and numbers the batch of every topic-partition with a gapless sequence number. A batch that has to be
 * sent again - a lost acknowledgement, a leader that moved - goes out with the very same producer id, epoch and
 * sequence numbers, and the leader recognises it as the batch it already holds: it answers the offset of the
 * ORIGINAL append and writes nothing. The records of a partition therefore reach the log exactly once and in
 * order, however often the client had to retry, and nothing about the API changes.
 *
 * The guarantee implies `acks = all` and a non-zero `retries`, both of which this client sets for you when you
 * left them alone (`retries` becomes 3) and refuses when you set them to something that contradicts it. It holds
 * within one producer **session**: a new KafkaProducer gets a new producer id, and a record the application sends
 * a second time is a new batch, which the broker has no way of recognising.
 *
 *   docker compose up -d
 *   php examples/idempotent-producer.php
 *   php examples/idempotent-producer.php my-topic
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/idempotent-producer.php
 *
 * @see docs/protocol/0.11.0.md, sections "The idempotent producer" and "InitProducerId API (key 22, v0)"
 */

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;

require dirname(__DIR__) . '/vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$brokerAddress    = 'tcp://' . trim(explode(',', $bootstrapServers)[0]);
$topic            = $argv[1] ?? 'kafka-client-example-idempotent';
$clientId         = 'kafka-client-example-idempotent';

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => [$brokerAddress],
    ClientConfig::CLIENT_ID         => $clientId,
];

/**
 * Waits until the broker has created the topic and elected a leader for each of its partitions
 */
function awaitTopic(string $brokerAddress, string $topic, string $clientId): void
{
    $deadline = microtime(true) + 30.0;
    $attempt  = 0;

    do {
        $stream = new SocketStream($brokerAddress, [ClientConfig::REQUEST_TIMEOUT_MS => 5000], 5.0);
        new MetadataRequest([$topic], true, $clientId, ++$attempt)->writeTo($stream);
        $metadata = MetadataResponse::unpack($stream)->topics[$topic] ?? null;

        $hasLeaders = $metadata !== null
            && $metadata->partitions !== []
            && array_filter(
                $metadata->partitions,
                static fn(PartitionMetadata $partition): bool => $partition->leader < 0
            ) === [];
        if ($hasLeaders) {
            return;
        }
        usleep(250000);
    } while (microtime(true) < $deadline);

    throw new RuntimeException("The broker did not make the topic {$topic} available within 30 seconds");
}

awaitTopic($brokerAddress, $topic, $clientId);

// The one option that turns the guarantee on. `acks` and `retries` are derived from it: ProducerConfig refuses an
// explicit `acks` below `all` or a `retries` of 0 with an InvalidConfigurationException, because neither can keep
// the promise - a request the broker does not answer leaves the sequence numbers of this client where they were.
$producer = new KafkaProducer($configuration + [
    ProducerConfig::ENABLE_IDEMPOTENCE => true,
]);

$onSuccess = static function (RecordMetadata $metadata): void {
    echo "  stored at {$metadata->partition}@{$metadata->offset}\n";
};
$onFailure = static function (Throwable $error): void {
    echo '  failed: ' . $error->getMessage() . "\n";
};

echo "Producing three records into {$topic} with a producer id of its own:\n";
for ($index = 0; $index < 3; $index++) {
    $producer
        ->send($topic, Record::fromKeyValue("key-{$index}", "record #{$index} at " . date(DATE_ATOM)), 0)
        ->then($onSuccess, $onFailure);
}

// The producer id is asked for lazily, with the first flush: nothing is sent before this line
$producer->flush();

// Another batch of the same producer continues the sequence of the partition where the first one ended
echo "\nA second batch, continuing the sequence numbers of the first:\n";
$producer->send($topic, Record::fromValue('one more record'), 0)->then($onSuccess, $onFailure);
$producer->flush();

// What the broker really hands out, and what makes the deduplication possible in the first place. A null
// transactional id is answered by any broker with a fresh producer id and the epoch 0, and writes no transaction
// state at all - a transactional id, which the transactional producer uses, goes to the transaction coordinator.
$client   = new Client(Cluster::bootstrap($configuration), $configuration);
$assigned = $client->initProducerId();

echo "\nInitProducerId answered the producer id {$assigned->producerId} with the epoch {$assigned->epoch}\n";
echo "Every batch above carries the id of this producer, the epoch and the sequence number of its partition;\n";
echo "sending one of them again would be answered with the offset of the original append and written nowhere.\n";

echo "\nSee the producer id, the epoch and the sequences of the batches in the log:\n";
echo "  docker exec kafka-0-11-0-3 /opt/kafka/bin/kafka-run-class.sh kafka.tools.DumpLogSegments"
    . " --files /tmp/kafka-logs/{$topic}-0/00000000000000000000.log --print-data-log --deep-iteration\n";
