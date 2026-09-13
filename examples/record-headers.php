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
 * Writes records with **headers** and reads them back, which Kafka 0.11 made possible (KIP-82).
 *
 * A header is a key-value pair of metadata next to the key and the value of a record - the place for a content
 * type, a trace id, the name of the schema a value was serialized with, anything a consumer wants to look at
 * without deserializing the payload. Only the **record batch v2** of Kafka 0.11 has room for them: a message set
 * of the formats v0 and v1 has no such field, so {@see ProducerConfig::MESSAGE_FORMAT_VERSION} has to stay at its
 * default `0.11.0` (which sends a Produce v3), and a Fetch below version 4 of a partition whose records carry
 * headers is answered with the error code -1, because the broker cannot convert them down.
 *
 * Header keys are plain strings and are **not** unique: the same key may appear several times, in the order the
 * producer wrote it. A header value is a nullable byte array, so `null` and `''` are two different values.
 *
 *   docker compose up -d
 *   php examples/record-headers.php
 *   php examples/record-headers.php my-topic
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/record-headers.php
 *
 * @see docs/protocol/2.8.md, section "RecordBatch (message format v2)"
 */

use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;

require dirname(__DIR__) . '/vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$brokerAddress    = 'tcp://' . trim(explode(',', $bootstrapServers)[0]);
$topic            = $argv[1] ?? 'kafka-client-example-headers';
$clientId         = 'kafka-client-example-headers';

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

$producer = new KafkaProducer($configuration + [
    ProducerConfig::ACKS => ProducerConfig::ACKS_ALL,
    // The default, and the reason the headers below reach the log at all: '0.10.x' or '0.9.0' would write a
    // message set, which has no place for them, and KafkaProducer::send() would silently drop them
    ProducerConfig::MESSAGE_FORMAT_VERSION => ProducerConfig::MESSAGE_FORMAT_VERSION_0_11_0,
]);

$onSuccess = static function (RecordMetadata $metadata): void {
    echo "  stored at {$metadata->partition}@{$metadata->offset}\n";
};
$onFailure = static function (Throwable $error): void {
    echo '  failed: ' . $error->getMessage() . "\n";
};

echo "Producing records with headers into {$topic}:\n";

// withHeaders() returns a copy of the record with the given headers, so a record can be built once and stamped
// differently for every send. The order of the headers is preserved on the wire.
$producer
    ->send($topic, Record::fromKeyValue('order-1', '{"total":42}')->withHeaders(
        new Header('content-type', 'application/json'),
        new Header('trace-id', bin2hex(random_bytes(8))),
    ), 0)
    ->then($onSuccess, $onFailure);

// The same key twice, an empty value and a null value: all three are legal, and all three survive the round trip
$producer
    ->send($topic, Record::fromKeyValue('order-2', '{"total":7}')->withHeaders(
        new Header('tag', 'first'),
        new Header('tag', 'second'),
        new Header('empty', ''),
        new Header('absent', null),
    ), 0)
    ->then($onSuccess, $onFailure);

// A record without any header is written into the very same batch; its header count is simply 0
$producer->send($topic, Record::fromValue('a record without headers'), 0)->then($onSuccess, $onFailure);

$producer->flush();

$consumer = new KafkaConsumer($configuration + [
    ConsumerConfig::GROUP_ID           => 'kafka-client-example-headers-group',
    ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
    ConsumerConfig::ENABLE_AUTO_COMMIT => false,
]);

$consumer->assign([$topic => [0]]);
$consumer->seekToBeginning([$topic => [0]]);

echo "\nReading them back (a Fetch v5, which is the only version that can carry them):\n";

$emptyPolls = 0;
while ($emptyPolls < 3) {
    $isEmpty = true;

    foreach ($consumer->poll(1000) as $polledTopic => $partitions) {
        foreach ($partitions as $partitionId => $records) {
            foreach ($records as $record) {
                $isEmpty = false;

                printf('  %s:%d@%d key=%s%s', $polledTopic, $partitionId, (int) $record->offset, $record->key ?? '<null>', PHP_EOL);
                if ($record->headers === []) {
                    echo "      (no headers)\n";
                }
                foreach ($record->headers as $header) {
                    printf('      %s = %s%s', $header->key, $header->value === null ? '<null>' : "'{$header->value}'", PHP_EOL);
                }
            }
        }
    }

    $emptyPolls = $isEmpty ? $emptyPolls + 1 : 0;
}

$consumer->unsubscribe();

echo "\nThe same batch printed by the broker itself:\n";
echo "  docker exec kafka-2-8-2 /opt/kafka/bin/kafka-run-class.sh kafka.tools.DumpLogSegments"
    . " --files /tmp/kafka-logs/{$topic}-0/00000000000000000000.log --print-data-log --deep-iteration\n";
