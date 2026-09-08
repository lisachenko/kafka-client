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
 * Consumes a topic of a Kafka 0.9.0.1 cluster with the partitions picked by hand, see {@see KafkaConsumer}.
 *
 * Start the broker of this repository and run the example against it:
 *
 *   docker compose up -d
 *   php examples/consumer.php
 *   php examples/consumer.php my-topic my-group          # another topic and consumer group
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/consumer.php
 *
 * The example seeds the topic with a handful of records first, so that there is always something to read; pass
 * `--no-produce` to consume what is already in the log. It then assigns every partition of the topic explicitly -
 * an `assign()`ed consumer joins no group, so it neither rebalances nor heartbeats, and two consumers of one group
 * that assign the same partition both read it - rewinds to the beginning of the log, prints what it receives and
 * commits the positions it reached. {@see examples/consumer-group.php} is the same example with the broker-side
 * group membership of Kafka 0.9, where the partitions are handed out by the group coordinator.
 */

use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Serialization\StringDeserializer;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;

require dirname(__DIR__) . '/vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$brokerAddress    = 'tcp://' . trim(explode(',', $bootstrapServers)[0]);
$arguments        = array_values(array_filter(array_slice($argv, 1), static fn(string $argument): bool => !str_starts_with($argument, '--')));
$topic            = $arguments[0] ?? 'kafka-client-example';
$groupId          = $arguments[1] ?? 'kafka-client-example-group';
$shouldProduce    = !in_array('--no-produce', $argv, true);

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => [$brokerAddress],
    ClientConfig::CLIENT_ID         => 'kafka-client-example',
    // Where the committed offsets live: `kafka` uses the coordinator of the group (OffsetCommit v2, OffsetFetch v1),
    // `zookeeper` keeps them where the consumers of Kafka 0.8.1 did (v0). The two storages are independent.
    ClientConfig::OFFSETS_STORAGE   => ClientConfig::OFFSETS_STORAGE_KAFKA,

    ConsumerConfig::GROUP_ID                => $groupId,
    ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
    ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
    ConsumerConfig::FETCH_MAX_WAIT_MS       => 500,
    ConsumerConfig::FETCH_MIN_BYTES         => 1,
    ConsumerConfig::MAX_PARTITION_FETCH_BYTES => 65536,
    // A deserializer turns the raw bytes of a record into an application-level value; the records of a poll() are
    // then instances of ConsumerRecord, which keeps both next to each other.
    ConsumerConfig::VALUE_DESERIALIZER      => StringDeserializer::class,
];

/**
 * Waits until the broker has created the topic and elected a leader for each of its partitions.
 *
 * Asking for the metadata of an unknown topic creates it when `auto.create.topics.enable` is on, but that very
 * first answer announces the topic without any partition: the controller elects the leaders afterwards.
 */
function awaitTopic(string $brokerAddress, string $topic, array $configuration): void
{
    $deadline = microtime(true) + 30.0;
    $attempt  = 0;

    do {
        $stream = new SocketStream($brokerAddress, $configuration, 5.0);
        new MetadataRequest([$topic], 'kafka-client-example', ++$attempt)->writeTo($stream);
        $metadata = MetadataResponse::unpack($stream)->topics[$topic] ?? null;

        $hasLeaders = $metadata !== null
            && $metadata->partitions !== []
            && array_filter($metadata->partitions, static fn($partition): bool => $partition->leader < 0) === [];
        if ($hasLeaders) {
            return;
        }
        usleep(250000);
    } while (microtime(true) < $deadline);

    throw new RuntimeException("The broker did not make the topic {$topic} available within 30 seconds");
}

/**
 * Appends a few records to the first partition of the topic, so that the example has something to consume
 */
function produceDemoRecords(string $brokerAddress, string $topic, array $configuration, int $count): void
{
    $records = [];
    for ($index = 0; $index < $count; $index++) {
        $records[] = new Record(
            sprintf('Hello from the example #%d, produced at %s', $index, date(DATE_ATOM)),
            'key-' . $index
        );
    }

    $stream = new SocketStream($brokerAddress, $configuration, 5.0);
    new ProduceRequest(
        [$topic => [0 => MessageSet::fromRecords($records)]],
        1,
        5000,
        'kafka-client-example',
        1
    )->writeTo($stream);

    $partition = ProduceResponse::unpack($stream)->topics[$topic]->partitions[0];
    if ($partition->errorCode !== 0) {
        throw KafkaException::fromCode($partition->errorCode, ['topic' => $topic, 'partitionId' => 0]);
    }

    printf("Produced %d records into %s:0 at the offset %d%s", $count, $topic, $partition->baseOffset, PHP_EOL);
}

awaitTopic($brokerAddress, $topic, $configuration);
if ($shouldProduce) {
    produceDemoRecords($brokerAddress, $topic, $configuration, 5);
}

$consumer = new KafkaConsumer($configuration);

// assign() picks the partitions to read by hand - here simply all of them - and joins no group at all;
// examples/consumer-group.php is the same example with the broker-side group membership of Kafka 0.9
$partitionIds = array_keys($consumer->partitionsFor($topic));
sort($partitionIds);
$consumer->assign([$topic => $partitionIds]);

// Without this the consumer would start at the committed offsets of the group, or, for a group that has none, at
// the position that `auto.offset.reset` selects
$consumer->seekToBeginning([$topic => $partitionIds]);

printf(
    'Consuming %s (partitions %s) as the group "%s" from %s%s',
    $topic,
    implode(', ', $partitionIds),
    $groupId,
    $bootstrapServers,
    PHP_EOL
);

$received   = 0;
$emptyPolls = 0;
while ($emptyPolls < 3) {
    $isEmpty = true;

    // poll() returns [topic][partition] => records, in offset order
    foreach ($consumer->poll(1000) as $polledTopic => $partitions) {
        foreach ($partitions as $partitionId => $records) {
            foreach ($records as $record) {
                $isEmpty = false;
                $received++;

                $value = $record instanceof ConsumerRecord ? $record->deserializedValue : $record->value;
                printf(
                    '%s:%d@%d key=%s value=%s%s',
                    $polledTopic,
                    $partitionId,
                    (int) $record->offset,
                    $record->key ?? '<null>',
                    is_string($value) ? $value : var_export($value, true),
                    PHP_EOL
                );
            }
        }
    }

    $emptyPolls = $isEmpty ? $emptyPolls + 1 : 0;
}

// The positions of a poll() are the offsets of the records the next poll() would return, which is what a commit
// stores: a consumer that resumes from them does not receive the last record again.
$consumer->commitSync();

printf('Received %d records, committed %s%s', $received, json_encode($consumer->committed([$topic => $partitionIds])), PHP_EOL);

foreach ($consumer->assignment() as $assignedTopic => $assignedPartitions) {
    foreach ($assignedPartitions as $partitionId) {
        printf('Position of %s:%d is now %d%s', $assignedTopic, $partitionId, $consumer->position($assignedTopic, $partitionId), PHP_EOL);
    }
}

$consumer->unsubscribe();
