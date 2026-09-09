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
 * Finds the offset of a point in time, which Kafka 0.10.1 made possible (KIP-79).
 *
 * Message format v1 gave every record a timestamp, and version 1 of the Offsets api turned that into a question a
 * client can ask: "which is the first record of this partition whose timestamp is at or after t?". The answer is
 * one offset per partition together with the timestamp of the record it points at - `null` here when the partition
 * holds no such record, which is what a timestamp above the last record, and any timestamp on an empty partition,
 * comes back as. `beginningOffsets()` and `endOffsets()` are the two special timestamps -2 and -1 of the same api.
 *
 * All three are pure queries: nothing is consumed, no group is joined, and no assignment is needed. This example
 * produces five records one second apart, asks for the offset of the middle one and reads from there.
 *
 *   docker compose up -d
 *   php examples/offsets-for-times.php
 *   php examples/offsets-for-times.php my-topic
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/offsets-for-times.php
 */

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\Common\Serialization\StringDeserializer;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;

require dirname(__DIR__) . '/vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$topic            = $argv[1] ?? 'kafka-client-example-times';
$partition        = 0;

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . trim(explode(',', $bootstrapServers)[0])],
    ClientConfig::CLIENT_ID         => 'kafka-client-example-times',
];

// The topic has to exist and its partitions need a leader before anything can be produced to them; CreateTopics
// (Kafka 0.10.1) is the explicit way of getting there, and 36 (TopicAlreadyExists) simply means it is already
// around. The controller elects the leaders a moment later, which is what the loop below waits for.
$cluster = Cluster::bootstrap($configuration);
$admin   = new AdminClient($cluster, $configuration);
$created = $admin->createTopics([new NewTopic($topic, 3, 1)])[$topic] ?? null;
if ($created !== null && $created->getCode() !== KafkaException::TOPIC_ALREADY_EXISTS) {
    echo 'The topic could not be created: ' . $created->getMessage() . "\n";

    exit(1);
}

$deadline = microtime(true) + 30.0;
do {
    $cluster->reload();
    $partitions = $admin->describeTopics([$topic])[$topic]?->partitions ?? [];
    $leaderless = array_filter($partitions, static fn($metadata): bool => $metadata->leader < 0);
    if ($partitions !== [] && $leaderless === []) {
        break;
    }
    usleep(250000);
} while (microtime(true) < $deadline);

if ($partitions === [] || $leaderless !== []) {
    echo "No leader was elected for the partitions of {$topic}\n";

    exit(1);
}

// Five records whose CreateTime values are one second apart. A record that carries its own timestamp keeps it -
// the producer only stamps the ones that have none - which is what makes the times of this example predictable.
$now      = (int) (microtime(true) * 1000);
$producer = new KafkaProducer($configuration + [ProducerConfig::ACKS => ProducerConfig::ACKS_ALL]);
for ($index = 0; $index < 5; $index++) {
    $record            = Record::fromValue("Message #{$index}");
    $record->timestamp = $now + $index * 1000;
    $producer->send($topic, $record, $partition);
}

try {
    $producer->flush();
} catch (KafkaException $exception) {
    echo 'The cluster refused the batch: ' . $exception->getMessage() . "\n";

    exit(1);
}
echo "Produced five records to {$topic}:{$partition}, one second apart from " . date('H:i:s', intdiv($now, 1000)) . "\n\n";

$consumer       = new KafkaConsumer($configuration + [
    ConsumerConfig::GROUP_ID           => 'kafka-client-example-times-group',
    ConsumerConfig::ENABLE_AUTO_COMMIT => false,
    ConsumerConfig::VALUE_DESERIALIZER => StringDeserializer::class,
]);
$topicPartition = [$topic => [$partition]];

// -2 and -1: the two ends of the log, and pure queries as well
$beginning = $consumer->beginningOffsets($topicPartition)[$topic][$partition];
$end       = $consumer->endOffsets($topicPartition)[$topic][$partition];
echo "The log holds the offsets {$beginning} .. {$end}\n";

// The middle record, and a timestamp between two records: the answer is always the LATER of the two
foreach ([$now => 'the first record', $now + 2500 => 'between the third and the fourth record'] as $time => $what) {
    $found = $consumer->offsetsForTimes([$topic => [$partition => $time]])[$topic][$partition];
    echo 'Offset at ', date('H:i:s', intdiv($time, 1000)), " ({$what}): ",
        $found === null ? "none - no record is that new\n" : "{$found->offset}, stamped {$found->timestamp}\n";
}

// A timestamp above the last record is not an error: the broker answers the code 0 with the offset -1, which this
// client reports as null
$afterTheEnd = $consumer->offsetsForTimes([$topic => [$partition => $now + 3600000]])[$topic][$partition];
echo 'Offset an hour from now: ', $afterTheEnd === null ? "null, as expected\n" : "{$afterTheEnd->offset}\n";

// Reading from what was found: offsetsForTimes() moves nothing, the consumer seeks there itself
$from = $consumer->offsetsForTimes([$topic => [$partition => $now + 2500]])[$topic][$partition];
if ($from !== null) {
    $consumer->assign($topicPartition);
    $consumer->seek($topic, $partition, $from->offset);

    echo "\nConsuming from the offset {$from->offset}\n";
    $emptyPolls = 0;
    while ($emptyPolls < 3) {
        $isEmpty = true;
        foreach ($consumer->poll(1000)[$topic][$partition] ?? [] as $record) {
            $isEmpty = false;
            $value   = $record instanceof ConsumerRecord ? $record->deserializedValue : $record->value;
            printf(
                '  @%d %s %s (%s)%s',
                (int) $record->offset,
                is_string($value) ? $value : var_export($value, true),
                (string) $record->timestamp,
                TimestampType::name($record->timestampType),
                PHP_EOL
            );
        }
        $emptyPolls = $isEmpty ? $emptyPolls + 1 : 0;
    }
}

// A topic whose `message.format.version` is older than 0.10.0 has no timestamps to search: the broker answers the
// error code 43 (UnsupportedForMessageFormat) for it, which arrives as a TopicPartitionRequestException.
echo "\nDone. The same lookup against a topic of the message format of 0.9 would answer the error code 43.\n";
