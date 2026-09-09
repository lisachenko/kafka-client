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
 * Consumes a topic as a member of a consumer group of a Kafka 0.10.2.2 cluster.
 *
 * Kafka 0.9 moved the coordination of a group into the broker, so the partitions are not chosen by the application
 * any more ({@see examples/consumer.php} does that with `assign()`): the consumer subscribes to topics, the group
 * coordinator hands out the partitions, and the members of the group split them among themselves.
 *
 * Start the broker of this repository and run the example against it:
 *
 *   docker compose up -d
 *   php examples/consumer-group.php
 *   php examples/consumer-group.php my-topic my-group          # another topic and consumer group
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/consumer-group.php
 *
 * Start it a second time in another shell while the first one is running, and watch the two of them split the
 * partitions of the topic: both print the assignment they receive whenever the group rebalances.
 *
 * The example seeds the topic with a handful of records first, so that there is always something to read; pass
 * `--no-produce` to consume what is already in the log.
 *
 * **The heartbeat of a member is sent from poll(), because PHP has no background thread.** A consumer that stops
 * polling for longer than `session.timeout.ms` is dropped by the coordinator and its partitions are given to the
 * other members of the group, so the processing of a batch has to stay well below that - or the session timeout
 * has to be raised, within the `group.min.session.timeout.ms`/`group.max.session.timeout.ms` of the broker.
 */

use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Serialization\StringDeserializer;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerRebalanceListener;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\Consumer\RangeAssignor;
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
    // The coordinator answers a JoinGroup only once the whole rebalance is over, which can take a full rebalance
    // timeout, so the read timeout of the socket has to be larger than session.timeout.ms and max.poll.interval.ms
    ClientConfig::REQUEST_TIMEOUT_MS => 40000,

    ConsumerConfig::GROUP_ID                      => $groupId,
    // `range` (the default) or `roundrobin`, or the name of a class that implements PartitionAssignorInterface;
    // every member of a group has to offer the same one, the coordinator refuses the join otherwise (error 23)
    ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY => RangeAssignor::NAME,
    ConsumerConfig::SESSION_TIMEOUT_MS            => 10000,
    // The `rebalance_timeout` of the JoinGroup v1 request (Kafka 0.10.1): how long the coordinator waits for this
    // member to rejoin a rebalance. The default of 300000 would need a request.timeout.ms above five minutes.
    ConsumerConfig::MAX_POLL_INTERVAL_MS          => 30000,
    ConsumerConfig::HEARTBEAT_INTERVAL_MS         => 3000,
    ConsumerConfig::AUTO_OFFSET_RESET             => OffsetResetStrategy::EARLIEST,
    // The positions are committed by hand below, with the member id and the generation of this consumer
    ConsumerConfig::ENABLE_AUTO_COMMIT            => false,
    ConsumerConfig::VALUE_DESERIALIZER            => StringDeserializer::class,
];

/**
 * Waits until the broker has created the topic and elected a leader for each of its partitions.
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
 * Appends a few records to every partition of the topic, so that the example has something to consume
 */
function produceDemoRecords(string $brokerAddress, string $topic, array $configuration, int $perPartition): void
{
    $stream = new SocketStream($brokerAddress, $configuration, 5.0);
    new MetadataRequest([$topic], 'kafka-client-example', 1)->writeTo($stream);
    $partitionIds = array_keys(MetadataResponse::unpack($stream)->topics[$topic]->partitions);

    $topicPartitions = [];
    foreach ($partitionIds as $partitionId) {
        $records = [];
        for ($index = 0; $index < $perPartition; $index++) {
            $records[] = new Record(
                sprintf('Hello from partition %d, record #%d, produced at %s', $partitionId, $index, date(DATE_ATOM))
            );
        }
        $topicPartitions[$partitionId] = MessageSet::fromRecords($records);
    }

    $stream = new SocketStream($brokerAddress, $configuration, 5.0);
    new ProduceRequest([$topic => $topicPartitions], 1, 5000, 'kafka-client-example', 1)->writeTo($stream);

    foreach (ProduceResponse::unpack($stream)->topics[$topic]->partitions as $partitionId => $partition) {
        if ($partition->errorCode !== 0) {
            throw KafkaException::fromCode($partition->errorCode, ['topic' => $topic, 'partitionId' => $partitionId]);
        }
        printf('Produced %d records into %s:%d%s', $perPartition, $topic, $partitionId, PHP_EOL);
    }
}

/**
 * Prints what every rebalance of the group takes away from this consumer and gives to it
 */
final class PrintingRebalanceListener implements ConsumerRebalanceListener
{
    public function onPartitionsRevoked(array $partitions): void
    {
        printf('Rebalance: giving up %s%s', json_encode($partitions), PHP_EOL);
    }

    public function onPartitionsAssigned(array $partitions): void
    {
        printf('Rebalance: now consuming %s%s', json_encode($partitions), PHP_EOL);
    }
}

awaitTopic($brokerAddress, $topic, $configuration);
if ($shouldProduce) {
    produceDemoRecords($brokerAddress, $topic, $configuration, 3);
}

$consumer = new KafkaConsumer($configuration);

// Nothing is sent to the broker yet: the group is joined by the first poll(), which also brings the assignment
$consumer->subscribe([$topic], new PrintingRebalanceListener());

printf(
    'Consuming %s as a member of the group "%s" on %s, stop with Ctrl+C%s',
    $topic,
    $groupId,
    $bootstrapServers,
    PHP_EOL
);

$received   = 0;
$emptyPolls = 0;

// A real daemon polls forever; this example stops once the topic has been drained for a few polls in a row
while ($emptyPolls < 5) {
    $isEmpty = true;

    // poll() keeps the membership alive - it sends the heartbeat and rejoins the group when it has to - and
    // returns [topic][partition] => records, in offset order
    foreach ($consumer->poll(1000) as $polledTopic => $partitions) {
        foreach ($partitions as $partitionId => $records) {
            foreach ($records as $record) {
                $isEmpty = false;
                $received++;

                $value = $record instanceof ConsumerRecord ? $record->deserializedValue : $record->value;
                printf(
                    '%s:%d@%d %s%s',
                    $polledTopic,
                    $partitionId,
                    (int) $record->offset,
                    is_string($value) ? $value : var_export($value, true),
                    PHP_EOL
                );
            }
        }
    }

    // The positions of a poll() are the offsets of the records the next poll() would return; the commit carries
    // the member id and the generation of this consumer, so the coordinator refuses it once they are stale
    $consumer->commitSync();

    $emptyPolls = $isEmpty ? $emptyPolls + 1 : 0;
}

printf('Received %d records, committed %s%s', $received, json_encode($consumer->committed([$topic => array_keys($consumer->assignment()[$topic] ?? [])])), PHP_EOL);

// close() commits once more when `enable.auto.commit` is on and leaves the group with a LeaveGroup request, so
// that the other members take these partitions over right away instead of after a session timeout
$consumer->close();
