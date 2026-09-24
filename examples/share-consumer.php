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
 * Consumes a topic as a member of a **share group** (KIP-932, Kafka 4.1) of a Kafka 4.3.1 cluster.
 *
 * The members of a share group do not own partitions, they share them record by record: every record is delivered to
 * one member at a time, which acknowledges it - accept, release, reject or renew - and a released record comes back
 * with a higher delivery count, to this member or another one. Start it twice in two shells to watch two members
 * share the partitions of one topic without ever holding the same record.
 *
 *   docker compose up -d
 *   php examples/share-consumer.php
 *   php examples/share-consumer.php my-topic my-share-group      # another topic and share group
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/share-consumer.php
 *
 * The example seeds the topic with a handful of records first (pass `--no-produce` to consume what is already in the
 * log) and sets the group config `share.auto.offset.reset` to `earliest`, because a share group reads from `latest`
 * by default. It uses the **explicit** acknowledgement mode: it accepts every record, but releases each record once
 * on its first delivery, so that every record is shown twice - with the delivery count 1 and then 2.
 *
 * **Everything happens inside the calls of the consumer, because PHP has no background thread**: poll() joins the
 * group and sends the heartbeat, the acknowledgements travel with the next poll() or commit, and close() closes the
 * share sessions - releasing what the member still holds - and leaves the group.
 */

use Protocol\Kafka\Admin\AlterConfigOp;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Serialization\StringDeserializer;
use Protocol\Kafka\Consumer\AcknowledgeType;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\KafkaShareConsumer;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequestV12;
use Protocol\Kafka\Protocol\Request\ProduceResponseV12;

require dirname(__DIR__) . '/vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$brokerAddress    = 'tcp://' . trim(explode(',', $bootstrapServers)[0]);
$arguments        = array_values(array_filter(array_slice($argv, 1), static fn(string $argument): bool => !str_starts_with($argument, '--')));
$topic            = $arguments[0] ?? 'kafka-client-share-example';
$groupId          = $arguments[1] ?? 'kafka-client-share-example-group';
$shouldProduce    = !in_array('--no-produce', $argv, true);

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => [$brokerAddress],
    ClientConfig::CLIENT_ID         => 'kafka-client-share-example',
    ClientConfig::REQUEST_TIMEOUT_MS => 30000,

    ConsumerConfig::GROUP_ID                   => $groupId,
    // `implicit` (the default) accepts what a poll() returned with the next poll() or commit; `explicit` wants an
    // acknowledgement of every record before the next poll()
    ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_EXPLICIT,
    // `record_limit` has the node acquire no more than `max.poll.records` records (KIP-1206, Kafka 4.2)
    ConsumerConfig::SHARE_ACQUIRE_MODE         => ConsumerConfig::SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED,
    ConsumerConfig::MAX_POLL_RECORDS           => 100,
    ConsumerConfig::VALUE_DESERIALIZER         => StringDeserializer::class,
    // `auto.offset.reset`, `enable.auto.commit`, `group.protocol` and the other options of a classic consumer are
    // refused: where a share group starts reading is its group config `share.auto.offset.reset`
];

/**
 * Waits until the broker has created the topic and elected a leader for each of its partitions, and returns them.
 *
 * @return list<int>
 */
function awaitTopic(string $brokerAddress, string $topic, array $configuration): array
{
    $deadline = microtime(true) + 30.0;
    $attempt  = 0;

    do {
        $stream = new SocketStream($brokerAddress, $configuration, 5.0);
        new MetadataRequest([$topic], true, 'kafka-client-share-example', ++$attempt)->writeTo($stream);
        $metadata = MetadataResponse::unpack($stream)->topics[$topic] ?? null;

        $hasLeaders = $metadata !== null
            && $metadata->partitions !== []
            && array_filter($metadata->partitions, static fn($partition): bool => $partition->leader < 0) === [];
        if ($hasLeaders) {
            return array_keys($metadata->partitions);
        }
        usleep(250000);
    } while (microtime(true) < $deadline);

    throw new RuntimeException("The broker did not make the topic {$topic} available within 30 seconds");
}

/**
 * Sets the group config `share.auto.offset.reset` of the share group, the config resource type 32 of KIP-848
 */
function readFromEarliest(string $brokerAddress, string $groupId, array $configuration): void
{
    $stream = new SocketStream($brokerAddress, $configuration, 5.0);
    new IncrementalAlterConfigsRequest(
        [new IncrementalAlterConfigsRequestResource(32, $groupId, [AlterConfigOp::set('share.auto.offset.reset', 'earliest')])],
        false,
        'kafka-client-share-example'
    )->writeTo($stream);

    $answer = IncrementalAlterConfigsResponse::unpack($stream)->responses[0];
    if ($answer->errorCode !== 0) {
        throw KafkaException::fromCode($answer->errorCode, ['groupId' => $groupId]);
    }
}

/**
 * Appends a few records to every partition of the topic, so that the example has something to consume
 *
 * @param list<int> $partitionIds
 */
function produceDemoRecords(string $brokerAddress, string $topic, array $partitionIds, array $configuration): void
{
    $topicPartitions = [];
    foreach ($partitionIds as $partitionId) {
        $records = [];
        for ($index = 0; $index < 3; $index++) {
            $records[] = new Record(sprintf('Shared record #%d of partition %d, produced at %s', $index, $partitionId, date(DATE_ATOM)));
        }
        $topicPartitions[$partitionId] = RecordBatch::fromRecords($records);
    }

    $stream = new SocketStream($brokerAddress, $configuration, 5.0);
    new ProduceRequestV12([$topic => $topicPartitions], 1, 5000, 'kafka-client-share-example', 1)->writeTo($stream);
    foreach (ProduceResponseV12::unpack($stream)->topics[$topic]->partitions as $partitionId => $partition) {
        if ($partition->errorCode !== 0) {
            throw KafkaException::fromCode($partition->errorCode, ['topic' => $topic, 'partitionId' => $partitionId]);
        }
    }
    printf('Produced 3 records into each of the %d partitions of %s%s', count($partitionIds), $topic, PHP_EOL);
}

$partitionIds = awaitTopic($brokerAddress, $topic, $configuration);
readFromEarliest($brokerAddress, $groupId, $configuration);
if ($shouldProduce) {
    produceDemoRecords($brokerAddress, $topic, $partitionIds, $configuration);
}

$consumer = new KafkaShareConsumer($configuration);
// Called once per topic-partition of every request that carried acknowledgements, inside the call that sent it
$consumer->setAcknowledgementCommitCallback(static function (array $offsets, ?KafkaException $exception): void {
    foreach ($offsets as $ackTopic => $partitions) {
        foreach ($partitions as $partition => $partitionOffsets) {
            printf(
                '  acknowledged %s:%d %s: %s%s',
                $ackTopic,
                $partition,
                json_encode($partitionOffsets),
                $exception === null ? 'ok' : $exception->getMessage(),
                PHP_EOL
            );
        }
    }
});
$consumer->subscribe([$topic]);

printf('Consuming %s as a member of the share group "%s" on %s%s', $topic, $groupId, $bootstrapServers, PHP_EOL);

$emptyPolls = 0;
// A real daemon polls forever; this example stops once nothing came back for a few polls in a row
while ($emptyPolls < 5) {
    $records = $consumer->poll(1000);
    $emptyPolls = $records === [] ? $emptyPolls + 1 : 0;

    foreach ($records as $polledTopic => $partitions) {
        foreach ($partitions as $partitionId => $partitionRecords) {
            foreach ($partitionRecords as $record) {
                printf(
                    '%s:%d@%d delivery #%d %s%s',
                    $polledTopic,
                    $partitionId,
                    (int) $record->offset,
                    (int) $record->deliveryCount,
                    (string) $record->deserializedValue,
                    PHP_EOL
                );
                // A released record is delivered again with the next delivery count, a rejected one never again
                $consumer->acknowledge($record, $record->deliveryCount === 1 ? AcknowledgeType::RELEASE : AcknowledgeType::ACCEPT);
            }
        }
    }

    // Sends the acknowledgements now, in a ShareAcknowledge, instead of with the next poll()
    $consumer->commitSync();
}

printf('The acquisition lock of this group is %d ms%s', (int) $consumer->acquisitionLockTimeoutMs(), PHP_EOL);

// Closes the share session on every leader - which releases what this member still holds - and leaves the group
$consumer->close();
