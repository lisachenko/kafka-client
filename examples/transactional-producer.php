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
 * The consume-transform-produce loop of KIP-98 against a Kafka 0.11 cluster: read a topic, write a derived topic
 * and commit the offsets of the input **inside the same transaction**, so that a reader of the output either sees
 * a whole batch of work or none of it.
 *
 * Run it against the broker of the development environment:
 *
 *   docker compose up -d
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/transactional-producer.php [input-topic] [output-topic]
 *
 * The example writes three records into the input topic first, so that it has something to transform, and it runs
 * the loop once. Read the output back with a `read_committed` console consumer to see what a transaction makes
 * visible:
 *
 *   docker exec kafka-1-1-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
 *       --topic kafka-client-example-output --from-beginning --isolation-level read_committed
 *
 * Three rules the transactional API imposes, and the reason for each:
 *
 * * `initTransactions()` is called **once**, before anything is sent. It fences every earlier producer of the same
 *   `transactional.id` and rolls back whatever that producer left open, which is what makes the guarantee survive
 *   a restart - and what makes two live producers of one id impossible.
 * * every `send()` happens **between** `beginTransaction()` and `commitTransaction()`/`abortTransaction()`; a send
 *   outside of one is refused by this client, because a broker would refuse the batch with the error code 48.
 * * the consumer of the input reads `isolation.level = read_committed` and **must not commit its own offsets**
 *   (`enable.auto.commit = false`): the producer commits them, inside the transaction.
 */

use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;

require __DIR__ . '/../vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$inputTopic       = $argv[1] ?? 'kafka-client-example-input';
$outputTopic      = $argv[2] ?? 'kafka-client-example-output';
$clientId         = 'kafka-client-example-transactional';
$transactionalId  = 'kafka-client-example-etl';
$groupId          = 'kafka-client-example-etl-group';
$brokerAddresses  = array_map(
    static fn(string $server): string => 'tcp://' . trim($server),
    explode(',', $bootstrapServers)
);

/**
 * Waits until every partition of a topic has an elected leader, creating the topic on the way
 */
function awaitTopic(string $address, string $topic, string $clientId, float $timeout = 30.0): void
{
    $deadline = microtime(true) + $timeout;
    $attempt  = 0;
    do {
        $attempt++;
        $stream = new SocketStream($address, [ClientConfig::REQUEST_TIMEOUT_MS => 5000], 5.0);
        new MetadataRequest([$topic], true, $clientId, $attempt)->writeTo($stream);

        $topicMetadata = MetadataResponse::unpack($stream)->topics[$topic] ?? null;
        $leaderless    = array_filter(
            $topicMetadata->partitions ?? [],
            static fn(PartitionMetadata $partition): bool => $partition->leader < 0
        );
        if ($topicMetadata !== null && $topicMetadata->partitions !== [] && $leaderless === []) {
            return;
        }
        usleep(250000);
    } while (microtime(true) < $deadline);

    throw new RuntimeException("No leader was elected for the partitions of {$topic}");
}

awaitTopic($brokerAddresses[0], $inputTopic, $clientId);
awaitTopic($brokerAddresses[0], $outputTopic, $clientId);

// --- the input of the loop, written by an ordinary producer ------------------------------------------------------
$input = new KafkaProducer([
    ProducerConfig::BOOTSTRAP_SERVERS => $brokerAddresses,
    ProducerConfig::CLIENT_ID         => $clientId . '-input',
    ProducerConfig::ACKS              => ProducerConfig::ACKS_ALL,
]);
foreach (['one', 'two', 'three'] as $value) {
    $input->send($inputTopic, Record::fromValue($value), 0);
}
$input->flush();

echo "Wrote three records into {$inputTopic}\n";

// --- the consumer of the loop ------------------------------------------------------------------------------------
$consumer = new KafkaConsumer([
    ConsumerConfig::BOOTSTRAP_SERVERS  => $brokerAddresses,
    ConsumerConfig::CLIENT_ID          => $clientId . '-consumer',
    ConsumerConfig::GROUP_ID           => $groupId,
    ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,

    // The two options a consumer of a transactional pipeline has to set: it only ever sees committed records, and
    // it never commits its own position - the producer does that inside the transaction
    ConsumerConfig::ISOLATION_LEVEL    => ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED,
    ConsumerConfig::ENABLE_AUTO_COMMIT => false,
]);
$consumer->assign([$inputTopic => new PartitionsForTopic($inputTopic, [0])]);

// --- the transactional producer of the loop ----------------------------------------------------------------------
$producer = new KafkaProducer([
    ProducerConfig::BOOTSTRAP_SERVERS => $brokerAddresses,
    ProducerConfig::CLIENT_ID         => $clientId,

    // The one option that turns transactions on. It implies `enable.idempotence`, and with it `acks = all` and a
    // non-zero `retries`; the id has to be stable across restarts and unique in the cluster.
    ProducerConfig::TRANSACTIONAL_ID => $transactionalId,

    // How long the coordinator waits for a status update of an open transaction before it aborts it; it has to be
    // below the broker's `transaction.max.timeout.ms` (900000 by default)
    ProducerConfig::TRANSACTION_TIMEOUT_MS => 60000,

    ProducerConfig::BATCH_SIZE => 16384,
]);

// Once, before anything is sent: fences every earlier producer of this transactional id
$producer->initTransactions();

echo "Reading {$inputTopic} and writing {$outputTopic} in one transaction\n";

$records = $consumer->poll(5000)[$inputTopic][0] ?? [];
if ($records === []) {
    echo "Nothing to transform - the input topic is empty.\n";

    exit(0);
}

$producer->beginTransaction();

try {
    foreach ($records as $record) {
        $transformed = strtoupper((string) $record->value);
        $producer->send($outputTopic, Record::fromValue($transformed), 0);

        echo "  {$record->value} @ {$record->offset} => {$transformed}\n";
    }

    // Everything that is buffered has to be out before the offsets are added: a record that is still in the
    // producer when the transaction ends is not part of it
    $producer->flush();

    // The offsets of the input, committed by the **producer** and therefore part of the transaction. The value is
    // the offset of the next record to read, which is exactly what `position()` answers.
    $producer->sendOffsetsToTransaction([$inputTopic => [0 => $consumer->position($inputTopic, 0)]], $groupId);

    // One EndTxn request; the coordinator writes the COMMIT markers into the partitions right after it answers, so
    // a `read_committed` consumer sees the output a moment later
    $producer->commitTransaction();

    echo "Committed.\n";
} catch (KafkaException $exception) {
    // The only way out of a failed transaction: everything that was written is marked as aborted and a
    // `read_committed` consumer never sees it, and the group keeps the offsets it had before
    $producer->abortTransaction();

    echo 'The transaction was rolled back: ' . $exception->getMessage() . "\n";

    exit(1);
}

$consumer->close();

echo "Read the output back with a read_committed consumer:\n";
echo "  docker exec kafka-1-1-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092"
    . " --topic {$outputTopic} --from-beginning --isolation-level read_committed --max-messages "
    . count($records) . "\n";
