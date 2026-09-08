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
 * Produces a handful of records to a topic of a Kafka 0.8.2.2 cluster.
 *
 * Run it against the broker of the development environment:
 *
 *   docker compose up -d
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/producer.php [topic]
 *
 * The topic is created by the broker on the first request when `auto.create.topics.enable` is on; a topic that has
 * just been created has no leader for a moment, so the example waits for the metadata to settle before sending.
 */

use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;

require __DIR__ . '/../vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$topic            = $argv[1] ?? 'kafka-client-example';
$clientId         = 'kafka-client-example-producer';
$brokerAddresses  = array_map(
    static fn(string $server): string => 'tcp://' . trim($server),
    explode(',', $bootstrapServers)
);

/**
 * Waits until every partition of the topic has an elected leader, creating the topic on the way.
 *
 * A broker with `auto.create.topics.enable` creates a topic when a client asks for the metadata *of that topic*,
 * and the controller needs a moment to elect the leaders of its partitions afterwards. Until it has, a produce
 * request would be answered with LeaderNotAvailable (5) or NotLeaderForPartition (6).
 */
function awaitTopic(string $address, string $topic, string $clientId, float $timeout = 30.0): void
{
    $deadline = microtime(true) + $timeout;
    $attempt  = 0;
    do {
        $attempt++;
        $stream = new SocketStream($address, [ClientConfig::REQUEST_TIMEOUT_MS => 5000], 5.0);
        new MetadataRequest([$topic], $clientId, $attempt)->writeTo($stream);

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

awaitTopic($brokerAddresses[0], $topic, $clientId);

$producer = new KafkaProducer([
    ProducerConfig::BOOTSTRAP_SERVERS => $brokerAddresses,
    ProducerConfig::CLIENT_ID         => $clientId,

    // Wait for every in-sync replica to store the record; use ACKS_LEADER for a faster, less durable write
    ProducerConfig::ACKS       => ProducerConfig::ACKS_ALL,
    ProducerConfig::TIMEOUT_MS => 5000,

    // Collect up to 16 KiB of records, but never wait longer than 100 ms for a batch to fill up
    ProducerConfig::BATCH_SIZE => 16384,
    ProducerConfig::LINGER_MS  => 100,

    // Compress every batch as a whole; 'none' and 'snappy' are the other supported values
    ProducerConfig::COMPRESSION_TYPE => ProducerConfig::COMPRESSION_TYPE_GZIP,

    // Send a batch again when the leader of its partition moved while it was in flight
    ProducerConfig::RETRIES          => 3,
    ProducerConfig::RETRY_BACKOFF_MS => 200,
]);

echo "Partitions of {$topic}:\n";
foreach ($producer->partitionsFor($topic) as $partitionMetadata) {
    echo "  {$partitionMetadata->partitionId} => leader {$partitionMetadata->leader}\n";
}

$onSuccess = static function (RecordMetadata $metadata): void {
    echo "  stored {$metadata}\n";
};
$onFailure = static function (\Throwable $error): void {
    echo '  failed: ' . $error->getMessage() . "\n";
};

// A record with a key always goes to the partition that the murmur2 hash of the key selects
for ($index = 0; $index < 10; $index++) {
    $producer
        ->send($topic, Record::fromKeyValue("user-{$index}", "Message #{$index} at " . date(DATE_ATOM)))
        ->then($onSuccess, $onFailure);
}

// A record without a key is spread over the available partitions in a round-robin fashion
$producer->send($topic, Record::fromValue('A message without a key'))->then($onSuccess, $onFailure);

// ... and one record for an explicitly chosen partition
$producer->send($topic, Record::fromValue('A message for partition 0'), 0)->then($onSuccess, $onFailure);

echo "Sending:\n";

try {
    // Sends everything that is still buffered and settles the promises of the batches above
    $producer->flush();
} catch (KafkaException $exception) {
    echo 'The cluster refused the batch: ' . $exception->getMessage() . "\n";

    exit(1);
}

echo "Done. Read the records back with the console consumer of the broker container:\n";
echo "  docker exec kafka08 /opt/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181"
    . " --topic {$topic} --from-beginning --max-messages 12\n";
