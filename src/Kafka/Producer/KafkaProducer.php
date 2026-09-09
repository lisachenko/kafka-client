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
 * @author Alexander.Lisachenko
 * @date   29.07.2016
 */

namespace Protocol\Kafka\Producer;

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\MessageTooLargeException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Record\RecordV2;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use React\Promise\Deferred;
use React\Promise\Promise;

/**
 * A Kafka client that publishes records to the Kafka cluster.
 *
 * Records are buffered per topic-partition and handed over to the broker in batches; every {@see KafkaProducer::send()}
 * hands back a promise that is resolved with the {@see RecordMetadata} of the batch as soon as the broker acknowledged
 * it, and rejected with the mapped {@see KafkaException} when it did not:
 *
 * <code>
 *   $producer = new KafkaProducer([
 *       ProducerConfig::BOOTSTRAP_SERVERS => ['tcp://127.0.0.1:9092'],
 *       ProducerConfig::CLIENT_ID         => 'my-application',
 *       ProducerConfig::ACKS              => ProducerConfig::ACKS_ALL,
 *       ProducerConfig::BATCH_SIZE        => 16384,
 *       ProducerConfig::LINGER_MS         => 50,
 *       ProducerConfig::COMPRESSION_TYPE  => ProducerConfig::COMPRESSION_TYPE_GZIP,
 *       ProducerConfig::RETRIES           => 3,
 *   ]);
 *
 *   $producer->send('my-topic', Record::fromKeyValue('user-42', 'hello'))->then(
 *       function (RecordMetadata $metadata): void {
 *           echo "stored as {$metadata}\n";                        // my-topic-1@17
 *           echo "throttled for {$metadata->throttleTimeMs} ms\n";   // 0 unless a produce quota was exceeded
 *       },
 *       function (\Throwable $error): void {
 *           echo "not stored: {$error->getMessage()}\n";
 *       }
 *   );
 *
 *   $producer->flush(); // blocks until every buffered batch was acknowledged
 * </code>
 *
 * The promises are settled while {@see KafkaProducer::flush()} runs, which is either the call above, the automatic
 * flush of a full batch inside {@see KafkaProducer::send()}, or the destructor of the producer. There is no event
 * loop behind them: this is a synchronous client, the promise only carries the result of a send back to its caller.
 *
 * A batch whose topic-partitions failed with a retriable error is sent again by {@see Client::produce()}, `retries`
 * times with `retry.backoff.ms` in between and with a refresh of the cluster metadata before each attempt; the
 * default of that option is 0, so a batch is sent exactly once unless the producer is configured to retry.
 *
 * Without a key a record goes to the next available partition in a round-robin fashion, with a key it goes to the
 * partition that the murmur2 hash of the key selects, exactly like the official Java client, see
 * {@see DefaultPartitioner}.
 *
 * Every record is stamped with a **CreateTime** - the current time in milliseconds - unless it already carries a
 * {@see Record::$timestamp}, and the batch is written in the message format that `message.format.version` selects,
 * v2 (the record batch) by default. {@see RecordMetadata::$timestamp} reports the create time of the first record
 * of the acknowledged batch; the `LogAppendTime` that a broker assigns to a topic configured for it is only
 * visible through version 2 of the Produce API and above.
 *
 * The **headers** of a record ({@see Record::withHeaders()}, KIP-82) travel with it in the message format v2 and
 * only there: a batch of the formats v0 and v1 has no place for them and drops them silently, and a consumer only
 * ever sees them in an answer of Fetch v4 or above.
 *
 * A broker with a `producer_byte_rate` quota for the `client.id` of this producer does not reject anything: it
 * appends the batch and holds its answer back until the client is inside its quota again. That delay is what
 * {@see RecordMetadata::$throttleTimeMs} reports, and {@see KafkaProducer::flush()} simply takes that much longer.
 *
 * @see examples/producer.php for a runnable example
 * @see docs/protocol/0.11.0.md, section "Quotas and throttle time"
 */
class KafkaProducer
{
    /**
     * The producer configs
     *
     * @var array<string, mixed>
     */
    private array $configuration;

    /**
     * Kafka cluster configuration
     */
    private ?Cluster $cluster = null;

    /**
     * Low-level kafka client
     */
    private ?Client $client = null;

    /**
     * Instance of partitioner
     */
    private readonly PartitionerInterface $partitioner;

    /**
     * Size of the buffered batch in bytes, as it will be serialized into the produce request
     */
    private int $batchSize = 0;

    /**
     * Size of the buffered records of each topic-partition in bytes
     *
     * @var array<string, array<int, int>>
     */
    private array $topicPartitionBatchSize = [];

    /**
     * Buffer for storing topic-partition-messages
     *
     * @var array<string, array<int, list<Record>>>
     */
    private array $topicPartitionMessages = [];

    /**
     * Deferred task to send messages for each topic-partition
     *
     * We don't need this per each record because entire record batch is commited to the partition
     *
     * @var array<string, array<int, Deferred>>
     */
    private array $deferredTopicPartitionSend = [];

    /**
     * Point in time at which the oldest record of the current batch was buffered, for the `linger.ms` option
     */
    private ?float $batchStartTime = null;

    /**
     * @param array<string, mixed> $configuration Producer options, {@see ProducerConfig}
     *
     * @throws InvalidConfigurationException for an unusable partitioner or compression type
     */
    public function __construct(array $configuration = [])
    {
        $this->configuration = ($configuration + ProducerConfig::getDefaultConfiguration());
        $partitioner         = $this->configuration[ProducerConfig::PARTITIONER_CLASS];

        if (!is_subclass_of($partitioner, PartitionerInterface::class)) {
            throw new \InvalidArgumentException('Partitioner class should implement PartitionInterface');
        }
        $this->partitioner = new $partitioner();

        // Fail fast on a codec that this client can not write, instead of on the first flush of a batch
        ProducerConfig::compressionCodec($this->configuration[ProducerConfig::COMPRESSION_TYPE]);
        // ... and on a message format that it can not write either
        ProducerConfig::messageFormatMagic($this->configuration[ProducerConfig::MESSAGE_FORMAT_VERSION]);
    }

    /**
     * Sends a message to the topic
     *
     * The record is buffered and the promise is settled by the flush that sends its batch, so a caller that needs
     * the {@see RecordMetadata} of a single record has to call {@see KafkaProducer::flush()} to get it.
     *
     * @param string   $topic             Name of the topic
     * @param Record   $message           Message to send
     * @param int|null $concretePartition Optional partition for sending message
     *
     * @return Promise Resolves with a {@see RecordMetadata} once the broker acknowledges the record
     *
     * @throws MessageTooLargeException for a record that is bigger than the `max.request.size` option
     */
    public function send(string $topic, Record $message, ?int $concretePartition = null): Promise
    {
        // The metadata of the topic are needed either way: to place the record, and to find the leader to send it to
        $cluster = $this->getCluster($topic);

        if (isset($concretePartition)) {
            $partition = $concretePartition;
        } else {
            $partition = $this->partitioner->partition($topic, $message->key, $message->value, $cluster);
        }

        // Every record of a message format v1 batch carries a timestamp; the one the producer stamps is the
        // CreateTime of the record, and a record that already carries one keeps it, as in the Java producer
        if ($message->timestamp === null) {
            $message = $message->withCreateTime(self::currentTimestampMs());
        }

        $recordSize     = $this->recordSize($message);
        $maxRequestSize = (int) $this->configuration[ProducerConfig::MAX_REQUEST_SIZE];
        if ($recordSize > $maxRequestSize) {
            throw new MessageTooLargeException([
                'topic'            => $topic,
                'partition'        => $partition,
                'recordSize'       => $recordSize,
                'max.request.size' => $maxRequestSize,
                'error'            => 'The record is larger than the maximum size of a request',
            ]);
        }
        // The buffered batch becomes one request, so it is sent before it could grow past the allowed size
        if ($this->batchSize > 0 && ($this->batchSize + $recordSize) > $maxRequestSize) {
            $this->flush();
        }

        $bufferedPartitionSize = $this->topicPartitionBatchSize[$topic][$partition] ?? 0;

        $this->topicPartitionMessages[$topic][$partition][] = $message;
        $this->topicPartitionBatchSize[$topic][$partition]  = $bufferedPartitionSize + $recordSize;
        $this->batchSize                                  += $recordSize;
        $this->batchStartTime                            ??= microtime(true);

        $this->deferredTopicPartitionSend[$topic][$partition] ??= new Deferred();

        $promise = $this->deferredTopicPartitionSend[$topic][$partition]->promise();
        assert($promise instanceof Promise);

        if ($this->isBatchReady()) {
            $this->flush();
        }

        return $promise;
    }

    /**
     * Invoking this method makes all buffered records immediately available to send and blocks on the completion of
     * the requests associated with these records.
     *
     * Every topic-partition of the batch is settled by the time this method returns: resolved with the metadata that
     * the broker answered with, or rejected with the error of that partition.
     *
     * The retries of a failed partition happen one layer below, inside {@see Client::produce()}, which refreshes the
     * cluster metadata and sends the partitions that failed with a retriable error again, `retries` times with
     * `retry.backoff.ms` in between. The producer adds no second layer on top of it: `retries` is the whole budget
     * of a batch, and its default of 0 means that a batch is sent exactly once, as with the Java producer.
     */
    public function flush(): void
    {
        if ($this->topicPartitionMessages === []) {
            return;
        }

        [$produceResult, $produceExceptions] = $this->produceBufferedBatch();

        $this->resolveAcknowledgedPartitions($produceResult);

        foreach ($produceExceptions as $topic => $partitionExceptions) {
            foreach ($partitionExceptions as $partitionId => $partitionException) {
                $this->rejectPartition($topic, $partitionId, $partitionException);
            }
        }

        // A partition that was neither acknowledged nor reported as failed would keep its promise pending forever
        foreach ($this->topicPartitionMessages as $topic => $partitions) {
            foreach (array_keys($partitions) as $partitionId) {
                $this->rejectPartition($topic, $partitionId, new \RuntimeException(
                    "The broker did not report anything about {$topic}-{$partitionId}"
                ));
            }
        }

        $this->topicPartitionMessages     = [];
        $this->topicPartitionBatchSize    = [];
        $this->deferredTopicPartitionSend = [];
        $this->batchSize                  = 0;
        $this->batchStartTime             = null;
    }

    /**
     * Gets the partition metadata for the given topic.
     *
     * @return PartitionMetadata[]
     */
    public function partitionsFor(string $topic): array
    {
        return $this->getCluster($topic)->partitionsForTopic($topic);
    }

    /**
     * Automatic flushing of all waiting messages, to use async flush, just call fastcgi_finish_request() before
     */
    public function __destruct()
    {
        if ($this->topicPartitionMessages !== []) {
            $this->flush();
        }
    }

    /**
     * Returns the number of bytes that a record takes in a produce request, its entry of the record set.
     *
     * The size of the *uncompressed* record is the one that `batch.size` and `max.request.size` are measured in,
     * because the compression ratio of a batch is only known once the batch is complete. What one record costs
     * depends on the message format: v1 adds the eight bytes of a timestamp to the fixed overhead of v0, while a
     * record of the message format v2 stores its numbers as varints and its headers next to the key and the value,
     * so its size is the one of the {@see RecordV2} it becomes. The header of the batch around it is not counted
     * here, exactly as `AbstractRecords.estimateSizeInBytesUpperBound()` of the Java producer does not count it.
     */
    private function recordSize(Record $message): int
    {
        $magic = ProducerConfig::messageFormatMagic($this->configuration[ProducerConfig::MESSAGE_FORMAT_VERSION]);
        if ($magic >= RecordBatch::MAGIC) {
            return new RecordV2($message->value, $message->key, array_values($message->headers))->sizeInBytes();
        }

        return MessageSet::ENTRY_OVERHEAD
            + Message::ofMagic($magic, $message->value, $message->key)->sizeInBytes();
    }

    /**
     * Returns the current time in milliseconds since the epoch, the clock of a `CreateTime` timestamp
     */
    private static function currentTimestampMs(): int
    {
        return (int) round(microtime(true) * 1000);
    }

    /**
     * Tells whether the buffered batch has to be sent to the broker now.
     *
     * A batch is ready when it holds `batch.size` bytes; with `linger.ms` configured it is also held back until that
     * many milliseconds passed since its first record, which gives the following records the time to join it.
     */
    private function isBatchReady(): bool
    {
        $batchSizeBytes = (int) $this->configuration[ProducerConfig::BATCH_SIZE];
        $lingerMs       = (int) $this->configuration[ProducerConfig::LINGER_MS];

        if ($lingerMs <= 0) {
            // No lingering at all: a size of 0 disables the batching and sends every record on its own
            return $this->batchSize >= $batchSizeBytes;
        }

        if ($batchSizeBytes > 0 && $this->batchSize >= $batchSizeBytes) {
            return true;
        }

        return (microtime(true) - ($this->batchStartTime ?? microtime(true))) * 1000 >= $lingerMs;
    }

    /**
     * Sends the buffered batch to the partition leaders and splits the answer into results and per-partition errors.
     *
     * @return array{0: array<string, array<int, ProduceResponsePartition>>, 1: array<string, array<int, \Throwable>>}
     */
    private function produceBufferedBatch(): array
    {
        try {
            $produceResult = $this->getClient()->produce($this->topicPartitionMessages);

            // A fire-and-forget request is never answered, so the records count as sent as soon as they are written
            if ((int) $this->configuration[ProducerConfig::ACKS] === ProducerConfig::ACKS_NONE) {
                $produceResult = $this->fireAndForgetResult();
            }

            return [$produceResult, []];
        } catch (TopicPartitionRequestException $exception) {
            // Part of the topic-partitions of the request succeeded, the others carry an error each
            return [$exception->getPartialResult(), $exception->getExceptions()];
        } catch (KafkaException $exception) {
            // A client that reports the first error of a request instead of all of them fails the whole batch
            return [[], $this->spreadOverBufferedPartitions($exception)];
        }
    }

    /**
     * Builds the result of a request that the broker does not answer at all, `acks = 0`.
     *
     * The offset of a record that was never acknowledged is unknown, and the official clients report it as -1, and
     * so is the throttle time: a broker that is not allowed to answer can not report the delay it applied either.
     *
     * @return array<string, array<int, ProduceResponsePartition>>
     */
    private function fireAndForgetResult(): array
    {
        $result = [];
        foreach ($this->topicPartitionMessages as $topic => $partitions) {
            foreach (array_keys($partitions) as $partitionId) {
                $partitionResult             = new ProduceResponsePartition();
                $partitionResult->partition  = $partitionId;
                $partitionResult->errorCode  = 0;
                $partitionResult->baseOffset = -1;

                $result[$topic][$partitionId] = $partitionResult;
            }
        }

        return $result;
    }

    /**
     * Attributes an error of the whole request to every topic-partition that was part of it
     *
     * @return array<string, array<int, \Throwable>>
     */
    private function spreadOverBufferedPartitions(\Throwable $exception): array
    {
        $exceptions = [];
        foreach ($this->topicPartitionMessages as $topic => $partitions) {
            foreach (array_keys($partitions) as $partitionId) {
                $exceptions[$topic][$partitionId] = $exception;
            }
        }

        return $exceptions;
    }

    /**
     * Resolves the promises of the topic-partitions that the broker acknowledged and drops them from the batch
     *
     * @param array<string, array<int, ProduceResponsePartition>> $produceResult
     */
    private function resolveAcknowledgedPartitions(array $produceResult): void
    {
        foreach ($produceResult as $topic => $partitions) {
            foreach ($partitions as $partitionId => $partitionResult) {
                if (!isset($this->topicPartitionMessages[$topic][$partitionId])) {
                    continue;
                }
                // The `timestamp` is the CreateTime the producer stamped on the first record of the batch, which
                // is the record the answer reports the offset of - unless the broker stamped the batch itself,
                // which version 2 of the Produce API reports as the `LogAppendTime` of the partition; that value
                // is the one the log holds, so it wins, exactly as `RecordMetadata` of the Java producer does
                $createTime = $this->createTimeOf($topic, $partitionId);
                $timestamp  = $partitionResult->logAppendTime !== ProduceResponsePartition::NO_LOG_APPEND_TIME
                    ? $partitionResult->logAppendTime
                    : $createTime;
                $deferred = $this->forgetPartition($topic, $partitionId);

                $deferred?->resolve(new RecordMetadata(
                    $topic,
                    $partitionId,
                    $partitionResult->baseOffset,
                    $timestamp,
                    $partitionResult->throttleTimeMs
                ));
            }
        }
    }

    /**
     * Returns the CreateTime that the producer stamped on the first record of a buffered topic-partition
     */
    private function createTimeOf(string $topic, int $partitionId): ?int
    {
        $records = $this->topicPartitionMessages[$topic][$partitionId] ?? [];

        return $records === [] ? null : $records[0]->timestamp;
    }

    /**
     * Rejects the promise of a topic-partition and drops its records from the batch
     */
    private function rejectPartition(string $topic, int $partitionId, \Throwable $exception): void
    {
        $deferred = $this->forgetPartition($topic, $partitionId);

        $deferred?->reject($exception);
    }

    /**
     * Removes a topic-partition from the batch and returns the deferred that its promise was made of
     */
    private function forgetPartition(string $topic, int $partitionId): ?Deferred
    {
        $deferred  = $this->deferredTopicPartitionSend[$topic][$partitionId] ?? null;
        $this->batchSize -= $this->topicPartitionBatchSize[$topic][$partitionId] ?? 0;

        unset(
            $this->topicPartitionMessages[$topic][$partitionId],
            $this->topicPartitionBatchSize[$topic][$partitionId],
            $this->deferredTopicPartitionSend[$topic][$partitionId]
        );
        if (($this->topicPartitionMessages[$topic] ?? []) === []) {
            unset(
                $this->topicPartitionMessages[$topic],
                $this->topicPartitionBatchSize[$topic],
                $this->deferredTopicPartitionSend[$topic]
            );
        }

        return $deferred;
    }

    /**
     * Cluster lazy-loading
     *
     * The topic of the first record is passed on to the metadata request that bootstraps the cluster: a broker with
     * `auto.create.topics.enable` creates a topic when a client asks for the metadata of that topic, and a cluster
     * that hosts no topic at all only starts advertising its brokers once it holds one.
     *
     * @param string|null $topic Topic the producer is about to write to, if it knows it already
     */
    private function getCluster(?string $topic = null): Cluster
    {
        if ($this->cluster === null) {
            $this->cluster = Cluster::bootstrap($this->configuration, $topic);
        }

        return $this->cluster;
    }

    /**
     * Lazy-loading for kafka client
     */
    private function getClient(): Client
    {
        if ($this->client === null) {
            $this->client = $this->createClient($this->getCluster(), $this->configuration);
        }

        return $this->client;
    }

    /**
     * Creates the low-level client that the batches of this producer are sent with
     *
     * @param array<string, mixed> $configuration
     */
    protected function createClient(Cluster $cluster, array $configuration): Client
    {
        return new Client($cluster, $configuration);
    }
}
