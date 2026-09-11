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
use Protocol\Kafka\Consumer\ConsumerGroupMetadata;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Producer\Internals\TransactionManager;
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
 * With **`enable.idempotence = true`** (KIP-98, Kafka 0.11) the producer stops writing duplicates when it retries:
 * it asks a broker for a producer id with its first flush, numbers the batch of every topic-partition with the
 * next sequence number of that partition and re-sends a batch that failed with those very numbers, so the broker
 * recognises a batch it has already appended and answers it with the offset of the original append. The guarantee
 * needs `acks = all` and a non-zero `retries`, which {@see ProducerConfig::resolveIdempotence()} sets when the
 * caller left them alone and refuses when the caller set them to something else, and it holds for one producer
 * session: a new {@see KafkaProducer} gets a new producer id and can not deduplicate against what the old one
 * wrote. Application-level re-sends can not be deduplicated either - only a retry of the very same batch can.
 *
 * A **1.x broker** widens that in two ways this producer inherits without an option of its own: it recognises a
 * duplicate of any of the **last five** batches of a producer and partition rather than only of the very last one,
 * so a producer whose acknowledgement of several batches in a row was lost is answered as the original append
 * instead of being thrown out of sequence; and it says so with an error of its own, **59** `UnknownProducerId`,
 * when it lost the state of a producer because the records it held it by were deleted. That one is answered inside
 * {@see \Protocol\Kafka\Client::produce()}: the partition is numbered from the sequence 0 again and the batch is
 * sent once more, which is why a producer that writes into a partition with a short retention keeps working
 * instead of failing with an out-of-order sequence.
 *
 * <code>
 *   $producer = new KafkaProducer([
 *       ProducerConfig::BOOTSTRAP_SERVERS   => ['tcp://127.0.0.1:9092'],
 *       ProducerConfig::ENABLE_IDEMPOTENCE  => true,   // implies acks = all and retries = 3
 *   ]);
 * </code>
 *
 * With a **`transactional.id`** (KIP-98 as well) the producer goes one step further: the records of several
 * partitions - and the offsets of a consumer group - become one **transaction** that a `read_committed` consumer
 * either sees whole or does not see at all, and the guarantee survives a restart, because the id is what the
 * broker remembers the producer by. The five methods of the Java producer are here under the same names:
 * {@see KafkaProducer::initTransactions()} once at the start, then {@see KafkaProducer::beginTransaction()},
 * any number of {@see KafkaProducer::send()} calls, optionally
 * {@see KafkaProducer::sendOffsetsToTransaction()}, and finally {@see KafkaProducer::commitTransaction()} or
 * {@see KafkaProducer::abortTransaction()}.
 *
 * <code>
 *   $producer = new KafkaProducer([
 *       ProducerConfig::BOOTSTRAP_SERVERS => ['tcp://127.0.0.1:9092'],
 *       ProducerConfig::TRANSACTIONAL_ID  => 'orders-etl-1',   // implies enable.idempotence
 *   ]);
 *   $producer->initTransactions();
 *
 *   $producer->beginTransaction();
 *   try {
 *       $producer->send('orders', Record::fromValue('one'));
 *       $producer->send('audit',  Record::fromValue('one accepted'));
 *       $producer->commitTransaction();
 *   } catch (KafkaException $error) {
 *       $producer->abortTransaction();
 *   }
 * </code>
 *
 * A `send()` outside a transaction is refused, and so is one after an error that only an abort can clean up. A
 * transactional id must be used by **one producer at a time**: `initTransactions()` bumps the epoch of the id,
 * which fences every producer that still holds the old one.
 *
 * A broker with a `producer_byte_rate` quota for the `client.id` of this producer does not reject anything: it
 * appends the batch and holds its answer back until the client is inside its quota again. That delay is what
 * {@see RecordMetadata::$throttleTimeMs} reports, and {@see KafkaProducer::flush()} simply takes that much longer.
 *
 * @see examples/producer.php for a runnable example
 * @see examples/transactional-producer.php for the consume-transform-produce loop
 * @see docs/protocol/2.8.md, sections "Quotas and throttle time" and "Transactions"
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
     * Producer state of KIP-98, `null` while `enable.idempotence` is off or nothing has been flushed yet
     */
    private ?TransactionManager $transactionManager = null;

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
        // `enable.idempotence` overrides `acks` and `retries` when the caller left them alone and refuses a value
        // the guarantee can not live with, so it has to see the options before the defaults are merged into them
        $this->configuration = (ProducerConfig::resolveIdempotence($configuration)
            + ProducerConfig::getDefaultConfiguration());
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
        // A transactional producer writes inside a transaction and nowhere else, and a producer that hit an error
        // may not write anything until the transaction was rolled back - both are refused before the record is
        // even placed, exactly as `KafkaProducer.doSend()` @ 0.11.0.3 does
        $this->getTransactionManager()?->failIfNotReadyForSend();

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
     * Initializes the transactional producer, which every other transactional call needs first.
     *
     * One `InitProducerId` request goes to the **transaction coordinator** of the `transactional.id`, and the
     * answer is the producer id that `__transaction_state` holds for that id together with an epoch **one higher**
     * than the previous incarnation of the id used. That bump is the whole point of a transactional id: it fences
     * the previous incarnation for good - every batch and every transactional request of it is refused with the
     * error code 47 from then on - and it makes the coordinator **abort a transaction that incarnation had left
     * open**, so that a crashed producer can not block a `read_committed` consumer for ever.
     *
     * Call it exactly once, before the first {@see self::send()}; a second call is a state error.
     *
     * @throws InvalidConfigurationException For a producer without a `transactional.id`
     * @throws KafkaException                For an error the coordinator reports
     */
    public function initTransactions(): void
    {
        $this->requireTransactionManager()->initTransactions();
    }

    /**
     * Opens a transaction.
     *
     * There is no request behind this call - the protocol has no "BeginTransaction" frame - so nothing is sent
     * until the first {@see self::flush()} of the transaction, which enrols the partitions of its batch with an
     * `AddPartitionsToTxn` request; that request is what starts the `transaction.timeout.ms` of the coordinator
     * running.
     *
     * @throws InvalidConfigurationException For a producer without a `transactional.id`
     * @throws \LogicException               For a producer that was not initialized, or that is inside a
     *         transaction already, or that hit an error which has to be aborted first
     */
    public function beginTransaction(): void
    {
        $this->requireTransactionManager()->beginTransaction();
    }

    /**
     * Commits the offsets of a consumer group as part of the open transaction.
     *
     * This is what makes a *consume-transform-produce* loop atomic: instead of committing what it read, the
     * consumer hands its offsets to the producer, which writes them into `__consumer_offsets` inside the very
     * transaction that holds the records it derived from them. Either both become visible or neither does, so a
     * record can not be produced twice by a consumer that crashed between the produce and its offset commit.
     *
     * The consumer of that group must run with `enable.auto.commit = false` and read
     * `isolation.level = read_committed`, otherwise it would commit or read what the transaction may still abort.
     *
     * ```php
     * $producer->beginTransaction();
     * foreach ($consumer->poll(1000) as $topic => $partitions) { ... $producer->send(...); }
     * $producer->flush();
     * $producer->sendOffsetsToTransaction(['input' => [0 => $nextOffset]], $consumer->groupMetadata());
     * $producer->commitTransaction();
     * ```
     *
     * **KIP-447, Kafka 2.5**: the second argument is the {@see ConsumerGroupMetadata} of the consumer - its group,
     * its generation, its member id and its `group.instance.id`, which
     * {@see \Protocol\Kafka\Consumer\KafkaConsumer::groupMetadata()} answers - and the group coordinator refuses
     * the commit of a consumer that has been rebalanced away (22 `IllegalGeneration`, 25 `UnknownMemberId`, 82
     * `FencedInstanceId`) instead of letting it write offsets for partitions another member owns by now. A bare
     * group id still works, as in the 1.x line, and means {@see ConsumerGroupMetadata::forGroup()}: the generation
     * -1 with the empty member id, the "not a member" commit every version below 3 of the api sent.
     *
     * @param array<string, array<int, int|OffsetAndMetadata>> $topicPartitionOffsets Offset the group continues at,
     *        as topic => partition => offset; the offset of the **next** record to read, as everywhere in Kafka
     * @param string|ConsumerGroupMetadata $consumerGroupMetadata Consumer whose offsets are committed, or the bare
     *        id of its group
     *
     * @throws InvalidConfigurationException For a producer without a `transactional.id`
     * @throws \LogicException               For a producer that has no open transaction
     * @throws KafkaException                For an error a coordinator reports
     */
    public function sendOffsetsToTransaction(
        array $topicPartitionOffsets,
        string|ConsumerGroupMetadata $consumerGroupMetadata
    ): void {
        $this->requireTransactionManager()
            ->sendOffsetsToTransaction($topicPartitionOffsets, $consumerGroupMetadata);
    }

    /**
     * Flushes everything that is buffered and commits the open transaction.
     *
     * The buffered records are sent first - a record that is still in the buffer when the transaction ends is not
     * part of it - and then one `EndTxn` request tells the coordinator to commit. The coordinator answers as soon
     * as it has written that decision; the control batches that make the records visible to a `read_committed`
     * consumer are written by the coordinator afterwards, so such a consumer sees them a moment after this call
     * returns.
     *
     * @throws InvalidConfigurationException For a producer without a `transactional.id`
     * @throws \LogicException               For a producer that has no open transaction
     * @throws KafkaException                For an error the coordinator reports
     */
    public function commitTransaction(): void
    {
        $manager = $this->requireTransactionManager();

        $this->flush();

        $manager->commitTransaction();
    }

    /**
     * Rolls the open transaction back.
     *
     * Everything that is still buffered is **dropped** instead of being sent - its promises are rejected - and one
     * `EndTxn` request tells the coordinator to abort. The records that were already written stay in the log;
     * they are marked with an ABORT control batch, a `read_committed` consumer skips them, and a
     * `read_uncommitted` one has been seeing them all along.
     *
     * This is the only way out of an abortable error, so a `catch` around a transaction ends here:
     *
     * ```php
     * try {
     *     $producer->beginTransaction();
     *     // … send() …
     *     $producer->commitTransaction();
     * } catch (KafkaException $error) {
     *     $producer->abortTransaction();
     * }
     * ```
     *
     * @throws InvalidConfigurationException For a producer without a `transactional.id`
     * @throws \LogicException               For a producer that has no open transaction
     * @throws KafkaException                For an error the coordinator reports
     */
    public function abortTransaction(): void
    {
        $manager = $this->requireTransactionManager();

        $this->discardBufferedBatch();

        $manager->abortTransaction();
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
     * Drops every buffered record without sending it, rejecting the promise of each of its topic-partitions.
     *
     * This is what an abort does with the records that never made it out of the producer: they were meant for a
     * transaction that is being rolled back, so sending them would be pointless work whose result is thrown away
     * by the ABORT marker anyway.
     */
    private function discardBufferedBatch(): void
    {
        foreach ($this->topicPartitionMessages as $topic => $partitions) {
            foreach (array_keys($partitions) as $partitionId) {
                $this->rejectPartition($topic, $partitionId, new \RuntimeException(
                    "The transaction was aborted before the batch of {$topic}-{$partitionId} was sent"
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
            // Every partition of a transaction has to be known to the coordinator before the first Produce request
            // that touches it, otherwise it receives no commit marker and its records stay uncommitted for ever
            $manager = $this->getTransactionManager();
            if ($manager !== null && $manager->isTransactional()) {
                $manager->maybeAddPartitionsToTransaction($this->topicPartitionMessages);
            }

            $produceResult = $this->getClient()->produce(
                $this->topicPartitionMessages,
                $this->getTransactionManager()
            );

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
     * Returns the producer state of KIP-98, or `null` for a producer that is not idempotent.
     *
     * The manager is created with the first batch that is flushed, and it asks for the producer id itself when the
     * first request is built, so a producer that never sends anything never talks to a broker either.
     */
    private function getTransactionManager(): ?TransactionManager
    {
        if (!ProducerConfig::isIdempotenceEnabled($this->configuration[ProducerConfig::ENABLE_IDEMPOTENCE] ?? false)) {
            return null;
        }

        $transactionalId = $this->configuration[ProducerConfig::TRANSACTIONAL_ID] ?? null;

        return $this->transactionManager ??= new TransactionManager(
            $this->getClient(),
            $transactionalId === null ? null : (string) $transactionalId,
            (int) $this->configuration[ProducerConfig::TRANSACTION_TIMEOUT_MS],
            $this->configuration
        );
    }

    /**
     * Returns the producer state of a **transactional** producer, refusing the call on any other one
     *
     * @throws InvalidConfigurationException For a producer without a `transactional.id`
     */
    private function requireTransactionManager(): TransactionManager
    {
        $manager = $this->getTransactionManager();
        if ($manager === null || !$manager->isTransactional()) {
            throw new InvalidConfigurationException(
                'The transactional API of the producer needs a ' . ProducerConfig::TRANSACTIONAL_ID
                . ' in its configuration'
            );
        }

        return $manager;
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
