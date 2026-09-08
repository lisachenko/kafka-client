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

namespace Protocol\Kafka\Consumer;

use BadMethodCallException;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\RecordTooLargeException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Serialization\Deserializer;
use Protocol\Kafka\Consumer\Internals\SubscriptionState;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * A Kafka client that consumes records from a Kafka 0.8.2.2 cluster.
 *
 * Kafka 0.8 has no broker-side group membership: the coordinator of a group only stores its committed offsets
 * (GroupCoordinator, OffsetCommit, OffsetFetch), while the partition assignment and the rebalancing of the 0.8
 * high-level consumer were done by the consumers themselves through ZooKeeper, which this client does not speak.
 * This consumer is therefore the equivalent of the Java `SimpleConsumer` with broker-stored offsets, behind the
 * API of the later protocol lines: partitions are selected with {@see assign()}, and {@see subscribe()} throws.
 *
 * Usage against a broker on 127.0.0.1:9092, see also examples/consumer.php:
 *
 * ```php
 * $consumer = new KafkaConsumer([
 *     ConsumerConfig::BOOTSTRAP_SERVERS  => ['tcp://127.0.0.1:9092'],
 *     ConsumerConfig::GROUP_ID           => 'my-group',
 *     ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
 *     ConsumerConfig::ENABLE_AUTO_COMMIT => false,
 * ]);
 *
 * // Partitions are assigned explicitly; assignment() reports them back
 * $consumer->assign(['my-topic' => [0, 1]]);
 *
 * while (true) {
 *     // [topic][partition] => list of records, in offset order
 *     foreach ($consumer->poll(500) as $topic => $partitions) {
 *         foreach ($partitions as $partition => $records) {
 *             foreach ($records as $record) {
 *                 echo $topic, ':', $partition, '@', $record->offset, ' ', $record->value, PHP_EOL;
 *             }
 *         }
 *     }
 *     $consumer->commitSync();
 * }
 * ```
 *
 * Where the committed offsets are kept is chosen with `offsets.storage`: `kafka` commits them to the coordinator
 * of the group with the version 1 of the offset APIs, `zookeeper` uses the version 0, which is what the consumers
 * of Kafka 0.8.1 did. The two storages are independent, so a group has one position per storage.
 *
 * The options that only drive the group membership of Kafka 0.9 (session.timeout.ms, heartbeat.interval.ms,
 * rebalance.timeout.ms, partition.assignment.strategy) do not exist on this branch, and neither does the
 * `offset.retention.ms` of the OffsetCommit v2 or the `isolation.level` of the transactional protocol of 0.11.
 *
 * A message that does not fit into `max.partition.fetch.bytes` is refused with a {@see RecordTooLargeException}
 * rather than silently stalling the partition, because a 0.8.2.2 broker cuts a message set off at that size
 * without guaranteeing that a single message fits into it.
 */
class KafkaConsumer
{
    /**
     * The consumer configs
     *
     * @var array<string, mixed>
     */
    private array $configuration;

    /**
     * Kafka cluster configuration, resolved on the first use
     */
    private ?Cluster $cluster = null;

    /**
     * Low-level kafka client, created on the first use
     */
    private ?Client $client = null;

    /**
     * Assignment and positions of this consumer
     */
    private readonly SubscriptionState $subscriptionState;

    /**
     * Offset coordinator node of the configured consumer group
     */
    private ?Node $coordinator = null;

    /**
     * Last commit time in ms
     */
    private ?int $lastAutoCommitMs = null;

    /**
     * Deserializer for the record keys, or null to keep them as raw byte strings
     */
    private readonly ?Deserializer $keyDeserializer;

    /**
     * Deserializer for the record values, or null to keep them as raw byte strings
     */
    private readonly ?Deserializer $valueDeserializer;

    /**
     * @param array<string, mixed> $configuration Consumer options, see {@see ConsumerConfig}
     */
    public function __construct(array $configuration = [])
    {
        $this->configuration     = $configuration + ConsumerConfig::getDefaultConfiguration();
        $this->subscriptionState = new SubscriptionState();

        if ($this->isAutoCommitEnabled() && $this->groupId() === '') {
            throw new InvalidConfigurationException(
                'Committed offsets are stored per consumer group, so ' . ConsumerConfig::GROUP_ID . ' is required '
                . 'when ' . ConsumerConfig::ENABLE_AUTO_COMMIT . ' is on. Configure a group or switch the automatic '
                . 'commit off to consume without one.'
            );
        }

        $this->keyDeserializer   = self::resolveDeserializer($this->configuration[ConsumerConfig::KEY_DESERIALIZER]);
        $this->valueDeserializer = self::resolveDeserializer($this->configuration[ConsumerConfig::VALUE_DESERIALIZER]);
    }

    /**
     * Manually assign a list of partitions to this consumer.
     *
     * This interface does not allow for incremental assignment and replaces the previous assignment, if there is
     * one; an empty list is treated the same as {@see unsubscribe()}. The position of a partition that stays
     * assigned is kept, the position of a newly assigned one is read from the committed offsets of the group and,
     * when the group has none, follows `auto.offset.reset`.
     *
     * If auto-commit is enabled, the positions of the previous assignment are committed before it is replaced.
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Topic name => partitions of that topic,
     *                                                                     either as a plain list or as the DTO of
     *                                                                     the protocol layer
     */
    public function assign(array $topicPartitions): void
    {
        if ($topicPartitions === []) {
            $this->unsubscribe();

            return;
        }

        if ($this->isAutoCommitEnabled()) {
            $this->commitSync($this->subscriptionState->allConsumed());
        }

        $assignment = self::normalizeAssignment($topicPartitions);
        $this->subscriptionState->assignFromUser($assignment);
        $this->refreshTopicPartitionOffsets($assignment);
    }

    /**
     * Get the set of topic partitions currently assigned to this consumer.
     *
     * @return array<string, array<int, int>> [topic: string][partition: int] => partition
     */
    public function assignment(): array
    {
        $result = [];
        foreach ($this->subscriptionState->getAssignment() as $topic => $partitions) {
            foreach (array_keys($partitions) as $partitionId) {
                $result[$topic][$partitionId] = $partitionId;
            }
        }

        return $result;
    }

    /**
     * Commit offsets for the assigned list of topics and partitions.
     *
     * Without an argument the current positions of the consumer are committed, which are the offsets of the
     * records that the next poll() would return - the record of the offset that was committed is *not* consumed
     * again by a consumer that resumes from it.
     *
     * @param array<string, array<int, int|OffsetAndMetadata>>|null $topicPartitionOffsets Offsets to commit, or
     *                                                                                     null for the positions
     *                                                                                     of this consumer
     */
    public function commitSync(?array $topicPartitionOffsets = null): void
    {
        $topicPartitionOffsets ??= $this->subscriptionState->allConsumed();

        if ($topicPartitionOffsets === []) {
            return;
        }

        $this->getClient()->commitGroupOffsets(
            $this->getCoordinator(),
            $this->requireGroupId(),
            $topicPartitionOffsets
        );
    }

    /**
     * Get the last committed offset of every given topic-partition, whether this consumer committed it or not.
     *
     * A topic-partition that the group has never committed comes back with the offset -1, whichever storage the
     * `offsets.storage` option selects.
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Topic name => partitions of that topic
     *
     * @return array<string, array<int, int>> [topic: string][partition: int] => committed offset, or -1
     */
    public function committed(array $topicPartitions): array
    {
        if ($topicPartitions === []) {
            return [];
        }

        return $this->getClient()->fetchGroupOffsets(
            $this->getCoordinator(),
            $this->requireGroupId(),
            self::normalizeAssignment($topicPartitions)
        );
    }

    /**
     * Gets the partition metadata for the given topic.
     *
     * @return PartitionMetadata[]
     */
    public function partitionsFor(string $topic): array
    {
        return $this->getCluster()->partitionsForTopic($topic);
    }

    /**
     * Suspend fetching from the requested partitions.
     *
     * A paused partition keeps its position and its committed offsets, poll() simply stops returning records for
     * it until {@see resume()} is called.
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Topic name => partitions to suspend
     */
    public function pause(array $topicPartitions): void
    {
        $this->subscriptionState->pause(self::normalizePartitionLists($topicPartitions));
    }

    /**
     * Fetches data for the partitions specified with the assign() API.
     *
     * Every assigned partition that is not paused is fetched from its current position; the returned records are
     * the ones the broker had, in offset order, and the position of each partition moves behind its last record,
     * so that the next poll() continues where this one stopped. When `enable.auto.commit` is on, the positions
     * are committed once `auto.commit.interval.ms` has passed since the last commit.
     *
     * @param int $timeout The time, in milliseconds, spent waiting in poll if data is not available. If 0, returns
     *                     immediately with any records that are available now. The broker never waits longer than
     *                     `fetch.max.wait.ms` and answers as soon as `fetch.min.bytes` are available.
     *
     * @return array<string, array<int, list<Record>>> [topic: string][partition: int] => list of received records
     *
     * @throws RecordTooLargeException when the next message of a partition does not fit into
     *                                 `max.partition.fetch.bytes` and the consumer can not make progress
     * @throws OffsetOutOfRangeException when a position is outside of the log and `auto.offset.reset` is `none`
     */
    public function poll(int $timeout): array
    {
        $milliSeconds = (int) (microtime(true) * 1e3);

        $activeTopicPartitionOffsets = $this->subscriptionState->fetchablePartitions();
        if ($activeTopicPartitionOffsets === []) {
            return [];
        }

        $fetchedPartitions = $this->fetchMessages($activeTopicPartitionOffsets, $timeout);
        $result            = $this->collectRecords($fetchedPartitions);

        $this->updateFetchPositions($fetchedPartitions);

        if ($this->isAutoCommitEnabled()) {
            $elapsedInterval = $milliSeconds - (int) $this->lastAutoCommitMs;
            if ($elapsedInterval >= $this->configuration[ConsumerConfig::AUTO_COMMIT_INTERVAL_MS]) {
                $this->commitSync();
                $this->lastAutoCommitMs = $milliSeconds;
            }
        }

        return $this->deserializeRecords($result);
    }

    /**
     * Get the offset of the next record that will be fetched (if a record with that offset exists).
     */
    public function position(string $topic, int $partition): int
    {
        return $this->subscriptionState->position($topic, $partition);
    }

    /**
     * Resume the specified partitions which have been paused with pause($topicPartitions).
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Topic name => partitions to resume
     */
    public function resume(array $topicPartitions): void
    {
        $this->subscriptionState->resume(self::normalizePartitionLists($topicPartitions));
    }

    /**
     * Overrides the fetch offset that the consumer will use on the next poll(timeout).
     */
    public function seek(string $topic, int $partition, int $offset): void
    {
        $this->subscriptionState->seek($topic, $partition, $offset);
    }

    /**
     * Seek to the first available offset of each of the given partitions.
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Topic name => partitions to rewind
     */
    public function seekToBeginning(array $topicPartitions): void
    {
        $this->fetchOffsetAndSeek(self::normalizePartitionLists($topicPartitions), OffsetsRequest::EARLIEST);
    }

    /**
     * Seek to the end of each of the given partitions, the offset the next produced message will get.
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Topic name => partitions to forward
     */
    public function seekToEnd(array $topicPartitions): void
    {
        $this->fetchOffsetAndSeek(self::normalizePartitionLists($topicPartitions), OffsetsRequest::LATEST);
    }

    /**
     * Subscribing to topics is not available on the Kafka 0.8 protocol line.
     *
     * Dynamic partition assignment requires the broker-side group membership protocol (API keys 11-14), which was
     * introduced in Kafka 0.9; a 0.8.2.2 broker either answers those API keys with "Unknown api code" or closes
     * the connection. Assign the partitions explicitly with {@see assign()} instead.
     *
     * @param string[] $topics List of topics to subscribe to
     */
    public function subscribe(array $topics): void
    {
        throw new BadMethodCallException(
            'Kafka 0.8 has no broker-side group membership, so subscribe() can not be supported: the group '
            . 'membership APIs (keys 11-14) only exist since Kafka 0.9, and a 0.8.2.2 broker either answers them '
            . 'with "Unknown api code" or closes the connection. Use assign() to select the topic partitions for '
            . 'this consumer explicitly.'
        );
    }

    /**
     * Get the current subscription, which is always empty because {@see subscribe()} does not exist in 0.8.
     *
     * @return list<string>
     */
    public function subscription(): array
    {
        return $this->subscriptionState->getSubscription();
    }

    /**
     * Clears the partitions assigned through assign(array $topicPartitions).
     *
     * Nothing is sent to the broker: without group membership there is no group to leave, and the committed
     * offsets of the group stay where they are.
     */
    public function unsubscribe(): void
    {
        $this->subscriptionState->unsubscribe();

        $this->coordinator = null;
    }

    /**
     * Return the coordinator node that keeps the committed offsets of the configured group
     */
    protected function getCoordinator(): Node
    {
        return $this->coordinator ??= $this->getClient()->getGroupCoordinator($this->requireGroupId());
    }

    /**
     * Cluster lazy-loading
     */
    protected function getCluster(): Cluster
    {
        return $this->cluster ??= Cluster::bootstrap($this->configuration);
    }

    /**
     * Lazy-loading for the low-level kafka client
     */
    protected function getClient(): Client
    {
        return $this->client ??= new Client($this->getCluster(), $this->configuration);
    }

    /**
     * Replaces the positions that no committed offset is known for, following `auto.offset.reset`
     *
     * @param array<string, array<int, int>> $topicPartitionOffsets Committed offsets, -1 where there is none
     *
     * @return array<string, array<int, int>> The same offsets, with the unknown ones resolved
     *
     * @throws OffsetOutOfRangeException when `auto.offset.reset` is `none`
     */
    protected function autoResetOffsets(array $topicPartitionOffsets): array
    {
        $unknownTopicPartitions = $this->findUnknownTopicPartitions($topicPartitionOffsets);
        if ($unknownTopicPartitions === []) {
            return $topicPartitionOffsets;
        }

        $fetchedOffsets = match ($this->configuration[ConsumerConfig::AUTO_OFFSET_RESET]) {
            OffsetResetStrategy::LATEST => $this->fetchOffsetAndSeek($unknownTopicPartitions, OffsetsRequest::LATEST),
            OffsetResetStrategy::EARLIEST => $this->fetchOffsetAndSeek($unknownTopicPartitions, OffsetsRequest::EARLIEST),
            default => throw new OffsetOutOfRangeException([
                'unknownTopicPartitions' => $unknownTopicPartitions,
                'reason'                 => 'no committed offset and ' . ConsumerConfig::AUTO_OFFSET_RESET
                    . ' is ' . OffsetResetStrategy::NONE,
            ]),
        };

        return array_replace_recursive($topicPartitionOffsets, $fetchedOffsets);
    }

    /**
     * Asks the broker for the earliest or the latest offsets of the given partitions and seeks them there
     *
     * @param array<string, list<int>> $topicPartitions Topic name => partitions to move
     * @param int                      $requestType     OffsetsRequest::EARLIEST or OffsetsRequest::LATEST
     *
     * @return array<string, array<int, int>> The offsets the partitions were moved to
     */
    protected function fetchOffsetAndSeek(array $topicPartitions, int $requestType): array
    {
        $topicPartitionOffsetsRequest = [];

        $assignment    = $this->assignment();
        $unknownTopics = array_diff_key($topicPartitions, $assignment);
        if ($unknownTopics !== []) {
            throw new UnknownTopicOrPartitionException(['unknownTopics' => array_keys($unknownTopics)]);
        }
        foreach ($topicPartitions as $topic => $partitions) {
            $unknownPartitions = array_diff($partitions, $assignment[$topic]);
            if ($unknownPartitions !== []) {
                throw new UnknownTopicOrPartitionException([
                    'topic'             => $topic,
                    'unknownPartitions' => array_values($unknownPartitions),
                ]);
            }
            $topicPartitionOffsetsRequest[$topic] = array_fill_keys($partitions, $requestType);
        }

        $topicPartitionOffsets = $this->getClient()->fetchTopicPartitionOffsets($topicPartitionOffsetsRequest);
        foreach ($topicPartitionOffsets as $topic => $partitionOffsets) {
            foreach ($partitionOffsets as $partition => $offset) {
                $this->subscriptionState->seek((string) $topic, (int) $partition, (int) $offset);
            }
        }

        return $topicPartitionOffsets;
    }

    /**
     * Look for the topic-partitions that have no committed offset, which the broker reports as the offset -1
     *
     * @param array<string, array<int, int>> $topicPartitionOffsets [topic][partition] => offset
     *
     * @return array<string, list<int>> Topic name => partitions without an offset
     */
    protected function findUnknownTopicPartitions(array $topicPartitionOffsets): array
    {
        $unknownTopicPartitions = [];
        foreach ($topicPartitionOffsets as $topic => $partitionOffsets) {
            $unknownPartitionOffsets = array_keys($partitionOffsets, -1, true);
            if ($unknownPartitionOffsets !== []) {
                $unknownTopicPartitions[$topic] = $unknownPartitionOffsets;
            }
        }

        return $unknownTopicPartitions;
    }

    /**
     * Moves the position of every partition behind the last record that was received for it
     *
     * A partition that returned nothing keeps its position, so that the next poll() asks for the same offset
     * again; the position never moves backwards, whatever a compressed set carried.
     *
     * @param array<string, array<int, FetchedPartition>> $fetchedPartitions Partitions of one poll()
     */
    protected function updateFetchPositions(array $fetchedPartitions): void
    {
        foreach ($fetchedPartitions as $topic => $partitions) {
            foreach ($partitions as $partitionId => $fetchedPartition) {
                $nextOffset = $fetchedPartition->getNextOffset();
                if ($nextOffset > $fetchedPartition->fetchOffset) {
                    $this->subscriptionState->seek((string) $topic, (int) $partitionId, $nextOffset);
                }
            }
        }
    }

    /**
     * Turns the answers of the broker into the records of a poll(), refusing a partition that can not progress
     *
     * A compressed message set is stored and returned as a whole, so a fetch that starts in the middle of one also
     * carries the records before the requested offset; those have already been consumed and are dropped here.
     *
     * @param array<string, array<int, FetchedPartition>> $fetchedPartitions Partitions of one poll()
     *
     * @return array<string, array<int, list<Record>>> [topic: string][partition: int] => list of records
     *
     * @throws RecordTooLargeException
     */
    private function collectRecords(array $fetchedPartitions): array
    {
        $result = [];
        foreach ($fetchedPartitions as $topic => $partitions) {
            foreach ($partitions as $partitionId => $fetchedPartition) {
                // A 0.8.2.2 broker fills the answer up to MaxBytes without guaranteeing that one message fits, so
                // a partition whose next message is bigger would come back empty forever
                if ($fetchedPartition->isSingleMessageTooLarge()) {
                    throw new RecordTooLargeException(
                        (string) $topic,
                        (int) $partitionId,
                        $fetchedPartition->fetchOffset,
                        (int) $this->configuration[ConsumerConfig::MAX_PARTITION_FETCH_BYTES],
                        $fetchedPartition->highWaterMarkOffset
                    );
                }

                $fetchOffset                  = $fetchedPartition->fetchOffset;
                $result[$topic][$partitionId] = array_values(array_filter(
                    $fetchedPartition->getRecords(),
                    static fn(Record $record): bool => $record->offset === null || $record->offset >= $fetchOffset
                ));
            }
        }

        return $result;
    }

    /**
     * Fetches the messages of the given positions, resetting the ones the broker refuses as out of range
     *
     * A position that fell out of the log - because the retention deleted the segment it pointed at, or because
     * the topic was recreated - is answered with the error code 1, OffsetOutOfRange. The consumer resolves that
     * exactly like a missing committed offset: it follows `auto.offset.reset` and fetches again.
     *
     * @param array<string, array<int, int>> $activeTopicPartitionOffsets Positions to fetch from
     * @param int                            $timeout                     Poll timeout in milliseconds
     *
     * @return array<string, array<int, FetchedPartition>> What the broker answered for each partition
     */
    private function fetchMessages(array $activeTopicPartitionOffsets, int $timeout): array
    {
        try {
            return $this->getClient()->fetchPartitions($activeTopicPartitionOffsets, $timeout);
        } catch (OffsetOutOfRangeException $exception) {
            $resetOffsets = $this->resetOutOfRangeOffsets($activeTopicPartitionOffsets, $exception);
        } catch (TopicPartitionRequestException $exception) {
            // A client that reports the failed partitions separately keeps the answers of the healthy ones
            if (!self::hasOffsetOutOfRange($exception)) {
                throw $exception;
            }
            $resetOffsets = $this->resetOutOfRangeOffsets($activeTopicPartitionOffsets, $exception);
        }

        return $this->getClient()->fetchPartitions($resetOffsets, $timeout);
    }

    /**
     * Moves every position that is outside of the log of its partition, following `auto.offset.reset`
     *
     * @param array<string, array<int, int>> $activeTopicPartitionOffsets Positions of the failed fetch
     * @param \Throwable                     $exception                   The failure to re-throw if nothing helps
     *
     * @return array<string, array<int, int>> The positions to fetch from after the reset
     */
    private function resetOutOfRangeOffsets(array $activeTopicPartitionOffsets, \Throwable $exception): array
    {
        $strategy = $this->configuration[ConsumerConfig::AUTO_OFFSET_RESET];
        if ($strategy !== OffsetResetStrategy::EARLIEST && $strategy !== OffsetResetStrategy::LATEST) {
            throw $exception;
        }

        // The Fetch response of 0.8 does not say what the valid range of a partition is, so it has to be asked for
        $earliestRequest = $latestRequest = [];
        foreach ($activeTopicPartitionOffsets as $topic => $partitionOffsets) {
            foreach (array_keys($partitionOffsets) as $partition) {
                $earliestRequest[$topic][$partition] = OffsetsRequest::EARLIEST;
                $latestRequest[$topic][$partition]   = OffsetsRequest::LATEST;
            }
        }
        $earliestOffsets = $this->getClient()->fetchTopicPartitionOffsets($earliestRequest);
        $latestOffsets   = $this->getClient()->fetchTopicPartitionOffsets($latestRequest);

        $wasReset = false;
        foreach ($activeTopicPartitionOffsets as $topic => $partitionOffsets) {
            foreach ($partitionOffsets as $partition => $position) {
                $logStart = $earliestOffsets[$topic][$partition] ?? 0;
                $logEnd   = $latestOffsets[$topic][$partition] ?? 0;
                if ($position >= $logStart && $position <= $logEnd) {
                    continue;
                }

                $wasReset = true;
                $this->subscriptionState->seek(
                    (string) $topic,
                    (int) $partition,
                    $strategy === OffsetResetStrategy::EARLIEST ? $logStart : $logEnd
                );
            }
        }

        if (!$wasReset) {
            throw $exception;
        }

        return $this->subscriptionState->fetchablePartitions();
    }

    /**
     * Tells whether a partitioned failure holds at least one OffsetOutOfRange error
     */
    private static function hasOffsetOutOfRange(TopicPartitionRequestException $exception): bool
    {
        foreach ($exception->getExceptions() as $partitionExceptions) {
            foreach ($partitionExceptions as $partitionException) {
                if ($partitionException instanceof OffsetOutOfRangeException) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Applies `key.deserializer` and `value.deserializer` to the records of a poll()
     *
     * @param array<string, array<int, list<Record>>> $fetchResult Records that came back
     *
     * @return array<string, array<int, list<Record>>> The same records, as ConsumerRecord when a deserializer is set
     */
    private function deserializeRecords(array $fetchResult): array
    {
        if ($this->keyDeserializer === null && $this->valueDeserializer === null) {
            return $fetchResult;
        }

        foreach ($fetchResult as $topic => $partitions) {
            foreach ($partitions as $partition => $records) {
                $fetchResult[$topic][$partition] = array_map(
                    fn(Record $record): ConsumerRecord => ConsumerRecord::fromRecord(
                        $record,
                        (string) $topic,
                        (int) $partition,
                        $this->deserialize($this->keyDeserializer, (string) $topic, $record->key),
                        $this->deserialize($this->valueDeserializer, (string) $topic, $record->value)
                    ),
                    $records
                );
            }
        }

        return $fetchResult;
    }

    /**
     * Runs one deserializer over the raw bytes of a key or a value, which are null for a record that has none
     */
    private function deserialize(?Deserializer $deserializer, string $topic, ?string $data): mixed
    {
        if ($deserializer === null || $data === null) {
            return $data;
        }

        return $deserializer->deserialize($topic, $data);
    }

    /**
     * Reads the committed offsets of the newly assigned partitions and makes them the positions of this consumer
     *
     * @param array<string, PartitionsForTopic> $topicPartitions The new assignment
     */
    private function refreshTopicPartitionOffsets(array $topicPartitions): void
    {
        if ($this->groupId() === '') {
            // Without a group there is nothing to read the positions from, they all follow auto.offset.reset
            $committedOffsets = [];
            foreach ($topicPartitions as $topic => $partitions) {
                $committedOffsets[$topic] = array_fill_keys($partitions->partitions, -1);
            }
        } else {
            $committedOffsets = $this->getClient()->fetchGroupOffsets(
                $this->getCoordinator(),
                $this->groupId(),
                $topicPartitions
            );
        }

        foreach ($this->autoResetOffsets($committedOffsets) as $topic => $partitionOffsets) {
            foreach ($partitionOffsets as $partition => $offset) {
                $this->subscriptionState->seek((string) $topic, (int) $partition, (int) $offset);
            }
        }
    }

    /**
     * Tells whether the positions are committed automatically by poll()
     */
    private function isAutoCommitEnabled(): bool
    {
        return (bool) $this->configuration[ConsumerConfig::ENABLE_AUTO_COMMIT];
    }

    /**
     * Return the configured consumer group, which may be empty for a consumer that does not commit anything
     */
    private function groupId(): string
    {
        return (string) $this->configuration[ConsumerConfig::GROUP_ID];
    }

    /**
     * Return the configured consumer group, failing when there is none
     */
    private function requireGroupId(): string
    {
        $groupId = $this->groupId();
        if ($groupId === '') {
            throw new InvalidConfigurationException(
                'Committed offsets are stored per consumer group, so ' . ConsumerConfig::GROUP_ID . ' has to be '
                . 'configured to commit or to read them.'
            );
        }

        return $groupId;
    }

    /**
     * Builds a deserializer out of the configured instance or class name
     */
    private static function resolveDeserializer(mixed $deserializer): ?Deserializer
    {
        if ($deserializer === null || $deserializer instanceof Deserializer) {
            return $deserializer;
        }

        if (is_string($deserializer) && is_subclass_of($deserializer, Deserializer::class)) {
            return new $deserializer();
        }

        throw new InvalidConfigurationException(
            'A deserializer has to be an instance of ' . Deserializer::class . ' or the name of a class that '
            . 'implements it, ' . get_debug_type($deserializer) . ' given.'
        );
    }

    /**
     * Normalizes an assignment into the DTOs of the protocol layer, which is what `main` passes around
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Topic name => partitions of that topic
     *
     * @return array<string, PartitionsForTopic>
     */
    private static function normalizeAssignment(array $topicPartitions): array
    {
        $result = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $result[$topic] = $partitions instanceof PartitionsForTopic
                ? $partitions
                : new PartitionsForTopic((string) $topic, array_values(array_map(intval(...), $partitions)));
        }

        return $result;
    }

    /**
     * Normalizes an assignment into plain lists of partition ids
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Topic name => partitions of that topic
     *
     * @return array<string, list<int>>
     */
    private static function normalizePartitionLists(array $topicPartitions): array
    {
        $result = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $result[$topic] = $partitions instanceof PartitionsForTopic
                ? array_values($partitions->partitions)
                : array_values(array_map(intval(...), $partitions));
        }

        return $result;
    }
}
