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

use InvalidArgumentException;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\IllegalGenerationException;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\RebalanceInProgressException;
use Protocol\Kafka\Common\Errors\RecordTooLargeException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Serialization\Deserializer;
use Protocol\Kafka\Consumer\Internals\ConsumerCoordinator;
use Protocol\Kafka\Consumer\Internals\SubscriptionState;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Throwable;

/**
 * A Kafka client that consumes records from a Kafka 0.9.0.1 cluster.
 *
 * Kafka 0.9 moved the coordination of a consumer group out of ZooKeeper into the broker, so this consumer knows
 * both ways of getting partitions, and they are mutually exclusive, exactly as in the Java client:
 *
 * - {@see subscribe()} names the topics and lets the group coordinator hand out the partitions. The membership is
 *   established on the next {@see poll()} - JoinGroup, the assignor of the leader, SyncGroup - and every further
 *   poll() keeps it alive with a heartbeat.
 * - {@see assign()} picks the partitions itself and joins no group at all; the offsets of the configured group are
 *   still committed to and read from its coordinator, which is what the 0.8 line could do.
 *
 * Usage against a broker on 127.0.0.1:9092, see also examples/consumer-group.php:
 *
 * ```php
 * $consumer = new KafkaConsumer([
 *     ConsumerConfig::BOOTSTRAP_SERVERS  => ['tcp://127.0.0.1:9092'],
 *     ConsumerConfig::GROUP_ID           => 'my-group',
 *     ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
 *     ConsumerConfig::SESSION_TIMEOUT_MS => 10000,
 *     // request.timeout.ms has to be larger, a JoinGroup blocks until the whole rebalance is over
 *     ClientConfig::REQUEST_TIMEOUT_MS   => 30000,
 * ]);
 *
 * $consumer->subscribe(['my-topic']);
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
 * $consumer->close();
 * ```
 *
 * **The heartbeat is sent from poll(), because PHP has no background thread.** A consumer that does not poll for
 * longer than `session.timeout.ms` is dropped by the coordinator and its partitions are given to another member;
 * the next poll() notices that from the error code of its heartbeat (25/22/27) and rejoins the group. An
 * application whose processing of a batch can take longer than the session timeout therefore has to raise
 * `session.timeout.ms` - within the `group.min.session.timeout.ms`/`group.max.session.timeout.ms` of the broker -
 * or poll more often. There is no `max.poll.interval.ms` on this line, that is Kafka 0.10.1.
 *
 * Where the committed offsets are kept is chosen with `offsets.storage`: `kafka` commits them to the coordinator
 * of the group with the version 2 of the OffsetCommit api, which carries the member id and the generation of this
 * consumer, `zookeeper` uses the version 0, which is what the consumers of Kafka 0.8.1 did. The two storages are
 * independent, so a group has one position per storage.
 *
 * What arrived after 0.9.0.1 is absent: `rebalance.timeout.ms` and `max.poll.interval.ms` (JoinGroup v1, Kafka
 * 0.10.1), the `isolation.level` of the transactional protocol of 0.11 and the message format v1 with timestamps.
 *
 * A message that does not fit into `max.partition.fetch.bytes` is refused with a {@see RecordTooLargeException}
 * rather than silently stalling the partition, because a 0.9.0.1 broker cuts a message set off at that size
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
     * Membership of the configured consumer group, created when the consumer first needs its coordinator
     */
    private ?ConsumerCoordinator $groupCoordinator = null;

    /**
     * Assignor that this consumer offers as the group protocol of its JoinGroup requests
     */
    private readonly PartitionAssignorInterface $assignor;

    /**
     * Callback that observes the partitions this consumer loses and receives with every rebalance
     */
    private ?ConsumerRebalanceListener $rebalanceListener = null;

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

        $this->assignor = AbstractPartitionAssignor::fromStrategy(
            (string) $this->configuration[ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY]
        );

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
     * A consumer that is a member of its group commits with the member id and the generation it holds, which the
     * coordinator refuses once that generation is over (22 IllegalGeneration) or the member was dropped (25
     * UnknownMemberId); a consumer that picked its partitions with {@see assign()} commits as a "simple consumer",
     * with the empty member id and the generation -1 of a request that belongs to no generation.
     * `offset.retention.ms` is passed on as the `retention_time` of the v2 request, with -1 asking the broker for
     * its own `offsets.retention.minutes`.
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

        $groupCoordinator = $this->groupCoordinator();

        $this->getClient()->commitGroupOffsets(
            $groupCoordinator->getNode(),
            $this->requireGroupId(),
            $groupCoordinator->getMemberId(),
            $groupCoordinator->getGenerationId(),
            $topicPartitionOffsets,
            (int) $this->configuration[ConsumerConfig::OFFSET_RETENTION_MS]
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
     * Fetches data for the partitions specified with one of the subscribe/assign APIs.
     *
     * A consumer that subscribed to topics keeps its group membership here, because PHP has no background thread:
     * the first poll() joins the group and receives an assignment, every further one sends a heartbeat as soon as
     * `heartbeat.interval.ms` has elapsed and rejoins the group when the coordinator reports a rebalance, a
     * generation that is over or a member id it does not know. At most one heartbeat is sent per poll(), so the
     * call exceeds the timeout of the caller by at most that single round trip - plus the rebalance itself, whose
     * JoinGroup the coordinator holds until every member of the group has rejoined.
     *
     * Every assigned partition that is not paused is fetched from its current position; the returned records are
     * the ones the broker had, in offset order, and the position of each partition moves behind its last record,
     * so that the next poll() continues where this one stopped. When `enable.auto.commit` is on, the positions
     * are committed once `auto.commit.interval.ms` has passed since the last commit, and always right before the
     * consumer gives its partitions up in a rebalance.
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

        if ($this->subscriptionState->partitionsAutoAssigned()) {
            $this->ensureActiveGroup($milliSeconds);
        }

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
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     *
     * Topic subscriptions are not incremental, this list replaces the current subscription; an empty list is
     * treated the same as {@see unsubscribe()}. It is not possible to combine a subscription with the manual
     * assignment of {@see assign()}, exactly as in the Java client.
     *
     * Nothing is sent to the broker here: the group is joined on the next {@see poll()}, which is also where the
     * consumer notices that the group has to be rebalanced, because
     *
     * - a member joined the group or left it,
     * - an existing member died, i.e. missed its `session.timeout.ms`,
     * - the leader of the group published a new assignment for another reason.
     *
     * The partitions that a rebalance takes away and hands over are reported to the given listener, which is the
     * place to commit the offsets of the partitions that are being revoked when the automatic commit is off.
     *
     * @param list<string>                  $topics   List of topics to subscribe to
     * @param ConsumerRebalanceListener|null $listener Observer of the rebalances of this consumer, if any
     *
     * @throws InvalidConfigurationException when `request.timeout.ms` does not exceed `session.timeout.ms`, which
     *                                       a JoinGroup that waits for the whole rebalance needs it to
     */
    public function subscribe(array $topics, ?ConsumerRebalanceListener $listener = null): void
    {
        if ($topics === []) {
            $this->unsubscribe();

            return;
        }

        $topicNames = array_values(array_map(strval(...), $topics));
        if (array_filter($topicNames, static fn(string $topic): bool => trim($topic) === '') !== []) {
            throw new InvalidArgumentException('The topics to subscribe to can not contain an empty topic name');
        }

        $this->requireGroupId();
        $this->requireRequestTimeoutAboveSessionTimeout();

        $this->rebalanceListener = $listener;
        $this->subscriptionState->subscribeByTopics($topicNames);
        $this->groupCoordinator()->requestRejoin();
    }

    /**
     * Get the topics this consumer subscribed to, which is empty for a manual assignment
     *
     * @return list<string>
     */
    public function subscription(): array
    {
        return $this->subscriptionState->getSubscription();
    }

    /**
     * Unsubscribe from the topics of subscribe(array $topics) and clear the assignment of assign().
     *
     * A member of a group leaves it with a LeaveGroup request, so that the coordinator rebalances the group right
     * away instead of waiting for the session timeout of a member that simply stopped answering. Nothing else is
     * sent to the broker, and the committed offsets of the group stay where they are.
     */
    public function unsubscribe(): void
    {
        if ($this->subscriptionState->partitionsAutoAssigned() && $this->groupCoordinator !== null) {
            $this->groupCoordinator->leaveGroup();
        }

        $this->subscriptionState->unsubscribe();

        $this->groupCoordinator  = null;
        $this->rebalanceListener = null;
    }

    /**
     * Commits the current positions and releases the membership of this consumer.
     *
     * This is what an application calls when it is done consuming: with `enable.auto.commit` on, the positions of
     * the assignment are committed once more, and a member of a group leaves it, which starts the rebalance that
     * hands its partitions to the other members within milliseconds.
     */
    public function close(): void
    {
        if ($this->isAutoCommitEnabled()) {
            try {
                $this->commitSync();
            } catch (KafkaException) {
                // A generation that is already over, or a coordinator that is gone, must not fail a shutdown
            }
        }

        $this->unsubscribe();
    }

    /**
     * A consumer that goes out of scope releases its membership, so that its group does not wait for its session
     */
    public function __destruct()
    {
        if (!$this->subscriptionState->partitionsAutoAssigned()) {
            return;
        }

        try {
            $this->close();
        } catch (Throwable) {
            // The process is shutting down; a broker that is not reachable any more can not be helped here
        }
    }

    /**
     * Return the coordinator node that keeps the committed offsets and the membership of the configured group
     */
    protected function getCoordinator(): Node
    {
        return $this->groupCoordinator()->getNode();
    }

    /**
     * Returns the partition ids of the given topics, which the leader of a group hands to its assignor
     *
     * A topic the cluster does not know - it was deleted, or it does not exist yet and `auto.create.topics.enable`
     * is off on the broker - is left out, exactly as the Java leader leaves out a topic its metadata has no
     * partitions for; the members that subscribed to it simply receive nothing for it in this generation.
     *
     * @param list<string> $topics Union of the topics that the members of the group subscribed to
     *
     * @return array<string, list<int>> Topic name => partition ids of that topic
     */
    protected function partitionsForAssignment(array $topics): array
    {
        $partitionsPerTopic = [];
        foreach ($topics as $topic) {
            try {
                $partitionsPerTopic[$topic] = array_values(array_map(
                    static fn(PartitionMetadata $partition): int => $partition->partitionId,
                    $this->getCluster()->partitionsForTopic($topic)
                ));
            } catch (KafkaException) {
                // The metadata of this topic is not available, it takes part in the next generation
            }
        }

        return $partitionsPerTopic;
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
     * Keeps the membership of a subscribed consumer alive, and rebalances the group when that is needed
     *
     * @param int $nowMs Moment this poll() started, in milliseconds
     */
    private function ensureActiveGroup(int $nowMs): void
    {
        $groupCoordinator = $this->groupCoordinator();
        $groupCoordinator->maybeHeartbeat($nowMs);

        if (!$groupCoordinator->needsRejoin()) {
            return;
        }

        // The partitions are about to be given up, so what has been consumed of them is committed while this
        // member still holds the generation that the coordinator accepts a commit for
        if ($this->isAutoCommitEnabled()) {
            $this->commitBeforeRebalance();
        }

        $revokedPartitions = $this->assignedPartitionLists();
        if ($revokedPartitions !== []) {
            $this->rebalanceListener?->onPartitionsRevoked($revokedPartitions);
        }

        $assignment = self::normalizeAssignment($groupCoordinator->ensureActiveGroup(
            $this->subscriptionState->getSubscription(),
            fn(array $topics): array => $this->partitionsForAssignment($topics)
        ));

        $this->subscriptionState->assignFromSubscribed($assignment);
        if ($assignment !== []) {
            $this->refreshTopicPartitionOffsets($assignment);
        }

        $this->rebalanceListener?->onPartitionsAssigned($this->assignedPartitionLists());
    }

    /**
     * Commits the positions of the assignment that a rebalance is about to take away
     *
     * The commit is best effort: the coordinator refuses it once this member has lost its generation (22), was
     * dropped (25) or the group is already rebalancing (27), and none of the three may keep the consumer from
     * joining the new generation - the records of those partitions are simply consumed again by whoever gets them.
     */
    private function commitBeforeRebalance(): void
    {
        try {
            $this->commitSync();
        } catch (IllegalGenerationException | UnknownMemberIdException | RebalanceInProgressException) {
            // The generation is over, the offsets of it can not be committed any more
        }
    }

    /**
     * Returns the membership of the configured consumer group, created on the first use
     */
    private function groupCoordinator(): ConsumerCoordinator
    {
        return $this->groupCoordinator ??= new ConsumerCoordinator(
            $this->getClient(),
            $this->requireGroupId(),
            $this->assignor,
            (int) $this->configuration[ConsumerConfig::HEARTBEAT_INTERVAL_MS],
            (int) ($this->configuration[ConsumerConfig::RETRY_BACKOFF_MS] ?? 100)
        );
    }

    /**
     * Returns the current assignment as plain partition lists, which is what a rebalance listener receives
     *
     * @return array<string, list<int>>
     */
    private function assignedPartitionLists(): array
    {
        $result = [];
        foreach ($this->subscriptionState->getAssignment() as $topic => $partitions) {
            $result[$topic] = array_map(intval(...), array_keys($partitions));
        }

        return $result;
    }

    /**
     * Refuses a group membership whose JoinGroup could time out before the rebalance it waits for is over
     *
     * The coordinator answers a JoinGroup only once every member of the group has rejoined or has missed its
     * session timeout, so a socket read timeout - `request.timeout.ms` - that is not larger than
     * `session.timeout.ms` turns a perfectly normal rebalance into a network error. The Java consumer of 0.9.0.1
     * refuses that combination in its constructor, and this one refuses it when a group is actually joined.
     *
     * @throws InvalidConfigurationException
     */
    private function requireRequestTimeoutAboveSessionTimeout(): void
    {
        $requestTimeoutMs = (int) $this->configuration[ConsumerConfig::REQUEST_TIMEOUT_MS];
        $sessionTimeoutMs = (int) $this->configuration[ConsumerConfig::SESSION_TIMEOUT_MS];
        if ($requestTimeoutMs > $sessionTimeoutMs) {
            return;
        }

        throw new InvalidConfigurationException(
            sprintf(
                '%s (%d) has to be greater than %s (%d): the coordinator answers the JoinGroup request of a '
                . 'member only once the whole rebalance is over, which can take a full session timeout.',
                ConsumerConfig::REQUEST_TIMEOUT_MS,
                $requestTimeoutMs,
                ConsumerConfig::SESSION_TIMEOUT_MS,
                $sessionTimeoutMs
            )
        );
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
