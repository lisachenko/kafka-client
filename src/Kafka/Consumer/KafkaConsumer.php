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
use Protocol\Kafka\Common\Errors\FencedLeaderEpochException;
use Protocol\Kafka\Common\Errors\IllegalGenerationException;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\LogTruncationException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\RebalanceInProgressException;
use Protocol\Kafka\Common\Errors\RecordTooLargeException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownLeaderEpochException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Serialization\Deserializer;
use Protocol\Kafka\Consumer\Internals\AbortedTransactionFilter;
use Protocol\Kafka\Consumer\Internals\ConsumerCoordinator;
use Protocol\Kafka\Consumer\Internals\SubscriptionState;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochRequestPartition;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Throwable;

/**
 * A Kafka client that consumes records from a Kafka 0.10.2.2 cluster.
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
 *     ConsumerConfig::SESSION_TIMEOUT_MS   => 10000,
 *     ConsumerConfig::MAX_POLL_INTERVAL_MS => 30000,
 *     // request.timeout.ms has to be larger than both, a JoinGroup blocks until the rebalance is over
 *     ClientConfig::REQUEST_TIMEOUT_MS     => 40000,
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
 * or poll more often.
 *
 * `max.poll.interval.ms` (Kafka 0.10.1) is the second half of that story, and it works differently here than it
 * does in Java. Its value is sent to the coordinator as the `rebalance_timeout` of every JoinGroup v1 request, so
 * it really is what the coordinator waits for this member in a rebalance; but the Java consumer *also* leaves the
 * group by itself when the application does not call poll() within that interval, which it can only do because its
 * heartbeats come from a thread of their own. **This consumer has no such thread**: an application that stops
 * polling stops heartbeating, and the coordinator drops the member when `session.timeout.ms` expires. What
 * `max.poll.interval.ms` therefore buys here is the time the *rest* of the group is willing to wait for this member
 * in a rebalance - and the requirement that `request.timeout.ms` exceed it, because a JoinGroup blocks that long.
 *
 * Where the committed offsets are kept is chosen with `offsets.storage`: `kafka` commits them to the coordinator
 * of the group with the version 2 of the OffsetCommit api, which carries the member id and the generation of this
 * consumer, `zookeeper` uses the version 0, which is what the consumers of Kafka 0.8.1 did. The two storages are
 * independent, so a group has one position per storage.
 *
 * The records are fetched with **Fetch v7**, and every broker this consumer reads from holds an **incremental
 * fetch session** for it (KIP-227, Kafka 1.1, {@see \Protocol\Kafka\Consumer\Internals\FetchSessionHandler}): the
 * first request to a broker states the whole assignment and opens the session, every following one states only the
 * partitions whose position moved and lets the broker fill in the rest, a partition that leaves the assignment - a
 * rebalance, {@see pause()}, a topic that is gone - is dropped from the session with the `forgotten_topics_data`
 * of the next request, and the answer carries only the partitions that have news. Nothing of that is visible in
 * poll(): a session error (70, 71) and a dropped connection are answered with a full fetch by the client itself.
 *
 * A Fetch v7 answer is the one of v5, so a 1.1 broker answers with the log as it lies: record batches of
 * the message format v2, whose records carry their timestamps and their headers ({@see Record::$headers}, KIP-82),
 * and the control markers of a transaction are dropped by the record layer before a record ever reaches poll().
 * The whole answer is bounded by `fetch.max.bytes` on top of the per-partition `max.partition.fetch.bytes`; the
 * broker fills the partitions in the order of the request until that budget is used up, so this consumer rotates
 * the order of its partitions after every poll: a partition that returned records is moved behind the ones that
 * did not, which is what the Java consumer does as well (`Fetcher.parseCompletedFetch` ⇒
 * `SubscriptionState.movePartitionToEnd`). A single message that is larger than either limit is returned in full
 * as long as it is the first one of the answer, so a partition can no longer be stuck on it - the
 * {@see RecordTooLargeException} of the lower Fetch versions is not raised by a 0.11 broker any more.
 *
 * **`isolation.level = read_committed`** ({@see ConsumerConfig::ISOLATION_LEVEL}, KIP-98) makes this consumer the
 * read side of the transactional producer: the Fetch and the Offsets request state the level, so the broker stops
 * the answer at the *last stable offset* of a partition instead of at its high watermark - nothing of a
 * transaction that is still open is shown, and `endOffsets()` reports the offset a `read_committed` reader can
 * really reach - and the records of the transactions the answer reports as **aborted** are dropped by
 * {@see \Protocol\Kafka\Consumer\Internals\AbortedTransactionFilter} before poll() returns, because a 0.11.0.3
 * broker sends them and only names them. The default is `read_uncommitted`, which shows every record of the log.
 *
 * @see docs/protocol/2.8.md, section "Transactions"
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
     * Order in which the assigned topic-partitions are asked for, as a list of `[topic, partition]` pairs
     *
     * Version 3 of the Fetch API bounds the whole answer with `fetch.max.bytes` and the broker fills the
     * partitions in the order of the request, so the ones at its end come back empty while the ones in front of
     * them carry data. Rotating this order after every poll - the partitions that returned something move behind
     * the ones that did not - is what keeps every partition of a large assignment served.
     *
     * @var list<array{string, int}>
     */
    private array $fetchOrder = [];

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
     * Get the first offset that is still available in each of the given partitions.
     *
     * This is the `beginningOffsets()` of the Java consumer of Kafka 0.10.1: an {@see OffsetsRequest::EARLIEST}
     * lookup that only reports the offsets and, unlike {@see seekToBeginning()}, moves nothing and needs no
     * assignment. The offset of a partition whose log was never written to, and of one whose messages have all been
     * deleted by the retention, is the offset the next produced message will get.
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Topic name => partitions to look up
     *
     * @return array<string, array<int, int>> [topic: string][partition: int] => first available offset
     */
    public function beginningOffsets(array $topicPartitions): array
    {
        return $this->listOffsets($topicPartitions, OffsetsRequest::EARLIEST);
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
     * with the empty member id and the generation -1 of a request that belongs to no generation. A **static**
     * member ({@see ConsumerConfig::GROUP_INSTANCE_ID}, KIP-345) also names its instance in the commit, and a
     * commit of an instance another consumer has taken over is answered 82 (`FencedInstanceId`), which is fatal.
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
            (int) $this->configuration[ConsumerConfig::OFFSET_RETENTION_MS],
            $groupCoordinator->getGroupInstanceId()
        );
    }

    /**
     * Get the last committed offset of every given topic-partition, whether this consumer committed it or not.
     *
     * A topic-partition that the group has never committed comes back with the offset -1, whichever storage the
     * `offsets.storage` option selects.
     *
     * The partitions are always named explicitly here, as they are in the Java consumer. "Every topic the group
     * committed" is what the nullable topic array of OffsetFetch v2 asks for, and it is an administrative question
     * rather than a consumer one: {@see \Protocol\Kafka\Admin\AdminClient::listGroupOffsets()} answers it.
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
     * Get the offset the next produced message will get in each of the given partitions.
     *
     * This is the `endOffsets()` of the Java consumer of Kafka 0.10.1: an {@see OffsetsRequest::LATEST} lookup that
     * only reports the offsets and, unlike {@see seekToEnd()}, moves nothing and needs no assignment. The offset is
     * the high watermark of the partition, i.e. the end of what a consumer is allowed to read, and it is also the
     * number of messages the partition holds when nothing was ever deleted from it.
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Topic name => partitions to look up
     *
     * @return array<string, array<int, int>> [topic: string][partition: int] => log end offset
     */
    public function endOffsets(array $topicPartitions): array
    {
        return $this->listOffsets($topicPartitions, OffsetsRequest::LATEST);
    }

    /**
     * Look up the offsets of the given partitions by the timestamps of their messages.
     *
     * This is the `offsetsForTimes()` of the Java consumer, which Kafka 0.10.1 added together with version 1 of the
     * Offsets api (KIP-79): the offset of a partition is the one of the **first message whose own timestamp is at
     * or after** the timestamp that was searched for, and it comes back with that message's timestamp, which is
     * therefore usually larger than the one that was asked for. Whether those timestamps are the `CreateTime` of
     * the producer or the `LogAppendTime` of the broker is the `message.timestamp.type` of the topic.
     *
     * A partition that holds no such message - a timestamp above the last message of the log, and any timestamp on
     * an empty partition - is answered with `null` and no error at all, exactly as in the Java client. A topic whose
     * `message.format.version` is older than 0.10.0 has no message timestamps to search and makes the broker answer
     * the error code 43, `UnsupportedForMessageFormat`.
     *
     * Nothing is moved by this call: it is a query, and a consumer that wants to read from what it found seeks
     * there itself.
     *
     * ```php
     * $offsets = $consumer->offsetsForTimes(['my-topic' => [0 => $sinceMs, 1 => $sinceMs]]);
     * foreach ($offsets as $topic => $partitions) {
     *     foreach ($partitions as $partition => $found) {
     *         $consumer->seek($topic, $partition, $found?->offset ?? $consumer->endOffsets([$topic => [$partition]])[$topic][$partition]);
     *     }
     * }
     * ```
     *
     * @param array<string, array<int, int>> $timestampsToSearch [topic: string][partition: int] => timestamp in
     *                                                           milliseconds since the epoch
     *
     * @return array<string, array<int, OffsetAndTimestamp|null>> [topic][partition] => offset and the timestamp of
     *                                                            the message it points at, or null when the
     *                                                            partition holds no message at or after the time
     *
     * @throws TopicPartitionRequestException when a partition was answered with an error code, which is how the
     *         `UnsupportedForMessageFormatException` of a topic whose `message.format.version` is older than 0.10.0
     *         arrives
     */
    public function offsetsForTimes(array $timestampsToSearch): array
    {
        if ($timestampsToSearch === []) {
            return [];
        }

        return $this->getClient()->fetchTopicPartitionOffsetsForTimes($timestampsToSearch);
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
     * The partitions are asked for in a **rotating order**: a Fetch v7 request is bounded by `fetch.max.bytes`
     * for the whole answer and the broker serves the partitions in the order it was asked, so every partition
     * that returned records in this poll() is moved behind the ones that did not before the next one is sent.
     *
     * The request itself is the incremental fetch of the session this consumer holds with every broker (KIP-227):
     * it states the positions that moved since the previous poll(), forgets the partitions that left the
     * assignment, and is answered with the partitions that have news alone. A partition that is not in that
     * answer simply keeps its position and is polled again.
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

        $activeTopicPartitionOffsets = $this->inFetchOrder($this->subscriptionState->fetchablePartitions());
        if ($activeTopicPartitionOffsets === []) {
            return [];
        }

        // KIP-320: stamp every position with the leader epoch the metadata reports and, when that epoch is NEW,
        // ask the new leader where the epoch of the position ended before a single record is read
        $this->refreshLeaderEpochs($activeTopicPartitionOffsets);
        $activeTopicPartitionOffsets = $this->validatePositionsIfNeeded($activeTopicPartitionOffsets);
        if ($activeTopicPartitionOffsets === []) {
            return [];
        }

        $fetchedPartitions = $this->fetchMessages($activeTopicPartitionOffsets, $timeout);
        $result            = $this->collectRecords($fetchedPartitions);

        $this->updateFetchPositions($fetchedPartitions);
        $this->rotateFetchOrder($fetchedPartitions);

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
     * The partitions have to be assigned to this consumer; {@see beginningOffsets()} asks the same question about
     * any partition of the cluster without moving anything.
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
     * The partitions have to be assigned to this consumer; {@see endOffsets()} asks the same question about any
     * partition of the cluster without moving anything.
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
     * @throws InvalidConfigurationException when `request.timeout.ms` does not exceed both `session.timeout.ms` and
     *                                       `max.poll.interval.ms`, which a JoinGroup that waits for the whole
     *                                       rebalance needs it to
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
        $this->requireRequestTimeoutAboveTheBlockingTimeouts();

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
     *
     * **A static member ({@see ConsumerConfig::GROUP_INSTANCE_ID}, KIP-345) does not leave**: it commits and stops,
     * and the coordinator keeps its partitions for `session.timeout.ms` so that the very same instance picks them
     * up again when it comes back - a restart of such a consumer costs the group no rebalance at all.
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
     * Returns the newest leader epoch the metadata has reported for a partition, `null` while there is none
     *
     * The epoch comes from the `leader_epoch` of a Metadata v7 answer, which {@see Cluster} keeps per partition
     * and only ever moves forward (KIP-320); this method is the seam a test replaces to script a leader change.
     */
    protected function leaderEpochOf(string $topic, int $partition): ?int
    {
        return $this->getCluster()->lastSeenLeaderEpoch($topic, $partition);
    }

    /**
     * Reloads the cluster metadata, which is what the two leader-epoch errors of KIP-320 are cured with
     *
     * 74 `FENCED_LEADER_EPOCH` and 75 `UNKNOWN_LEADER_EPOCH` say that this client's picture of the leadership of a
     * partition is stale in one direction or the other; the answer is always a fresh Metadata, never a move of a
     * position. The seam exists so that a test can observe the refresh without a cluster.
     */
    protected function refreshMetadata(): void
    {
        $this->getCluster()->reload();
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
                    // KIP-320: the position and the epoch it belongs to move together. The epoch is the
                    // `partition_leader_epoch` of the LAST batch that was read, which is what the Java consumer
                    // keeps in `FetchPosition.offsetEpoch`; a legacy message set carries none and leaves it as it
                    // was, which is the "I have never seen an epoch of this partition" of a magic 0/1 topic.
                    $this->subscriptionState->seek(
                        (string) $topic,
                        (int) $partitionId,
                        $nextOffset,
                        $this->subscriptionState->positionEpoch((string) $topic, (int) $partitionId)
                    );
                    $this->subscriptionState->setPositionEpoch(
                        (string) $topic,
                        (int) $partitionId,
                        self::lastBatchLeaderEpochOf($fetchedPartition)
                    );
                }
            }
        }
    }

    /**
     * Returns the `partition_leader_epoch` of the last record batch of an answer, `null` when there is none
     *
     * A batch of the message format v2 carries the epoch its leader was on when it was appended (KIP-101); a
     * legacy message set has no such field, and a batch a client wrote carries -1 until the broker stamps it.
     */
    private static function lastBatchLeaderEpochOf(FetchedPartition $fetchedPartition): ?int
    {
        $epoch = null;
        foreach ($fetchedPartition->getMemoryRecords()->getBatches() as $batch) {
            if (!$batch instanceof RecordBatch) {
                continue;
            }
            if ($batch->partitionLeaderEpoch !== RecordBatch::NO_PARTITION_LEADER_EPOCH) {
                $epoch = $batch->partitionLeaderEpoch;
            }
        }

        return $epoch;
    }

    /**
     * Stamps every fetchable position with the leader epoch the metadata reports for its partition (KIP-320)
     *
     * The epoch comes from the `leader_epoch` of a **Metadata v7** answer, which {@see Cluster} keeps per
     * partition and only ever moves forward - an answer of a broker that has not caught up with the controller is
     * ignored, see {@see Cluster::updateLastSeenEpochIfNewer()}. A *new* epoch means that the partition has been
     * led by someone else since the position was taken, which is what marks the position for validation.
     *
     * @param array<string, array<int, int>> $activeTopicPartitionOffsets Positions of this poll
     */
    private function refreshLeaderEpochs(array $activeTopicPartitionOffsets): void
    {
        foreach ($activeTopicPartitionOffsets as $topic => $partitionOffsets) {
            foreach (array_keys($partitionOffsets) as $partitionId) {
                $this->subscriptionState->setCurrentLeaderEpoch(
                    (string) $topic,
                    (int) $partitionId,
                    $this->leaderEpochOf((string) $topic, (int) $partitionId)
                );
            }
        }
    }

    /**
     * Asks the new leader of every partition whose epoch changed where the epoch of the position ended (KIP-320)
     *
     * This is `Fetcher.validateOffsetsIfNeeded` of the Java consumer, and it is the whole point of the leader
     * epochs. A partition is validated exactly once per leader change, with an **OffsetForLeaderEpoch v2** that
     * names the epoch of the position (`leader_epoch`) and the epoch the consumer believes the partition is led
     * with (`current_leader_epoch`). Three answers are possible:
     *
     * * `end_offset` **at or above** the position - the position is inside a part of the log the new leader has,
     *   and the partition is fetched from as it was;
     * * `end_offset` **below** the position - the log diverged there: the records the consumer was about to read
     *   never made it into this leadership. With `auto.offset.reset = earliest` or `latest` the position is reset
     *   to the bound of the log, and with **`none`** a {@see LogTruncationException} reaches the caller;
     * * an error - **74** `FENCED_LEADER_EPOCH` or **75** `UNKNOWN_LEADER_EPOCH` say that the belief about the
     *   leadership is stale in one direction or the other, which is cured by a metadata refresh and never by
     *   moving the position; the partition simply stays unvalidated and is left out of this poll.
     *
     * @param array<string, array<int, int>> $activeTopicPartitionOffsets Positions of this poll
     *
     * @return array<string, array<int, int>> The positions to fetch from, without the ones that stay unvalidated
     *
     * @throws LogTruncationException When the log was truncated and `auto.offset.reset` is `none`
     */
    private function validatePositionsIfNeeded(array $activeTopicPartitionOffsets): array
    {
        $toValidate = [];
        foreach ($activeTopicPartitionOffsets as $topic => $partitionOffsets) {
            foreach (array_keys($partitionOffsets) as $partitionId) {
                if (!$this->subscriptionState->needsValidation((string) $topic, (int) $partitionId)) {
                    continue;
                }
                $positionEpoch = $this->subscriptionState->positionEpoch((string) $topic, (int) $partitionId);
                $currentEpoch  = $this->subscriptionState->currentLeaderEpoch((string) $topic, (int) $partitionId);
                if ($positionEpoch === null) {
                    // Nothing to validate: the consumer has never read a record batch of this partition
                    $this->subscriptionState->completeValidation((string) $topic, (int) $partitionId);
                    continue;
                }
                $toValidate[$topic][$partitionId] = [
                    $positionEpoch,
                    $currentEpoch ?? OffsetForLeaderEpochRequestPartition::UNKNOWN_LEADER_EPOCH,
                ];
            }
        }

        if ($toValidate === []) {
            return $activeTopicPartitionOffsets;
        }

        try {
            $answers = $this->getClient()->offsetsForLeaderEpochs($toValidate);
        } catch (TopicPartitionRequestException $exception) {
            // 74 and 75 are metadata problems, not position problems: refresh and leave the partition for the
            // next poll. Everything else is reported to the caller.
            if (!self::isLeaderEpochError($exception)) {
                throw $exception;
            }
            $this->refreshMetadata();
            $answers = $exception->getPartialResult();
        }

        $truncated = [];
        foreach ($answers as $topic => $partitions) {
            foreach ($partitions as $partitionId => $answer) {
                $position = $activeTopicPartitionOffsets[$topic][$partitionId] ?? null;
                $this->subscriptionState->completeValidation((string) $topic, (int) $partitionId);
                if ($position === null || $answer->endOffset < 0 || $answer->endOffset >= $position) {
                    continue;
                }
                $truncated[$topic][$partitionId] = $answer->endOffset;
            }
        }

        foreach ($truncated as $topic => $partitions) {
            foreach ($partitions as $partitionId => $endOffset) {
                $this->onLogTruncation((string) $topic, (int) $partitionId, $endOffset);
            }
        }

        // Only the partitions that are validated - or that never needed it - are fetched from in this poll
        $fetchable = [];
        foreach ($activeTopicPartitionOffsets as $topic => $partitionOffsets) {
            foreach ($partitionOffsets as $partitionId => $offset) {
                if ($this->subscriptionState->needsValidation((string) $topic, (int) $partitionId)) {
                    continue;
                }
                $fetchable[$topic][$partitionId] = $this->subscriptionState->position((string) $topic, (int) $partitionId);
            }
        }

        return $fetchable;
    }

    /**
     * Reacts to a position that the new leader of a partition does not have any more
     *
     * `auto.offset.reset` decides: `earliest` and `latest` move the position to the bound of the log, `none`
     * reports the divergence to the caller, exactly as the Java consumer does.
     *
     * @throws LogTruncationException With `auto.offset.reset = none`
     */
    private function onLogTruncation(string $topic, int $partition, int $truncationOffset): void
    {
        $position = $this->subscriptionState->position($topic, $partition);
        $strategy = $this->configuration[ConsumerConfig::AUTO_OFFSET_RESET] ?? OffsetResetStrategy::LATEST;

        if ($strategy !== OffsetResetStrategy::EARLIEST && $strategy !== OffsetResetStrategy::LATEST) {
            throw new LogTruncationException([
                'error'            => 'The log of the partition was truncated below the position of this consumer',
                'topic'            => $topic,
                'partition'        => $partition,
                'offset'           => $position,
                'truncationOffset' => $truncationOffset,
            ]);
        }

        $timestamp = $strategy === OffsetResetStrategy::EARLIEST ? OffsetsRequest::EARLIEST : OffsetsRequest::LATEST;
        $reset     = $this->listOffsets([$topic => [$partition]], $timestamp);
        foreach ($reset as $resetTopic => $partitionOffsets) {
            foreach ($partitionOffsets as $resetPartition => $offset) {
                $this->subscriptionState->seek((string) $resetTopic, (int) $resetPartition, (int) $offset);
            }
        }
    }

    /**
     * Tells whether every failed partition of a request failed with one of the two leader-epoch codes of KIP-320
     */
    private static function isLeaderEpochError(TopicPartitionRequestException $exception): bool
    {
        $errors = [];
        foreach ($exception->getExceptions() as $partitionExceptions) {
            foreach ($partitionExceptions as $partitionException) {
                $errors[] = $partitionException;
            }
        }
        if ($errors === []) {
            return false;
        }

        foreach ($errors as $error) {
            if (!$error instanceof FencedLeaderEpochException && !$error instanceof UnknownLeaderEpochException) {
                return false;
            }
        }

        return true;
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
                // Up to version 2 of the Fetch API the broker fills the answer up to MaxBytes without
                // guaranteeing that one message fits, so a partition whose next message is bigger would come
                // back empty forever; version 5, which this consumer sends, always returns that message instead
                if ($fetchedPartition->isSingleMessageTooLarge()) {
                    throw new RecordTooLargeException(
                        (string) $topic,
                        (int) $partitionId,
                        $fetchedPartition->fetchOffset,
                        (int) $this->configuration[ConsumerConfig::MAX_PARTITION_FETCH_BYTES],
                        $fetchedPartition->highWaterMarkOffset
                    );
                }

                $fetchOffset = $fetchedPartition->fetchOffset;
                // A `read_committed` consumer drops the records of the transactions the answer reports as aborted
                // itself: the broker only bounds the answer by the last stable offset and names those transactions
                $visibleRecords = $this->isReadCommitted()
                    ? AbortedTransactionFilter::committedRecords($fetchedPartition)
                    : $fetchedPartition->getRecords();

                $result[$topic][$partitionId] = array_values(array_filter(
                    $visibleRecords,
                    static fn(Record $record): bool => $record->offset === null || $record->offset >= $fetchOffset
                ));
            }
        }

        return $result;
    }

    /**
     * Puts the fetchable positions into the order in which this consumer asks the broker for them
     *
     * The order is the one of the last poll(), with the partitions that were served moved to its end; a partition
     * of a fresh assignment is appended behind everything that is already known, and one that is not assigned any
     * more is dropped. A paused partition keeps its place, it is only left out of the request itself. Because the
     * wire format groups the partitions of a topic together, a partition can only be moved behind the other
     * partitions of its own topic; that is the very same approximation the Java consumer makes
     * (`org.apache.kafka.common.internals.PartitionStates` @ 0.10.2.2).
     *
     * @param array<string, array<int, int>> $fetchablePartitions Positions of the partitions that may be fetched
     *
     * @return array<string, array<int, int>> The same positions, in the order they are asked for
     */
    private function inFetchOrder(array $fetchablePartitions): array
    {
        $assignment = $this->subscriptionState->getAssignment();

        $order = $seen = [];
        foreach ($this->fetchOrder as [$topic, $partition]) {
            if (isset($assignment[$topic][$partition])) {
                $order[]                    = [$topic, $partition];
                $seen[$topic][$partition] = true;
            }
        }
        // Everything this consumer has not fetched yet - a fresh assignment - goes behind what it already knows
        foreach ($assignment as $topic => $partitions) {
            foreach (array_keys($partitions) as $partition) {
                if (!isset($seen[$topic][$partition])) {
                    $order[] = [(string) $topic, (int) $partition];
                }
            }
        }
        $this->fetchOrder = $order;

        $ordered = [];
        foreach ($order as [$topic, $partition]) {
            if (isset($fetchablePartitions[$topic][$partition])) {
                $ordered[$topic][$partition] = $fetchablePartitions[$topic][$partition];
            }
        }

        return $ordered;
    }

    /**
     * Moves every partition that the broker served behind the partitions that it did not serve
     *
     * A partition that came back with records had its share of the `fetch.max.bytes` of the answer, so it goes to
     * the end of the queue; the ones that came back empty stay in front and are the first to be served by the
     * next request. This is the rule of the Java consumer, whose `Fetcher` moves a partition to the end of its
     * assignment as soon as the answer carried bytes for it or reported an error.
     *
     * @param array<string, array<int, FetchedPartition>> $fetchedPartitions Partitions of one poll()
     */
    private function rotateFetchOrder(array $fetchedPartitions): void
    {
        $served = [];
        foreach ($fetchedPartitions as $topic => $partitions) {
            foreach ($partitions as $partitionId => $fetchedPartition) {
                if ($fetchedPartition->count() > 0 || $fetchedPartition->errorCode !== 0) {
                    $served[$topic][$partitionId] = true;
                }
            }
        }
        if ($served === []) {
            return;
        }

        $waiting = $rotated = [];
        foreach ($this->fetchOrder as [$topic, $partition]) {
            if (isset($served[$topic][$partition])) {
                $rotated[] = [$topic, $partition];
            } else {
                $waiting[] = [$topic, $partition];
            }
        }

        $this->fetchOrder = array_merge($waiting, $rotated);
    }

    /**
     * Fetches the messages of the given positions, resetting the ones the broker refuses as out of range
     *
     * A position that fell out of the log - because the retention deleted the segment it pointed at, or because
     * the topic was recreated - is answered with the error code 1, OffsetOutOfRange. The consumer resolves that
     * exactly like a missing committed offset: it follows `auto.offset.reset` and fetches again.
     *
     * The fetch itself goes through {@see Client::fetchPartitionsWithSessions()}, i.e. through the **incremental
     * fetch session** (KIP-227) that this consumer holds with every broker it reads from, so what comes back are
     * only the partitions that have news. A partition the answer does not mention keeps everything the consumer
     * knows about it, its position included.
     *
     * @param array<string, array<int, int>> $activeTopicPartitionOffsets Positions to fetch from
     * @param int                            $timeout                     Poll timeout in milliseconds
     *
     * @return array<string, array<int, FetchedPartition>> What the broker answered for each partition
     */
    private function fetchMessages(array $activeTopicPartitionOffsets, int $timeout): array
    {
        try {
            return $this->getClient()->fetchPartitionsWithSessions($activeTopicPartitionOffsets, $timeout);
        } catch (OffsetOutOfRangeException $exception) {
            $resetOffsets = $this->resetOutOfRangeOffsets($activeTopicPartitionOffsets, $exception);
        } catch (TopicPartitionRequestException $exception) {
            // A client that reports the failed partitions separately keeps the answers of the healthy ones
            if (!self::hasOffsetOutOfRange($exception)) {
                throw $exception;
            }
            $resetOffsets = $this->resetOutOfRangeOffsets($activeTopicPartitionOffsets, $exception);
        }

        return $this->getClient()->fetchPartitionsWithSessions($resetOffsets, $timeout);
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
            (int) ($this->configuration[ConsumerConfig::RETRY_BACKOFF_MS] ?? 100),
            $this->rebalanceTimeoutMs(),
            $this->groupInstanceId()
        );
    }

    /**
     * Returns the `group.instance.id` of this consumer, null for a dynamic member (KIP-345, Kafka 2.3)
     *
     * An empty string is treated as "not configured": the broker refuses an empty instance id with 42
     * (InvalidRequest), and a configuration file that carries the option without a value must not turn a dynamic
     * consumer into a broken static one.
     */
    private function groupInstanceId(): ?string
    {
        $groupInstanceId = $this->configuration[ConsumerConfig::GROUP_INSTANCE_ID] ?? null;

        return is_string($groupInstanceId) && $groupInstanceId !== '' ? $groupInstanceId : null;
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
     * The coordinator answers a JoinGroup only once every member of the group has rejoined or has run out of time,
     * so a socket read timeout - `request.timeout.ms` - that is not larger than the time it may wait turns a
     * perfectly normal rebalance into a network error.
     *
     * Two options bound that wait since Kafka 0.10.1, and the request timeout has to exceed **both**:
     * `session.timeout.ms`, after which a member that stopped sending heartbeats is dropped, and
     * `max.poll.interval.ms`, which this consumer sends as the `rebalance_timeout` of its JoinGroup v1 request and
     * which is what the coordinator really waits for a member of the group to rejoin (`GroupMetadata` @ 0.10.2.2
     * takes the largest rebalance timeout of the members). The Java consumer refuses the combination in its
     * constructor and picks its `request.timeout.ms` default of 305000 for exactly this reason; this one refuses it
     * when a group is actually joined.
     *
     * @throws InvalidConfigurationException
     */
    private function requireRequestTimeoutAboveTheBlockingTimeouts(): void
    {
        $requestTimeoutMs = (int) $this->configuration[ConsumerConfig::REQUEST_TIMEOUT_MS];
        $blockingTimeouts = [
            ConsumerConfig::SESSION_TIMEOUT_MS   => (int) $this->configuration[ConsumerConfig::SESSION_TIMEOUT_MS],
            ConsumerConfig::MAX_POLL_INTERVAL_MS => $this->rebalanceTimeoutMs(),
        ];

        foreach ($blockingTimeouts as $option => $timeoutMs) {
            if ($requestTimeoutMs > $timeoutMs) {
                continue;
            }

            throw new InvalidConfigurationException(
                sprintf(
                    '%s (%d) has to be greater than %s (%d): the coordinator answers the JoinGroup request of a '
                    . 'member only once the whole rebalance is over, which can take a full %s.',
                    ConsumerConfig::REQUEST_TIMEOUT_MS,
                    $requestTimeoutMs,
                    $option,
                    $timeoutMs,
                    $option
                )
            );
        }
    }

    /**
     * Returns the `rebalance_timeout` this consumer sends with its JoinGroup requests, `max.poll.interval.ms`
     */
    private function rebalanceTimeoutMs(): int
    {
        return (int) ($this->configuration[ConsumerConfig::MAX_POLL_INTERVAL_MS]
            ?? ConsumerConfig::DEFAULT_MAX_POLL_INTERVAL_MS);
    }

    /**
     * Tells whether the positions are committed automatically by poll()
     */
    private function isAutoCommitEnabled(): bool
    {
        return (bool) $this->configuration[ConsumerConfig::ENABLE_AUTO_COMMIT];
    }

    /**
     * Tells whether this consumer only sees the records of committed transactions.
     *
     * The option is read here exactly as {@see Client} reads it for the Fetch and the Offsets request it builds -
     * either of the two strings of the Java consumer, or the wire value itself - so that the level a request
     * states and the level the fetch loop filters with can never disagree.
     */
    private function isReadCommitted(): bool
    {
        $configured = $this->configuration[ConsumerConfig::ISOLATION_LEVEL]
            ?? ConsumerConfig::ISOLATION_LEVEL_READ_UNCOMMITTED;

        if (is_int($configured)) {
            return $configured === FetchRequest::READ_COMMITTED;
        }

        return strtolower(trim((string) $configured)) === ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED;
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
     * Asks the leaders of the given partitions for one offset each, without touching the position of the consumer
     *
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Topic name => partitions to look up
     * @param int                                         $timestamp       {@see OffsetsRequest::LATEST},
     *                                                                     {@see OffsetsRequest::EARLIEST} or a
     *                                                                     timestamp in milliseconds
     *
     * @return array<string, array<int, int>> [topic: string][partition: int] => offset
     */
    private function listOffsets(array $topicPartitions, int $timestamp): array
    {
        if ($topicPartitions === []) {
            return [];
        }

        $request = [];
        foreach (self::normalizePartitionLists($topicPartitions) as $topic => $partitions) {
            $request[$topic] = array_fill_keys($partitions, $timestamp);
        }

        return $this->getClient()->fetchTopicPartitionOffsets($request);
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
