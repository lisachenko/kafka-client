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

namespace Protocol\Kafka\Consumer\Internals;

use InvalidArgumentException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;

/**
 * Bookkeeping of the partitions a consumer works on: which ones are assigned, where it reads them and which of
 * them are paused.
 *
 * Kafka 0.10.2.2 knows two ways to get partitions, and they are mutually exclusive, exactly as in the Java client:
 * {@see assignFromUser()} for the partitions an application picked itself ({@see \Protocol\Kafka\Consumer\KafkaConsumer::assign()})
 * and {@see subscribeByTopics()} plus {@see assignFromSubscribed()} for the ones the group coordinator handed out
 * through JoinGroup/SyncGroup ({@see \Protocol\Kafka\Consumer\KafkaConsumer::subscribe()}). The pattern subscription
 * of the Java client (`TYPE_AUTO_PATTERN`) is not implemented here, exactly as it is not implemented on `main`: it
 * is a client-side concern - the consumer matches the pattern against the topics of the cluster metadata - and no
 * wire structure carries it. Kafka 0.10 gives it the piece it was missing, the "every topic" metadata refresh that
 * the nullable topic array of Metadata v1 asks for ({@see \Protocol\Kafka\Common\Cluster::topics()}), so the day it
 * is implemented `exclude.internal.topics` is what decides whether a pattern may match `__consumer_offsets`.
 *
 * @see docs/protocol/2.8.md, section "Consumer group protocol (protocol_type = consumer)"
 */
final class SubscriptionState
{
    /**
     * No subscription type has been defined yet
     */
    public const int TYPE_NONE = 0;

    /**
     * Subscription to a list of topics through KafkaConsumer::subscribe(), the partitions come from the group
     */
    public const int TYPE_AUTO_TOPICS = 1;

    /**
     * Subscription is assigned manually through KafkaConsumer::assign()
     */
    public const int TYPE_USER_ASSIGNED = 3;

    /**
     * Assigned partitions as [topic: string][partition: int] => ['position' => ?int, 'isPaused' => bool]
     *
     * @var array<string, array<int, array{position: int|null, isPaused: bool}>>
     */
    private array $assignment = [];

    /**
     * Topics of a subscription, as a set of topic name => true; empty for a manual assignment
     *
     * @var array<string, true>
     */
    private array $subscription = [];

    /**
     * Type of this subscription, one of the self::TYPE_* constants
     */
    private int $subscriptionType = self::TYPE_NONE;

    /**
     * Return type of this subscription
     */
    public function getSubscriptionType(): int
    {
        return $this->subscriptionType;
    }

    /**
     * Assigns partitions manually
     *
     * The position of a partition that is assigned again is kept, exactly as the later protocol lines do it, so
     * that a re-assignment of the same partitions does not rewind a running consumer.
     *
     * @param array<string, PartitionsForTopic> $topicPartitions Topic name => DTO with the partitions of that topic
     */
    public function assignFromUser(array $topicPartitions): void
    {
        $this->setSubscriptionType(self::TYPE_USER_ASSIGNED);
        $this->setAssignment($topicPartitions);
    }

    /**
     * Subscribes to the given list of topics, whose partitions are handed out by the group coordinator
     *
     * The subscription replaces the previous one; the partitions themselves only arrive with the SyncGroup answer
     * of the next rebalance, which is what {@see assignFromSubscribed()} stores.
     *
     * @param list<string> $topics Topics to subscribe to
     */
    public function subscribeByTopics(array $topics): void
    {
        $this->setSubscriptionType(self::TYPE_AUTO_TOPICS);
        $this->subscription = array_fill_keys($topics, true);
    }

    /**
     * Stores the partitions that the leader of the group assigned to this member
     *
     * The assignment is refused when it names a topic this member did not subscribe to: a group whose members do
     * not agree on the assignor - or a leader with a bug - would otherwise silently make a consumer read a topic
     * its application knows nothing about.
     *
     * @param array<string, PartitionsForTopic> $assignments Topic name => DTO with the partitions of that topic
     */
    public function assignFromSubscribed(array $assignments): void
    {
        if (!$this->partitionsAutoAssigned()) {
            throw new InvalidArgumentException(
                'Attempt to dynamically assign partitions while manual assignment is in use'
            );
        }

        $unknownTopics = array_diff_key($assignments, $this->subscription);
        if ($unknownTopics !== []) {
            throw new InvalidArgumentException(
                sprintf(
                    'The leader assigned the not subscribed topics [%s]; the subscription is [%s]',
                    implode(', ', array_keys($unknownTopics)),
                    implode(', ', $this->getSubscription())
                )
            );
        }

        $this->setAssignment($assignments);
    }

    /**
     * Return the list of topics this consumer subscribed to, empty for a manual assignment
     *
     * @return list<string>
     */
    public function getSubscription(): array
    {
        return array_keys($this->subscription);
    }

    /**
     * Tells whether the partitions of this state are handed out by the group coordinator
     */
    public function partitionsAutoAssigned(): bool
    {
        return $this->subscriptionType === self::TYPE_AUTO_TOPICS;
    }

    /**
     * Return the assignment as [topic: string][partition: int] => partition state
     *
     * @return array<string, array<int, array{position: int|null, isPaused: bool}>>
     */
    public function getAssignment(): array
    {
        return $this->assignment;
    }

    /**
     * Drops every assignment and every subscription of this state
     */
    public function unsubscribe(): void
    {
        $this->subscriptionType = self::TYPE_NONE;
        $this->assignment       = [];
        $this->subscription     = [];
    }

    /**
     * Test if the given topic-partition is assigned to this subscription
     */
    public function isAssigned(string $topic, int $partition): bool
    {
        return isset($this->assignment[$topic][$partition]);
    }

    /**
     * Return the topic-partitions that may be fetched from the broker, with the offset to fetch them from.
     *
     * A partition that has been paused, and one whose position is not known yet, is not fetchable.
     *
     * @return array<string, array<int, int>> [topic: string][partition: int] => offset
     */
    public function fetchablePartitions(): array
    {
        $result = [];
        foreach ($this->assignment as $topic => $partitions) {
            foreach ($partitions as $partition => $state) {
                if (!$state['isPaused'] && $state['position'] !== null) {
                    $result[$topic][$partition] = $state['position'];
                }
            }
        }

        return $result;
    }

    /**
     * Return the positions of every assigned topic-partition, paused ones included.
     *
     * This is what a commit without an explicit argument sends to the broker.
     *
     * @return array<string, array<int, int>> [topic: string][partition: int] => offset
     */
    public function allConsumed(): array
    {
        $result = [];
        foreach ($this->assignment as $topic => $partitions) {
            foreach ($partitions as $partition => $state) {
                if ($state['position'] !== null) {
                    $result[$topic][$partition] = $state['position'];
                }
            }
        }

        return $result;
    }

    /**
     * Overrides the fetch offset that the consumer will use on the next poll().
     *
     * @param string $topic     Name of the topic
     * @param int    $partition Id of the partition
     * @param int    $offset    New offset value
     */
    public function seek(string $topic, int $partition, int $offset, ?int $offsetEpoch = null): void
    {
        if (!$this->isAssigned($topic, $partition)) {
            throw new UnknownTopicOrPartitionException(['topic' => $topic, 'partition' => $partition]);
        }

        $this->assignment[$topic][$partition]['position']      = $offset;
        $this->assignment[$topic][$partition]['positionEpoch']  = $offsetEpoch;
        // A position that was just set is trusted; only a leader change marks it for validation again
        $this->assignment[$topic][$partition]['needsValidation'] = false;
    }

    /**
     * Returns the leader epoch the position of a partition belongs to, `null` while none is known (KIP-320)
     *
     * This is the `offsetEpoch` of the Java `FetchPosition`: the epoch that was leading when the record at the
     * position was written, taken from the batch the consumer last read, from the `leader_epoch` of a ListOffsets
     * v4 answer, or from the committed offset of an OffsetFetch v5 answer. It is what an OffsetForLeaderEpoch
     * request asks the new leader about after a leader change, see
     * {@see \Protocol\Kafka\Consumer\KafkaConsumer::validatePositionsIfNeeded()}.
     */
    public function positionEpoch(string $topic, int $partition): ?int
    {
        return $this->assignment[$topic][$partition]['positionEpoch'] ?? null;
    }

    /**
     * Returns the leader epoch the consumer believes this partition is being led with, `null` when unknown
     *
     * This is the `currentLeader` of the Java `FetchPosition`, and it is what travels as the
     * `current_leader_epoch` of a Fetch v9, a ListOffsets v4 and an OffsetForLeaderEpoch v2 request: the broker
     * refuses the request with 74 or 75 when the belief is stale in either direction.
     */
    public function currentLeaderEpoch(string $topic, int $partition): ?int
    {
        return $this->assignment[$topic][$partition]['currentLeaderEpoch'] ?? null;
    }

    /**
     * Records the leader epoch the metadata reports for a partition and marks the position for validation
     *
     * A **new** epoch means that the partition has been led by someone else in the meantime, which is exactly the
     * moment at which the position may point into a part of the log that the new leader never had
     * (KIP-320). The position is therefore marked as "needs validation" and
     * {@see \Protocol\Kafka\Consumer\KafkaConsumer::validatePositionsIfNeeded()} asks the new leader with an
     * OffsetForLeaderEpoch v2 before the next fetch. A partition whose position carries no epoch of its own - a
     * consumer that has never read a record batch of it - has nothing to validate and is only re-stamped.
     */
    public function setCurrentLeaderEpoch(string $topic, int $partition, ?int $epoch): void
    {
        if (!$this->isAssigned($topic, $partition)) {
            return;
        }

        $previous = $this->assignment[$topic][$partition]['currentLeaderEpoch'] ?? null;
        $this->assignment[$topic][$partition]['currentLeaderEpoch'] = $epoch;

        if ($epoch === null || $previous === null || $epoch <= $previous) {
            return;
        }
        if ($this->assignment[$topic][$partition]['positionEpoch'] === null) {
            return;
        }

        $this->assignment[$topic][$partition]['needsValidation'] = true;
    }

    /**
     * Tells whether the position of a partition has to be validated before it is fetched from again
     */
    public function needsValidation(string $topic, int $partition): bool
    {
        return (bool) ($this->assignment[$topic][$partition]['needsValidation'] ?? false);
    }

    /**
     * Marks the position of a partition as validated against the leader it is about to be fetched from
     */
    public function completeValidation(string $topic, int $partition): void
    {
        if ($this->isAssigned($topic, $partition)) {
            $this->assignment[$topic][$partition]['needsValidation'] = false;
        }
    }

    /**
     * Records the leader epoch of the last record batch that was read from a partition
     *
     * The Java consumer takes the `partitionLeaderEpoch` of the batch the last consumed record came from and
     * keeps it next to the position, so that the epoch and the offset always describe the same point of the log.
     */
    public function setPositionEpoch(string $topic, int $partition, ?int $offsetEpoch): void
    {
        if ($offsetEpoch !== null && $this->isAssigned($topic, $partition)) {
            $this->assignment[$topic][$partition]['positionEpoch'] = $offsetEpoch;
        }
    }

    /**
     * Get the offset of the next record that will be fetched from this topic-partition
     */
    public function position(string $topic, int $partition): int
    {
        if (!$this->isAssigned($topic, $partition)) {
            throw new UnknownTopicOrPartitionException(['topic' => $topic, 'partition' => $partition]);
        }

        $position = $this->assignment[$topic][$partition]['position'];
        if ($position === null) {
            throw new UnknownTopicOrPartitionException([
                'topic'     => $topic,
                'partition' => $partition,
                'reason'    => 'the position of this partition is not known yet',
            ]);
        }

        return $position;
    }

    /**
     * Suspend consumption of the given topic-partitions
     *
     * @param array<string, list<int>> $topicPartitions Topic name => list of partitions to pause
     */
    public function pause(array $topicPartitions): void
    {
        $this->pauseConsumption($topicPartitions, true);
    }

    /**
     * Resume consumption of the given topic-partitions
     *
     * @param array<string, list<int>> $topicPartitions Topic name => list of partitions to resume
     */
    public function resume(array $topicPartitions): void
    {
        $this->pauseConsumption($topicPartitions, false);
    }

    /**
     * Tells whether consumption of the given topic-partition is suspended
     */
    public function isPaused(string $topic, int $partition): bool
    {
        return $this->assignment[$topic][$partition]['isPaused'] ?? false;
    }

    /**
     * Changes the type of this state
     *
     * @param int $type New type, must be one of the self::TYPE_* constants
     */
    private function setSubscriptionType(int $type): void
    {
        if ($this->subscriptionType === self::TYPE_NONE) {
            $this->subscriptionType = $type;
        }

        if ($this->subscriptionType !== $type) {
            throw new InvalidArgumentException(
                'Subscription to topics, partitions and pattern are mutually exclusive'
            );
        }
    }

    /**
     * Replaces the assignment of this state, keeping the state of the partitions that stay assigned
     *
     * @param array<string, PartitionsForTopic> $assignment Topic name => DTO with the partitions of that topic
     */
    private function setAssignment(array $assignment): void
    {
        $targetAssignment = [];
        foreach ($assignment as $topic => $topicPartitions) {
            foreach ($topicPartitions->partitions as $partitionId) {
                $targetAssignment[$topic][$partitionId] = $this->assignment[$topic][$partitionId]
                    ?? [
                        'position'           => null,
                        'isPaused'           => false,
                        // KIP-320: the epoch the position belongs to, the epoch the partition is believed to be
                        // led with, and whether the position still has to be validated against a new leader
                        'positionEpoch'      => null,
                        'currentLeaderEpoch' => null,
                        'needsValidation'    => false,
                    ];
            }
        }

        $this->assignment = $targetAssignment;
    }

    /**
     * Return a string with all the current assignments, used in error messages
     */
    private function formatAssignment(string $separator = ', '): string
    {
        $result = [];
        foreach ($this->assignment as $topic => $partitions) {
            foreach (array_keys($partitions) as $partition) {
                $result[] = sprintf('%s:%s', $topic, $partition);
            }
        }

        return implode($separator, $result);
    }

    /**
     * Suspends or resumes consumption of the given topic-partitions
     *
     * @param array<string, list<int>> $topicPartitions Topic name => list of partitions to pause or resume
     * @param bool                     $isPaused        True to pause the consumption, false to resume it
     */
    private function pauseConsumption(array $topicPartitions, bool $isPaused): void
    {
        foreach ($topicPartitions as $topic => $partitionIds) {
            foreach ($partitionIds as $partitionId) {
                if (!$this->isAssigned((string) $topic, (int) $partitionId)) {
                    throw new InvalidArgumentException(
                        sprintf(
                            'Paused partition %s:%s is not assigned to this consumer; assignment is [%s]',
                            $topic,
                            $partitionId,
                            $this->formatAssignment()
                        )
                    );
                }

                $this->assignment[$topic][$partitionId]['isPaused'] = $isPaused;
            }
        }
    }
}
