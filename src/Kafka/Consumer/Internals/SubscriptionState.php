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
 * Kafka 0.8.2.2 has no broker-side group membership, so the only way to get partitions is {@see assignFromUser()};
 * the subscription types that the later protocol lines fill in through JoinGroup/SyncGroup (TYPE_AUTO_TOPICS,
 * TYPE_AUTO_PATTERN) do not exist here.
 */
final class SubscriptionState
{
    /**
     * No subscription type has been defined yet
     */
    public const int TYPE_NONE = 0;

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
     * Return the list of subscribed topics, which is always empty on this protocol line
     *
     * Topic subscriptions need the group membership APIs of Kafka 0.9, see KafkaConsumer::subscribe().
     *
     * @return list<string>
     */
    public function getSubscription(): array
    {
        return [];
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
     * Drops every assignment of this state
     */
    public function unsubscribe(): void
    {
        $this->subscriptionType = self::TYPE_NONE;
        $this->assignment       = [];
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
    public function seek(string $topic, int $partition, int $offset): void
    {
        if (!$this->isAssigned($topic, $partition)) {
            throw new UnknownTopicOrPartitionException(['topic' => $topic, 'partition' => $partition]);
        }

        $this->assignment[$topic][$partition]['position'] = $offset;
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
                    ?? ['position' => null, 'isPaused' => false];
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
