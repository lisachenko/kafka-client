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

use function is_int;

use Protocol\Kafka\Common\Errors\InvalidConfigurationException;

/**
 * Common grunt work of the built-in assignors: the plain lists they work on and the structures the protocol wants.
 *
 * A subclass only implements {@see assignPartitions()}, which maps the member ids to the partitions they get; this
 * class turns the {@see Subscription} structures of the members into the topic lists such an implementation works
 * with, and wraps the result back into the {@see MemberAssignment} structures that the leader publishes with its
 * SyncGroup request. Like `AbstractPartitionAssignor` of the Java client of 0.9.0.1, it keeps no state between two
 * assignments and sends no `userData`.
 *
 * @see \Protocol\Kafka\Consumer\RangeAssignor
 * @see \Protocol\Kafka\Consumer\RoundRobinAssignor
 */
abstract class AbstractPartitionAssignor implements PartitionAssignorInterface
{
    /**
     * Built-in assignors of Kafka 0.9.0.1, indexed by the wire name they announce
     *
     * @var array<string, class-string<PartitionAssignorInterface>>
     */
    private const array BUILTIN_ASSIGNORS = [
        RangeAssignor::NAME      => RangeAssignor::class,
        RoundRobinAssignor::NAME => RoundRobinAssignor::class,
    ];

    /**
     * Creates the assignor that the `partition.assignment.strategy` option asks for.
     *
     * The option holds either the wire name of a built-in assignor - `range`, the default of the Java client of
     * 0.9.0.1, or `roundrobin` - or the name of a class that implements {@see PartitionAssignorInterface}.
     *
     * @param string $strategy Value of {@see ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY}
     *
     * @throws InvalidConfigurationException for a name that is neither built in nor a usable class
     */
    final public static function fromStrategy(string $strategy): PartitionAssignorInterface
    {
        if (isset(self::BUILTIN_ASSIGNORS[$strategy])) {
            $assignorClass = self::BUILTIN_ASSIGNORS[$strategy];

            return new $assignorClass();
        }

        if (!is_subclass_of($strategy, PartitionAssignorInterface::class)) {
            $knownNames = implode(', ', array_keys(self::BUILTIN_ASSIGNORS));
            throw new InvalidConfigurationException(
                "Unknown partition assignment strategy {$strategy}: use one of {$knownNames}, or the name of a class"
                . ' that implements ' . PartitionAssignorInterface::class
            );
        }

        return new $strategy();
    }

    /**
     * @inheritdoc
     */
    public function subscription(array $topics): Subscription
    {
        return new Subscription(array_values($topics));
    }

    /**
     * @inheritdoc
     */
    final public function assign(array $partitionsPerTopic, array $subscriptions): array
    {
        $subscribedTopics = [];
        foreach ($subscriptions as $memberId => $subscription) {
            $subscribedTopics[$memberId] = array_values($subscription->topics);
        }

        $partitions = [];
        foreach ($partitionsPerTopic as $topic => $topicPartitions) {
            $partitions[$topic] = self::partitionsOf($topicPartitions);
        }

        $assignments = [];
        foreach ($this->assignPartitions($partitions, $subscribedTopics) as $memberId => $memberPartitions) {
            $assignments[$memberId] = new MemberAssignment(array_filter($memberPartitions));
        }

        return $assignments;
    }

    /**
     * Distributes the partitions of the subscribed topics over the members of the group.
     *
     * The result has to hold an entry for every member of the input, even for a member that gets no partition at
     * all, because the leader publishes an assignment for every member of the group.
     *
     * @param array<string, list<int>>    $partitionsPerTopic Partition ids of every topic the cluster metadata knows
     * @param array<string, list<string>> $subscribedTopics   Topics of every member, indexed by its member id
     *
     * @return array<string, array<string, list<int>>> Partitions per topic for every member, by its member id
     */
    abstract protected function assignPartitions(array $partitionsPerTopic, array $subscribedTopics): array;

    /**
     * Returns the members that subscribe to each of the topics, indexed by the topic name
     *
     * @param array<string, list<string>> $subscribedTopics Topics of every member, indexed by its member id
     *
     * @return array<string, list<string>>
     */
    final protected static function membersPerTopic(array $subscribedTopics): array
    {
        $membersPerTopic = [];
        foreach ($subscribedTopics as $memberId => $topics) {
            foreach ($topics as $topic) {
                $membersPerTopic[$topic][] = (string) $memberId;
            }
        }

        return $membersPerTopic;
    }

    /**
     * Normalizes the partitions of one topic: a number of partitions becomes the ids 0..n-1, a list of ids is sorted
     *
     * @param int|list<int> $partitions Number of partitions of a topic, or the partition ids themselves
     *
     * @return list<int>
     */
    private static function partitionsOf(int|array $partitions): array
    {
        if (is_int($partitions)) {
            return $partitions > 0 ? range(0, $partitions - 1) : [];
        }

        $partitionIds = array_values($partitions);
        sort($partitionIds);

        return $partitionIds;
    }
}
