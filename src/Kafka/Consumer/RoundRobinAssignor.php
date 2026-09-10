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

use function count;
use function in_array;

/**
 * The roundrobin assignor lays out all the available partitions and all the available members.
 *
 * It then proceeds to do a round-robin assignment from partition to member: the topics are walked in lexicographic
 * order, their partitions in numeric order, and every partition goes to the next member - in lexicographic order of
 * the member ids - that subscribes to its topic. If the subscriptions of all members are identical, the partitions
 * are uniformly distributed, i.e. the partition ownership counts are within a delta of exactly one across all
 * members.
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1 and t1p2.
 *
 * The assignment will be:
 *
 * C0: [t0p0, t0p2, t1p1]
 * C1: [t0p1, t1p0, t1p2]
 *
 * @see docs/protocol/1.1.md, section "Consumer group protocol (protocol_type = consumer)"
 */
class RoundRobinAssignor extends AbstractPartitionAssignor
{
    /**
     * Wire name of this assignor, the `protocol_name` of the JoinGroup request
     */
    public const string NAME = 'roundrobin';

    /**
     * @inheritdoc
     */
    public function name(): string
    {
        return self::NAME;
    }

    /**
     * @inheritdoc
     */
    protected function assignPartitions(array $partitionsPerTopic, array $subscribedTopics): array
    {
        $assignment = [];
        foreach (array_keys($subscribedTopics) as $memberId) {
            $assignment[$memberId] = [];
        }

        $memberIds = array_map(strval(...), array_keys($subscribedTopics));
        sort($memberIds, SORT_STRING);
        if ($memberIds === []) {
            return $assignment;
        }

        // The Java assignor walks a circular iterator over the sorted member ids and skips the members that do not
        // subscribe to the topic of the partition it is about to hand out; this index does the same
        $memberIndex = 0;
        foreach (self::allPartitionsSorted($partitionsPerTopic, $subscribedTopics) as [$topic, $partition]) {
            while (!in_array($topic, $subscribedTopics[$memberIds[$memberIndex]], true)) {
                $memberIndex = ($memberIndex + 1) % count($memberIds);
            }
            $assignment[$memberIds[$memberIndex]][$topic][] = $partition;
            $memberIndex                                    = ($memberIndex + 1) % count($memberIds);
        }

        return $assignment;
    }

    /**
     * Returns every partition of every subscribed topic, topics in lexicographic and partitions in numeric order
     *
     * @param array<string, list<int>>    $partitionsPerTopic Partition ids of every topic the metadata knows
     * @param array<string, list<string>> $subscribedTopics   Topics of every member, indexed by its member id
     *
     * @return list<array{0: string, 1: int}> Topic and partition id of every partition to assign
     */
    private static function allPartitionsSorted(array $partitionsPerTopic, array $subscribedTopics): array
    {
        $topics = [];
        foreach ($subscribedTopics as $memberTopics) {
            foreach ($memberTopics as $topic) {
                $topics[$topic] = $topic;
            }
        }
        ksort($topics, SORT_STRING);

        $allPartitions = [];
        foreach ($topics as $topic) {
            foreach ($partitionsPerTopic[$topic] ?? [] as $partition) {
                $allPartitions[] = [$topic, $partition];
            }
        }

        return $allPartitions;
    }
}
