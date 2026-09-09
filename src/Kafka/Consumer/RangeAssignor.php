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

/**
 * The range assignor works on a per-topic basis and is the default of the Java client of Kafka 0.10.2.2.
 *
 * For each topic it lays out the available partitions in numeric order and the members that subscribe to it in
 * lexicographic order, then divides the number of partitions by the number of those members to get the number of
 * partitions per member. If the division is not even, the first few members get one extra partition.
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1 and t1p2.
 *
 * The assignment will be:
 *
 * C0: [t0p0, t0p1, t1p0, t1p1]
 * C1: [t0p2, t1p2]
 *
 * Because the split is per topic, a group that consumes many topics with few partitions each puts most of the work
 * on the first members: with one partition per topic, the lexicographically first member gets every partition.
 * {@see RoundRobinAssignor} is the alternative that spreads such a subscription evenly.
 *
 * @see docs/protocol/0.10.2.md, section "Consumer group protocol (protocol_type = consumer)"
 */
class RangeAssignor extends AbstractPartitionAssignor
{
    /**
     * Wire name of this assignor, the `protocol_name` of the JoinGroup request
     */
    public const string NAME = 'range';

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

        $membersPerTopic = self::membersPerTopic($subscribedTopics);
        ksort($membersPerTopic);

        foreach ($membersPerTopic as $topic => $membersForTopic) {
            // A topic the cluster metadata does not know is skipped, exactly as the Java assignor skips a null count
            if (!isset($partitionsPerTopic[$topic])) {
                continue;
            }

            $partitions   = $partitionsPerTopic[$topic];
            $memberCount  = count($membersForTopic);
            $perMember    = intdiv(count($partitions), $memberCount);
            $withOneMore  = count($partitions) % $memberCount;
            sort($membersForTopic, SORT_STRING);

            foreach ($membersForTopic as $index => $memberId) {
                $start  = $perMember * $index + min($index, $withOneMore);
                $length = $perMember + ($index + 1 > $withOneMore ? 0 : 1);
                if ($length > 0) {
                    $assignment[$memberId][$topic] = array_slice($partitions, $start, $length);
                }
            }
        }

        return $assignment;
    }
}
