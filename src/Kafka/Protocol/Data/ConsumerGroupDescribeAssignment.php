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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One assignment of a member in a ConsumerGroupDescribe answer (key 69, Kafka 3.7, KIP-848)
 *
 * <pre>
 *   Assignment => [topic_partitions]
 *     topic_partitions => topic_id topic_name [partitions]
 * </pre>
 *
 * The `commonStructs` entry `Assignment` of `ConsumerGroupDescribeResponse.json` @ 3.9.2. Every member of the
 * answer carries **two** of them - the `assignment` it owns right now and the `target_assignment` the coordinator
 * wants it to own - and the pair is what makes a reconciliation visible from outside: while the two differ, the
 * member is still giving partitions up or taking them over, and a group whose members all have the two equal is
 * settled.
 *
 * Unlike the assignment of a ConsumerGroupHeartbeat answer ({@see ConsumerGroupHeartbeatAssignment}) this
 * structure is **not nullable**: a member that owns nothing carries an assignment with an empty array, and there
 * is no "unchanged" to express here.
 *
 * @see \Protocol\Kafka\Protocol\Request\ConsumerGroupDescribeResponse
 * @see docs/protocol/3.9.md, section "ConsumerGroupDescribe API (key 69, v0)"
 */
class ConsumerGroupDescribeAssignment implements BinarySchemaInterface
{
    /**
     * @param list<ConsumerGroupDescribeTopicPartitions> $topicPartitions Partitions of this assignment
     */
    public function __construct(
        /**
         * Partitions of this assignment, one entry per topic
         *
         * @var list<ConsumerGroupDescribeTopicPartitions>
         */
        public array $topicPartitions = []
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicPartitions' => [ConsumerGroupDescribeTopicPartitions::class],
        ];
    }

    /**
     * Returns the assignment as a map of the topic name to the partitions of that topic
     *
     * @return array<string, list<int>>
     */
    public function partitionsByTopic(): array
    {
        $partitions = [];
        foreach ($this->topicPartitions as $entry) {
            $partitions[$entry->topicName] = $entry->partitions;
        }

        return $partitions;
    }
}
