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
 * The assignment a ConsumerGroupHeartbeat answer hands to one member (key 68, Kafka 3.5, KIP-848)
 *
 * <pre>
 *   Assignment => [topic_partitions]        -- NULLABLE struct
 *     topic_partitions => topic_id [partitions]
 * </pre>
 *
 * `Assignment` of `ConsumerGroupHeartbeatResponse.json` @ 3.9.2, *"null if not provided; the assignment
 * otherwise"* - a {@see \Protocol\Kafka\Protocol\NullableStruct} field, i.e. one byte in front of the structure,
 * `ff` for the `null` and `01` for a structure that follows. The two are not the same thing:
 *
 * * a **null** assignment means "nothing changed since the last heartbeat", and it is what every steady-state
 *   answer of the coordinator carries;
 * * an assignment **with an empty array** means "you own nothing at all", which is what a member is answered
 *   while the coordinator is still computing the target assignment of a new group epoch, and what the last
 *   member of a group sees when it has given everything up.
 *
 * A member therefore may not treat the two the same way: the empty one revokes, the null one keeps what it has.
 * This is the whole reconciliation protocol of KIP-848 in one field - the coordinator names the partitions the
 * member may own *now*, the member acknowledges them by echoing them in its next heartbeat's `topic_partitions`,
 * and the two agree once the answer stops changing.
 *
 * @see \Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatResponse
 * @see \Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator
 * @see docs/protocol/4.3.md, section "ConsumerGroupHeartbeat API (key 68, v0)"
 */
class ConsumerGroupHeartbeatAssignment implements BinarySchemaInterface
{
    /**
     * @param list<ConsumerGroupHeartbeatTopicPartitions> $topicPartitions Partitions this member may own now
     */
    public function __construct(
        /**
         * Partitions the member holds after this answer, one entry per topic id
         *
         * @var list<ConsumerGroupHeartbeatTopicPartitions>
         */
        public array $topicPartitions = []
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicPartitions' => [ConsumerGroupHeartbeatTopicPartitions::class],
        ];
    }

    /**
     * Returns the assignment as a map of the raw topic id to the partitions of that topic
     *
     * @return array<string, list<int>>
     */
    public function partitionsByTopicId(): array
    {
        $partitions = [];
        foreach ($this->topicPartitions as $entry) {
            $partitions[$entry->topicId] = $entry->partitions;
        }

        return $partitions;
    }
}
