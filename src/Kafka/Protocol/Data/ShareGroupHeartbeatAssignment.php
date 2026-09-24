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
 * The assignment a ShareGroupHeartbeat answer hands to one member (key 76, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   Assignment => [topic_partitions]        -- NULLABLE struct
 * </pre>
 *
 * *"null if not provided; the assignment otherwise"* (`ShareGroupHeartbeatResponse.json` @ 4.1.0), a nullable
 * structure as in the consumer protocol: `ff` for "nothing changed", `01` and the array for the partitions the member
 * may fetch now. Unlike a KIP-848 member, a share member does not acknowledge the assignment - the same partition is
 * assigned to several members of a share group at once, and a record is what one of them acquires, not a partition.
 *
 * @see docs/protocol/4.3.md, section "ShareGroupHeartbeat API (key 76, v1)"
 */
final class ShareGroupHeartbeatAssignment implements BinarySchemaInterface
{
    /**
     * @param list<ShareGroupHeartbeatTopicPartitions> $topicPartitions Partitions assigned to the member
     */
    public function __construct(
        public array $topicPartitions = []
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicPartitions' => [ShareGroupHeartbeatTopicPartitions::class],
        ];
    }

    /**
     * Returns the assignment as the 16 raw bytes of a topic id => its partitions
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
