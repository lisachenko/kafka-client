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
 * The assignment of one member in a ShareGroupDescribe answer (key 77, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   Assignment => [topic_partitions]
 * </pre>
 *
 * The `Assignment` common struct of `ShareGroupDescribeResponse.json` @ 4.1.0. A share member has one assignment
 * only - there is no target assignment to reconcile with, because nothing is revoked from a share member before
 * another one may fetch the same partition.
 *
 * @see docs/protocol/4.3.md, section "ShareGroupDescribe API (key 77, v1)"
 */
final class ShareGroupDescribeAssignment implements BinarySchemaInterface
{
    /**
     * @param list<ShareGroupDescribeTopicPartitions> $topicPartitions Partitions assigned to the member
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
            'topicPartitions' => [ShareGroupDescribeTopicPartitions::class],
        ];
    }

    /**
     * Returns the assignment as topic name => partitions
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
