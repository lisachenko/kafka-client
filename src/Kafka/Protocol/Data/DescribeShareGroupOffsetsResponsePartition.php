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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The share-partition start offset of one partition (ApiKey 90, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   DescribeShareGroupOffsetsResponsePartition => PartitionIndex StartOffset LeaderEpoch ErrorCode ErrorMessage
 *                                                 TAG_BUFFER
 *     PartitionIndex => INT32
 *     StartOffset    => INT64
 *     LeaderEpoch    => INT32
 *     ErrorCode      => INT16
 *     ErrorMessage   => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * The **start offset** is the first offset of the partition a share group has not finished with yet - where the
 * share-partition leader resumes - and it is what the share coordinator keeps in `__share_group_state`. The
 * `lag` of the version 1 is Kafka 4.2's.
 *
 * @see docs/protocol/4.3.md, section "DescribeShareGroupOffsets API (key 90, v0)"
 */
final class DescribeShareGroupOffsetsResponsePartition implements BinarySchemaInterface
{
    /**
     * Index of the partition
     */
    public int $partitionIndex;

    /**
     * The share-partition start offset, -1 when the group holds no state for the partition
     */
    public int $startOffset = -1;

    /**
     * Leader epoch of the partition the state was written under
     */
    public int $leaderEpoch = -1;

    /**
     * Error of this partition, 0 when it could be described
     */
    public int $errorCode = 0;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionIndex' => BinarySchema::TYPE_INT32,
            'startOffset'    => BinarySchema::TYPE_INT64,
            'leaderEpoch'    => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
