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
 * The share-partition start offset and lag of one partition (ApiKey 90, Kafka 4.1, KIP-932; v1 Kafka 4.2, KIP-1226)
 *
 * <pre>
 *   DescribeShareGroupOffsetsResponsePartition => PartitionIndex StartOffset LeaderEpoch Lag ErrorCode ErrorMessage
 *                                                 TAG_BUFFER
 *     PartitionIndex => INT32
 *     StartOffset    => INT64
 *     LeaderEpoch    => INT32
 *     Lag            => INT64 (version 1+)
 *     ErrorCode      => INT16
 *     ErrorMessage   => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * The **start offset** is the first offset of the partition a share group has not finished with yet - where the
 * share-partition leader resumes - and it is what the share coordinator keeps in `__share_group_state`. The
 * **lag** of the version 1 (Kafka 4.2, *"Version 1 introduces Lag (KIP-1226)"*) is what the group still has to
 * deliver: the end offset of the partition minus the start offset minus the records past the start offset that are
 * already done with, -1 when the node cannot compute it. {@see DescribeShareGroupOffsetsResponsePartitionV0} is the
 * entry of the version 0, without it.
 *
 * @see docs/protocol/4.3.md, section "DescribeShareGroupOffsets API (key 90, v0 and v1)"
 * @see docs/protocol/4.3.md, section "The share-partition lag of KIP-1226 (v1)"
 */
class DescribeShareGroupOffsetsResponsePartition implements BinarySchemaInterface
{
    /**
     * Version of the DescribeShareGroupOffsets API that this DTO is unpacked from
     */
    public const int VERSION = 1;

    /**
     * The lag of a partition the node could not compute one for, the `default` of the field
     */
    public const int UNINITIALIZED_LAG = -1;

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
     * The share-partition lag: records of the partition the group has not finished with, -1 when it is not known
     *
     * @since Version 1 of protocol (Kafka 4.2, KIP-1226)
     */
    public int $lag = self::UNINITIALIZED_LAG;

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
        $scheme = [
            'partitionIndex' => BinarySchema::TYPE_INT32,
            'startOffset'    => BinarySchema::TYPE_INT64,
            'leaderEpoch'    => BinarySchema::TYPE_INT32,
        ];
        if (static::VERSION >= 1) {
            $scheme['lag'] = BinarySchema::TYPE_INT64;
        }

        return $scheme + [
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
