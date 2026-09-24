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
 * The summary of one share partition in a ReadShareGroupStateSummary answer (key 87, v0, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   PartitionResult => Partition ErrorCode ErrorMessage StateEpoch LeaderEpoch StartOffset
 * </pre>
 *
 * `ReadShareGroupStateSummaryResponseData.PartitionResult` @ 4.1.0: the state of the partition without its
 * batches - what DescribeShareGroupOffsets (90) needs to answer the start offset of a share group.
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupStateSummary API (key 87, v0)"
 */
class ReadShareGroupStateSummaryResponsePartition implements BinarySchemaInterface
{
    /**
     * Index of the partition
     */
    public int $partition = 0;

    /**
     * Error of the partition, 0 when there is none
     */
    public int $errorCode = 0;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * State epoch of the share partition
     */
    public int $stateEpoch = 0;

    /**
     * Leader epoch of the share partition
     */
    public int $leaderEpoch = 0;

    /**
     * Start offset of the share partition
     */
    public int $startOffset = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'    => BinarySchema::TYPE_INT32,
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
            'stateEpoch'   => BinarySchema::TYPE_INT32,
            'leaderEpoch'  => BinarySchema::TYPE_INT32,
            'startOffset'  => BinarySchema::TYPE_INT64,
        ];
    }
}
