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
 * The summary of one share partition in a ReadShareGroupStateSummary answer (key 87, v1, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   PartitionResult => Partition ErrorCode ErrorMessage StateEpoch LeaderEpoch StartOffset DeliveryCompleteCount
 *     DeliveryCompleteCount => INT32   -- since version 1 (Kafka 4.2, KIP-1226), "default": -1
 * </pre>
 *
 * `ReadShareGroupStateSummaryResponseData.PartitionResult` @ 4.1.0: the state of the partition without its
 * batches - what DescribeShareGroupOffsets (90) needs to answer the start offset of a share group.
 *
 * **Version 1 (Kafka 4.2, KIP-1226) appended `DeliveryCompleteCount`**: "The number of offsets greater than or equal
 * to share-partition start offset for which delivery has been completed" (`"ignorable": "true"`, `"default": "-1"`),
 * the count of the last WriteShareGroupState v1 of the partition leader. A partition the share coordinator has never
 * seen answers -1 (`PartitionFactory.UNINITIALIZED_DELIVERY_COMPLETE_COUNT` @ 4.3.1), and so does one whose start
 * offset is still uninitialized. {@see ReadShareGroupStateSummaryResponsePartitionV0} is the entry of the version 0,
 * which keeps the -1 of the default.
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupStateSummary API (key 87, v0 and v1)"
 */
class ReadShareGroupStateSummaryResponsePartition implements BinarySchemaInterface
{
    /**
     * Version of the ReadShareGroupStateSummary API that this DTO is unpacked from
     */
    public const int VERSION = 1;

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
     * Number of offsets at or above the start offset whose delivery is complete, -1 when it is not known
     *
     * @since Version 1 of protocol (Kafka 4.2, KIP-1226)
     */
    public int $deliveryCompleteCount = -1;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'partition'    => BinarySchema::TYPE_INT32,
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
            'stateEpoch'   => BinarySchema::TYPE_INT32,
            'leaderEpoch'  => BinarySchema::TYPE_INT32,
            'startOffset'  => BinarySchema::TYPE_INT64,
        ];
        if (static::VERSION >= 1) {
            $scheme['deliveryCompleteCount'] = BinarySchema::TYPE_INT32;
        }

        return $scheme;
    }
}
