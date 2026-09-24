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
 * One partition of a ShareAcknowledge answer (key 79, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   PartitionData => partition_index error_code error_message current_leader
 * </pre>
 *
 * The error of the acknowledgements of the partition: 0, the **121** `InvalidRecordState` of an offset that is not
 * acquired by this member (acknowledged already, released, or never acquired), or the **42** of a batch the broker
 * refuses to read.
 *
 * @see docs/protocol/4.3.md, section "ShareAcknowledge API (key 79, v1 and v2)"
 */
final class ShareAcknowledgeResponsePartition implements BinarySchemaInterface
{
    /**
     * Partition index
     */
    public int $partitionIndex = 0;

    /**
     * Error code of the acknowledgements of this partition
     */
    public int $errorCode = 0;

    /**
     * Error message, null without an error
     */
    public ?string $errorMessage = null;

    /**
     * Current leader of the partition, filled in only with the error codes 6 and 74 (`0 0` otherwise)
     */
    public ShareLeaderIdAndEpoch $currentLeader;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionIndex' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
            'currentLeader'  => ShareLeaderIdAndEpoch::class,
        ];
    }
}
