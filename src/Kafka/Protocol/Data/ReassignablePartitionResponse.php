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
 * The result of one partition of an AlterPartitionReassignments answer (key 45, Kafka 2.4, KIP-455)
 *
 * <pre>
 *   ReassignablePartitionResponse => PartitionIndex ErrorCode ErrorMessage TAG_BUFFER
 *     PartitionIndex => INT32
 *     ErrorCode      => INT16
 *     ErrorMessage   => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * **The error of this api is per partition**, and the top-level `errorCode` of the answer stays 0 even when every
 * partition of the request was refused: the codes measured on the container are **0** for an accepted (or already
 * satisfied) reassignment, **3** `UnknownTopicOrPartition` (*"The partition does not exist."*) for a topic or a
 * partition the cluster does not have, **39** `InvalidReplicaAssignment` for an empty replica list or a broker
 * that is not alive, and **85** `NoReassignmentInProgress` for a cancellation that had nothing to cancel.
 *
 * @see docs/protocol/2.8.md, section "AlterPartitionReassignments API (key 45, v0)"
 */
class ReassignablePartitionResponse implements BinarySchemaInterface
{
    /**
     * Index of the partition this result belongs to
     */
    public int $partitionIndex;

    /**
     * Error code of this partition, 0 when the controller accepted the reassignment
     */
    public int $errorCode;

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
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
