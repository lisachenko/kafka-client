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
 * The result of one partition of a DescribeProducers answer (key 61, Kafka 2.8, KIP-664)
 *
 * <pre>
 *   PartitionResponse => PartitionIndex ErrorCode ErrorMessage ActiveProducers
 *     PartitionIndex  => INT32
 *     ErrorCode       => INT16
 *     ErrorMessage    => COMPACT_NULLABLE_STRING
 *     ActiveProducers => COMPACT_ARRAY of {@see ProducerState}
 * </pre>
 *
 * A partition with no producer state answers the error code 0 and an **empty** producer array; a partition this
 * broker does not lead is **3** (`UnknownTopicOrPartition`), because the state lives in the log and only the
 * leader has it.
 *
 * @see docs/protocol/2.8.md, section "DescribeProducers API (key 61, v0)"
 */
class DescribeProducersResponsePartition implements BinarySchemaInterface
{
    /**
     * Index of the partition this result belongs to
     */
    public int $partitionIndex;

    /**
     * Error of this partition, 0 when its producer state could be read
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * Producers this partition still remembers, indexed by the producer id
     *
     * @var array<int, ProducerState>
     */
    public array $activeProducers = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionIndex'  => BinarySchema::TYPE_INT32,
            'errorCode'       => BinarySchema::TYPE_INT16,
            'errorMessage'    => BinarySchema::TYPE_NULLABLE_STRING,
            'activeProducers' => ['producerId' => ProducerState::class],
        ];
    }
}
