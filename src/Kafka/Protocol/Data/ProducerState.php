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
 * One producer that a partition still remembers, in a DescribeProducers answer (key 61, Kafka 2.8, KIP-664)
 *
 * <pre>
 *   ProducerState => ProducerId ProducerEpoch LastSequence LastTimestamp CoordinatorEpoch CurrentTxnStartOffset
 *     ProducerId            => INT64
 *     ProducerEpoch         => INT32
 *     LastSequence          => INT32   (-1 when unknown)
 *     LastTimestamp         => INT64   (-1 when unknown)
 *     CoordinatorEpoch      => INT32
 *     CurrentTxnStartOffset => INT64   (-1 when no transaction of this producer is open here)
 * </pre>
 *
 * This is the **producer state a log keeps for the idempotent producer** made visible: `ProducerStateManager` @
 * 2.8.2 holds the last sequence number and the last timestamp of every producer id that wrote to the partition,
 * which is how a duplicate is recognised, and `CurrentTxnStartOffset` is the first offset of an **open**
 * transaction of that producer in this partition - the offset the last stable offset cannot pass.
 *
 * **The producer epoch is an int32 here** although it is an int16 everywhere else in the protocol - in
 * `ProducerState` of the specification it is written wide, and the broker sends the same value in both places.
 *
 * @see docs/protocol/2.8.md, section "DescribeProducers API (key 61, v0)"
 */
class ProducerState implements BinarySchemaInterface
{
    /**
     * Producer id the partition remembers
     */
    public int $producerId;

    /**
     * Epoch of that producer id
     */
    public int $producerEpoch;

    /**
     * Last sequence number this producer wrote to the partition, -1 when the broker does not know one
     */
    public int $lastSequence = -1;

    /**
     * Timestamp of that write, -1 when unknown
     */
    public int $lastTimestamp = -1;

    /**
     * Epoch of the transaction coordinator that owns the open transaction, -1 without one
     */
    public int $coordinatorEpoch = -1;

    /**
     * First offset of the open transaction of this producer in this partition, -1 when none is open
     */
    public int $currentTxnStartOffset = -1;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'producerId'            => BinarySchema::TYPE_INT64,
            'producerEpoch'         => BinarySchema::TYPE_INT32,
            'lastSequence'          => BinarySchema::TYPE_INT32,
            'lastTimestamp'         => BinarySchema::TYPE_INT64,
            'coordinatorEpoch'      => BinarySchema::TYPE_INT32,
            'currentTxnStartOffset' => BinarySchema::TYPE_INT64,
        ];
    }
}
