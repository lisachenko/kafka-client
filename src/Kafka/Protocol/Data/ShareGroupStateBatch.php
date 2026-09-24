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
 * One batch of the delivery state of a share partition (keys 84 and 85, v0, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   StateBatch => FirstOffset LastOffset DeliveryState DeliveryCount
 *     FirstOffset   => INT64
 *     LastOffset    => INT64
 *     DeliveryState => INT8     (0 Available, 2 Acknowledged, 4 Archived)
 *     DeliveryCount => INT16
 * </pre>
 *
 * `ReadShareGroupStateResponseData.StateBatch` and `WriteShareGroupStateRequestData.StateBatch` @ 4.1.0, two
 * generated classes with the same four fields: the records of a share partition from one offset to another that
 * share a delivery state and a delivery count. This package carries both as this one structure.
 *
 * @see docs/protocol/4.3.md, section "The share-group state apis (keys 83 to 87) — wire only"
 */
class ShareGroupStateBatch implements BinarySchemaInterface
{
    /**
     * First offset of this state batch
     */
    public int $firstOffset = 0;

    /**
     * Last offset of this state batch
     */
    public int $lastOffset = 0;

    /**
     * Delivery state of its records: 0 available, 2 acknowledged, 4 archived
     */
    public int $deliveryState = 0;

    /**
     * How many times its records were delivered
     */
    public int $deliveryCount = 0;

    /**
     * @param int $firstOffset   First offset of this state batch
     * @param int $lastOffset    Last offset of this state batch
     * @param int $deliveryState Delivery state of its records: 0 available, 2 acknowledged, 4 archived
     * @param int $deliveryCount How many times its records were delivered
     */
    public function __construct(
        int $firstOffset = 0,
        int $lastOffset = 0,
        int $deliveryState = 0,
        int $deliveryCount = 0
    ) {
        $this->firstOffset = $firstOffset;
        $this->lastOffset = $lastOffset;
        $this->deliveryState = $deliveryState;
        $this->deliveryCount = $deliveryCount;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'firstOffset'   => BinarySchema::TYPE_INT64,
            'lastOffset'    => BinarySchema::TYPE_INT64,
            'deliveryState' => BinarySchema::TYPE_INT8,
            'deliveryCount' => BinarySchema::TYPE_INT16,
        ];
    }
}
