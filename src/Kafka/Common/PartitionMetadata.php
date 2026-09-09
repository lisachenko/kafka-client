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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Common;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Information about a topic-partition metadata.
 *
 * <pre>
 *   PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
 *     PartitionErrorCode => int16
 *     PartitionId        => int32
 *     Leader             => int32
 *     Replicas           => [int32]
 *     Isr                => [int32]
 * </pre>
 *
 * @see docs/protocol/0.11.0.md, section "Metadata API (key 3, v0 to v4)"
 */
class PartitionMetadata implements BinarySchemaInterface
{
    use RestorableTrait;

    /**
     * The error code for the partition, if any.
     */
    public int $partitionErrorCode = 0;

    /**
     * The id of the partition.
     */
    public int $partitionId = 0;

    /**
     * The id of the broker acting as leader for this partition, `-1` while a leader election is in progress.
     */
    public int $leader = -1;

    /**
     * The set of all nodes that host this partition.
     *
     * @var list<int>
     */
    public array $replicas = [];

    /**
     * The set of nodes that are in sync with the leader for this partition.
     *
     * @var list<int>
     */
    public array $isr = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionErrorCode' => BinarySchema::TYPE_INT16,
            'partitionId'        => BinarySchema::TYPE_INT32,
            'leader'             => BinarySchema::TYPE_INT32,
            'replicas'           => [BinarySchema::TYPE_INT32],
            'isr'                => [BinarySchema::TYPE_INT32],
        ];
    }
}
