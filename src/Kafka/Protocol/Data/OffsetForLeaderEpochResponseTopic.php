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
 * One topic of an OffsetForLeaderEpoch answer, version 0 (key 23)
 *
 * <pre>
 *   OffsetForLeaderEpochResponseTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => OffsetForLeaderEpochResponsePartition
 * </pre>
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0)"
 */
class OffsetForLeaderEpochResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic
     */
    public string $topic;

    /**
     * Answer for every requested partition of this topic, indexed by the partition id
     *
     * @var array<int, OffsetForLeaderEpochResponsePartition>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => OffsetForLeaderEpochResponsePartition::class],
        ];
    }
}
