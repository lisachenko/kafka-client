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
 * One topic of an OffsetForLeaderEpoch answer, version 1 (key 23)
 *
 * <pre>
 *   OffsetForLeaderEpochResponseTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => OffsetForLeaderEpochResponsePartition
 * </pre>
 *
 * The topic entry itself never changed; what the version constant selects is the shape of its partition entries -
 * a version 0 answer carries no `leader_epoch` ({@see OffsetForLeaderEpochResponseTopicV0}), a version 1 answer
 * does, see {@see self::partitionClass()}.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 and v1)"
 */
class OffsetForLeaderEpochResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the OffsetForLeaderEpoch API that this DTO is unpacked from
     */
    public const int VERSION = 1;

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
            'partitions' => ['partition' => static::partitionClass()],
        ];
    }

    /**
     * Returns the class of a partition entry for the version of the API that this DTO belongs to
     *
     * @return class-string<OffsetForLeaderEpochResponsePartition>
     */
    protected static function partitionClass(): string
    {
        return static::VERSION >= 1
            ? OffsetForLeaderEpochResponsePartition::class
            : OffsetForLeaderEpochResponsePartitionV0::class;
    }
}
