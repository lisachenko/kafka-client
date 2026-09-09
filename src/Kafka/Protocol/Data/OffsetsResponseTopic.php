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
 * One topic of an Offsets (ListOffset) response, version 1
 *
 * <pre>
 *   OffsetsResponseTopic => topic [partition_responses]
 *     topic => STRING
 * </pre>
 *
 * The topic entry itself is the same in both versions of the answer; only the layout of a partition entry changes,
 * so the class of the entries is derived from {@see OffsetsResponseTopic::VERSION}, which
 * {@see OffsetsResponseTopicV0} lowers.
 *
 * @see docs/protocol/0.11.0.md, section "Offsets API (key 2, v0 and v1), a.k.a. ListOffset"
 */
class OffsetsResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the Offsets API that this DTO is unpacked from
     */
    public const int VERSION = 1;

    /**
     * Name of the topic that the offsets were requested for
     */
    public string $topic;

    /**
     * Offsets for each of the requested partitions, indexed by the partition id
     *
     * @var array<int, OffsetsResponsePartition>
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
     * Returns the class of a partition entry for the version of the API that this class unpacks
     *
     * @return class-string<OffsetsResponsePartition>
     */
    protected static function partitionClass(): string
    {
        return static::VERSION >= 1 ? OffsetsResponsePartition::class : OffsetsResponsePartitionV0::class;
    }
}
