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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Produce response Topic DTO
 *
 * <pre>
 *   TopicName [Partition ErrorCode Offset LogAppendTime LogStartOffset]
 *     TopicName => string
 * </pre>
 *
 * The topic entry itself never changed; what a version selects is the shape of its partition entries - a version 0
 * or 1 answer carries no `LogAppendTime` ({@see ProduceResponseTopicV0}), a version 2, 3 or 4 answer no
 * `LogStartOffset` ({@see ProduceResponseTopicV2}) - which is what the version constant of this DTO picks in
 * {@see self::partitionClass()}. The versions 3 and 4 changed nothing about the answer at all; version 5 (Kafka
 * 1.0) appended the `LogStartOffset` to every partition entry.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v7)"
 */
class ProduceResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the Produce API that this DTO is unpacked from
     */
    public const int VERSION = 5;

    /**
     * The name of the topic
     */
    public string $topic = '';

    /**
     * Result for all partitions of this topic, indexed by the partition number
     *
     * @var array<int, ProduceResponsePartition>
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
     * @return class-string<ProduceResponsePartition>
     */
    protected static function partitionClass(): string
    {
        return match (true) {
            static::VERSION >= 5 => ProduceResponsePartition::class,
            static::VERSION >= 2 => ProduceResponsePartitionV2::class,
            default              => ProduceResponsePartitionV0::class,
        };
    }
}
