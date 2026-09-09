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
 *   TopicName [Partition ErrorCode Offset LogAppendTime]
 *     TopicName => string
 * </pre>
 *
 * The topic entry itself never changed; what a version selects is the shape of its partition entries - a version 0
 * or 1 answer carries no `LogAppendTime` ({@see ProduceResponseTopicV0}) - which is what the version constant of
 * this DTO picks in {@see self::partitionClass()}. Version 3 changed nothing about the answer at all.
 *
 * @see docs/protocol/0.11.0.md, section "Produce API (key 0, v0 to v3)"
 */
class ProduceResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the Produce API that this DTO is unpacked from
     */
    public const int VERSION = 2;

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
        return static::VERSION >= 2 ? ProduceResponsePartition::class : ProduceResponsePartitionV0::class;
    }
}
