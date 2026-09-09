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
 * One topic of an Offsets (ListOffset) request, version 1
 *
 * <pre>
 *   OffsetsRequestTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => OffsetsRequestPartition
 * </pre>
 *
 * The topic entry itself is the same in both versions of the request; only the layout of a partition entry changes,
 * so the class of the entries is derived from {@see OffsetsRequestTopic::VERSION}, which
 * {@see OffsetsRequestTopicV0} lowers.
 *
 * @see docs/protocol/0.10.2.md, section "Offsets API (key 2, v0 and v1), a.k.a. ListOffset"
 */
class OffsetsRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the Offsets API that this DTO is packed for
     */
    public const int VERSION = 1;

    /**
     * Name of the topic to list the offsets of
     */
    public string $topic;

    /**
     * Partitions of this topic to list the offsets of, indexed by the partition id
     *
     * @var array<int, OffsetsRequestPartition>
     */
    public array $partitions;

    /**
     * A plain integer value is the target time of the partition, an already built partition DTO is taken as it is.
     *
     * @param string $topic Name of the topic
     * @param array<int, int|OffsetsRequestPartition> $partitionTimestamps Target time for each partition, indexed
     *                                                                    by the partition id
     * @param int    $maxNumberOfOffsets Offsets to return per partition, version 0 of the api only
     */
    public function __construct(string $topic, array $partitionTimestamps, int $maxNumberOfOffsets = 1)
    {
        $partitionClass = static::partitionClass();
        $partitions     = [];
        foreach ($partitionTimestamps as $partition => $timestamp) {
            $partitions[$partition] = $timestamp instanceof OffsetsRequestPartition
                ? $timestamp
                : new $partitionClass((int) $partition, $timestamp, $maxNumberOfOffsets);
        }

        $this->topic      = $topic;
        $this->partitions = $partitions;
    }

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
     * Returns the class of a partition entry for the version of the API that this class packs
     *
     * @return class-string<OffsetsRequestPartition>
     */
    protected static function partitionClass(): string
    {
        return static::VERSION >= 1 ? OffsetsRequestPartition::class : OffsetsRequestPartitionV0::class;
    }
}
