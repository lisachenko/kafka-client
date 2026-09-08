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

use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * OffsetCommitRequestTopic DTO, version 1 of the OffsetCommit API
 *
 * <pre>
 *   OffsetCommitRequestTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => OffsetCommitRequestPartition
 * </pre>
 *
 * The layout of a partition entry depends on the version of the request, so the class of the entries is derived from
 * {@see OffsetCommitRequestTopic::VERSION}, which {@see OffsetCommitRequestTopicV0} lowers to 0.
 *
 * @see docs/protocol/0.9.0.md, section "OffsetCommit API (key 8, v0 and v1)"
 */
class OffsetCommitRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the OffsetCommit API that this DTO is packed for
     */
    public const int VERSION = 1;

    /**
     * Name of the topic
     */
    public string $topic;

    /**
     * Offsets to commit, indexed by the partition they belong to.
     *
     * @var array<int, OffsetCommitRequestPartition>
     */
    public array $partitions;

    /**
     * A plain integer value is the offset alone, an {@see OffsetAndMetadata} carries the metadata that the broker
     * should keep next to it, and an already built partition DTO is taken as it is.
     *
     * @param string $topic      Name of the topic
     * @param array<int, int|OffsetAndMetadata|OffsetCommitRequestPartition> $partitions Offset for each partition
     */
    public function __construct(string $topic, array $partitions)
    {
        $this->topic      = $topic;
        $partitionClass   = static::partitionClass();
        $packedPartitions = [];

        foreach ($partitions as $partition => $offset) {
            $packedPartitions[$partition] = match (true) {
                $offset instanceof OffsetCommitRequestPartition => $offset,
                $offset instanceof OffsetAndMetadata => new $partitionClass(
                    (int) $partition,
                    $offset->offset,
                    $offset->metadata
                ),
                default => new $partitionClass((int) $partition, $offset),
            };
        }
        $this->partitions = $packedPartitions;
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
     * @return class-string<OffsetCommitRequestPartition>
     */
    protected static function partitionClass(): string
    {
        return static::VERSION >= 1 ? OffsetCommitRequestPartition::class : OffsetCommitRequestPartitionV0::class;
    }
}
