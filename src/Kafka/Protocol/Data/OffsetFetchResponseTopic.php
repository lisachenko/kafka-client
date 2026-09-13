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
 * OffsetFetchResponseTopic DTO
 *
 * <pre>
 *   OffsetFetchResponseTopic => topic [partition_responses]
 *     topic               => STRING
 *     partition_responses => OffsetFetchResponsePartition
 * </pre>
 *
 * The entry itself did not change across the versions of the api - the topics array of a version 2 answer holds the
 * very same structures, only the group-level error code behind the array is new - but its **partitions** gained
 * the `committed_leader_epoch` of KIP-320 at version 5, so the class of a partition entry follows
 * {@see OffsetFetchResponseTopic::VERSION}, which {@see OffsetFetchResponseTopicV0} lowers.
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v7)"
 */
class OffsetFetchResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the OffsetFetch API that this DTO decodes an entry of
     */
    public const int VERSION = 5;

    /**
     * Name of the topic
     */
    public string $topic;

    /**
     * Committed offset of each partition of this topic
     *
     * @var array<int, OffsetFetchResponsePartition>
     */
    public array $partitions;

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
     * Returns the class of a partition entry for the version of the API that this class decodes
     *
     * @return class-string<OffsetFetchResponsePartition>
     */
    protected static function partitionClass(): string
    {
        return static::VERSION >= 5
            ? OffsetFetchResponsePartition::class
            : OffsetFetchResponsePartitionV0::class;
    }
}
