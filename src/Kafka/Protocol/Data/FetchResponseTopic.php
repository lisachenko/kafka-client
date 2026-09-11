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
 * One topic of a Fetch response
 *
 * <pre>
 *   FetchResponseTopic => TopicName [Partition ErrorCode HighwaterMarkOffset LastStableOffset LogStartOffset
 *                                    [AbortedTransactions] RecordSetSize RecordSet]
 *     TopicName => string
 * </pre>
 *
 * The topic entry itself never changed; what a version selects is the shape of its partition entries, which is
 * what the version constant of this DTO picks in {@see self::partitionClass()}, see {@see FetchResponseTopicV4}
 * and {@see FetchResponseTopicV0}.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v11)"
 */
class FetchResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO is unpacked from
     */
    public const int VERSION = 11;

    /**
     * Name of the topic that was fetched from
     */
    public string $topic;

    /**
     * Fetch result for each of the requested partitions, indexed by the partition id
     *
     * @var array<int, FetchResponsePartition>
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
     * @return class-string<FetchResponsePartition>
     */
    protected static function partitionClass(): string
    {
        return match (true) {
            static::VERSION >= 11 => FetchResponsePartition::class,
            static::VERSION >= 5  => FetchResponsePartitionV5::class,
            static::VERSION >= 4  => FetchResponsePartitionV4::class,
            default               => FetchResponsePartitionV0::class,
        };
    }
}
