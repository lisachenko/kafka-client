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
 * One topic of a Fetch request
 *
 * <pre>
 *   FetchRequestTopic => TopicName [Partition FetchOffset LogStartOffset MaxBytes]
 *     TopicName => string
 * </pre>
 *
 * The topic entry itself never changed; what a version selects is the shape of its partition entries, which is
 * what the version constant of this DTO picks in {@see self::partitionClass()}, see {@see FetchRequestTopicV0}.
 *
 * @see docs/protocol/1.1.md, section "Fetch API (key 1, v0 to v5)"
 */
class FetchRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO is packed for
     */
    public const int VERSION = 5;

    /**
     * Name of the topic to fetch from
     */
    public string $topic;

    /**
     * Partitions of this topic to fetch from, indexed by the partition id
     *
     * @var array<int, FetchRequestTopicPartition>
     */
    public array $partitions;

    /**
     * @param array<int, FetchRequestTopicPartition> $partitions Partitions to fetch from, indexed by partition id
     */
    public function __construct(string $topic, array $partitions = [])
    {
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
     * Returns the class of a partition entry for the version of the API that this DTO belongs to
     *
     * @return class-string<FetchRequestTopicPartition>
     */
    public static function partitionClass(): string
    {
        return static::VERSION >= 5 ? FetchRequestTopicPartition::class : FetchRequestTopicPartitionV0::class;
    }
}
