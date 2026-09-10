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
 * The entry did not change across the three versions of the api: the topics array of a version 2 answer holds the
 * very same structures, only the group-level error code behind the array is new.
 *
 * @see docs/protocol/1.1.md, section "OffsetFetch API (key 9, v0 to v3)"
 */
class OffsetFetchResponseTopic implements BinarySchemaInterface
{
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
            'partitions' => ['partition' => OffsetFetchResponsePartition::class],
        ];
    }
}
