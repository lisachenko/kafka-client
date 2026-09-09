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
 * @see docs/protocol/0.9.0.md, section "OffsetFetch API (key 9, v0 and v1)"
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
