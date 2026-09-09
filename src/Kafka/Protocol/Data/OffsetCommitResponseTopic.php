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
 * OffsetCommitResponseTopic DTO
 *
 * <pre>
 *   OffsetCommitResponseTopic => topic [partition_responses]
 *     topic               => STRING
 *     partition_responses => OffsetCommitResponsePartition
 * </pre>
 *
 * @see docs/protocol/0.11.0.md, section "OffsetCommit API (key 8, v0 to v3)"
 */
class OffsetCommitResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic
     */
    public string $topic;

    /**
     * Result of committing the offset of each partition of this topic
     *
     * @var array<int, OffsetCommitResponsePartition>
     */
    public array $partitions;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => OffsetCommitResponsePartition::class],
        ];
    }
}
