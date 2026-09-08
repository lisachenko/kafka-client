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
 * One topic of an Offsets (ListOffset) response v0
 *
 * <pre>
 *   OffsetsResponseTopic => TopicName [Partition ErrorCode [Offset]]
 *     TopicName => string
 * </pre>
 *
 * @see docs/protocol/0.9.0.md, section "Offsets API (key 2, v0), a.k.a. ListOffset"
 */
class OffsetsResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic that the offsets were requested for
     */
    public string $topic;

    /**
     * Offsets for each of the requested partitions, indexed by the partition id
     *
     * @var array<int, OffsetsResponsePartition>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => OffsetsResponsePartition::class],
        ];
    }
}
