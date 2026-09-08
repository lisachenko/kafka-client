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
 * One topic of an Offsets (ListOffset) request v0
 *
 * <pre>
 *   OffsetsRequestTopic => TopicName [Partition Time MaxNumberOfOffsets]
 *     TopicName => string
 * </pre>
 *
 * @see docs/protocol/0.9.0.md, section "Offsets API (key 2, v0), a.k.a. ListOffset"
 */
class OffsetsRequestTopic implements BinarySchemaInterface
{
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
     * @param array<int, int> $partitionTimestamps Target time for each partition, indexed by the partition id
     */
    public function __construct(string $topic, array $partitionTimestamps, int $maxNumberOfOffsets = 1)
    {
        $partitions = [];
        foreach ($partitionTimestamps as $partition => $timestamp) {
            $partitions[$partition] = new OffsetsRequestPartition($partition, $timestamp, $maxNumberOfOffsets);
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
            'partitions' => ['partition' => OffsetsRequestPartition::class],
        ];
    }
}
