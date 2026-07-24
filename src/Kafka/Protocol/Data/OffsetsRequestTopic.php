<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * OffsetsRequestTopic DTO
 *
 * OffsetsRequestTopic => topic [partitions]
 *   topic => STRING
 *   partitions => partition timestamp
 *     partition => INT32
 *     timestamp => INT64
 */
class OffsetsRequestTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic
     * @var string
     */
    public $topic;

    /**
     * Partitions to list offset.
     *
     * @var OffsetsRequestPartition[]
     */
    public $partitions;

    public function __construct(string $topic, array $partitions)
    {
        $packedPartitions = [];
        $this->topic      = $topic;
        foreach ($partitions as $partition => $timestamp) {
            $packedPartitions[$partition] = new OffsetsRequestPartition($partition, $timestamp);
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
            'partitions' => ['partition' => OffsetsRequestPartition::class],
        ];
    }
}
