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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Produce response Topic DTO
 *
 * <pre>
 *   TopicName [Partition ErrorCode Offset]
 *     TopicName => string
 * </pre>
 *
 * @see docs/protocol/0.9.0.md, section "Produce API (key 0, v0 and v1)"
 */
class ProduceResponseTopic implements BinarySchemaInterface
{
    /**
     * The name of the topic
     */
    public string $topic = '';

    /**
     * Result for all partitions of this topic, indexed by the partition number
     *
     * @var array<int, ProduceResponsePartition>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => ProduceResponsePartition::class],
        ];
    }
}
