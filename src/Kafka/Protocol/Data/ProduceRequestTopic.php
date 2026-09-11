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
 * Produce request Topic DTO
 *
 * <pre>
 *   TopicName [Partition RecordSetSize RecordSet]
 *     TopicName => string
 * </pre>
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v9)"
 */
class ProduceRequestTopic implements BinarySchemaInterface
{
    /**
     * The name of the topic to produce to
     */
    public string $topic = '';

    /**
     * Data for all partitions of this topic, indexed by the partition number
     *
     * @var array<int, ProduceRequestPartition>
     */
    public array $partitions = [];

    /**
     * @param string                              $topic         Name of the topic
     * @param array<int, ProduceRequestPartition> $partitionData Record sets, indexed by the partition number
     */
    public function __construct(string $topic = '', array $partitionData = [])
    {
        $this->topic      = $topic;
        $this->partitions = $partitionData;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => ProduceRequestPartition::class],
        ];
    }
}
