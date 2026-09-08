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
 * One topic of a Fetch request v0
 *
 * <pre>
 *   FetchRequestTopic => TopicName [Partition FetchOffset MaxBytes]
 *     TopicName => string
 * </pre>
 *
 * @see docs/protocol/0.9.0.md, section "Fetch API (key 1, v0 and v1)"
 */
class FetchRequestTopic implements BinarySchemaInterface
{
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
            'partitions' => ['partition' => FetchRequestTopicPartition::class],
        ];
    }
}
