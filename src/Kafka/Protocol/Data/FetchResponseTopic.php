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
 * One topic of a Fetch response v0
 *
 * <pre>
 *   FetchResponseTopic => TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]
 *     TopicName => string
 * </pre>
 *
 * @see docs/protocol/0.8.2.md, section "Fetch API (key 1, v0)"
 */
class FetchResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic that was fetched from
     */
    public string $topic;

    /**
     * Fetch result for each of the requested partitions, indexed by the partition id
     *
     * @var array<int, FetchResponsePartition>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => FetchResponsePartition::class],
        ];
    }
}
