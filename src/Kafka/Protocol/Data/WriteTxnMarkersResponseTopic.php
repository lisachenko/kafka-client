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
 * The result of writing the markers of one topic, i.e. one entry of the `topics` array of a marker
 *
 * <pre>
 *   WriteTxnMarkersResponseTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => WriteTxnMarkersResponsePartition
 * </pre>
 *
 * @see docs/protocol/1.1.md, section "WriteTxnMarkers API (key 27, v0)"
 */
class WriteTxnMarkersResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic this entry belongs to
     */
    public string $topic;

    /**
     * Result of every requested partition of this topic, indexed by the partition id
     *
     * @var array<int, WriteTxnMarkersResponsePartition>
     */
    public array $partitions;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => WriteTxnMarkersResponsePartition::class],
        ];
    }
}
