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

use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One topic of a group entry of an OffsetFetch request of version 10 (Kafka 4.2, KIP-848): the topic named by its id
 *
 * <pre>
 *   OffsetFetchRequestTopics => topic_id [partition_indexes]
 *     topic_id          => UUID
 *     partition_indexes => INT32
 * </pre>
 *
 * The `OffsetFetchRequestTopics` of `OffsetFetchRequest.json` @ 4.2.0, the topic entry of the `Groups` array of
 * version 8: its `Name` has the versions `8-9`, and version 10 - "adds support for topic ids and removes support for
 * topic names (KIP-848)" - put the `TopicId` in its place. The versions 8 and 9 write this entry as a
 * {@see PartitionsForTopic}, the name and the partitions of every topic entry of the versions below; this class is
 * the entry of version 10 alone. {@see self::$topic} keeps the name for the client and never reaches the wire.
 *
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The topic ids of OffsetFetch (v10, KIP-848)"
 */
final class OffsetFetchRequestTopics implements BinarySchemaInterface
{
    /**
     * @param string    $topic      Name of the topic, kept for the client and never on the wire
     * @param string    $topicId    Id of the topic, the 16 raw bytes of its uuid
     * @param list<int> $partitions Partition indexes to fetch the committed offsets of
     */
    public function __construct(
        public string $topic = '',
        public string $topicId = Uuid::ZERO,
        public array $partitions = []
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicId'    => BinarySchema::TYPE_UUID,
            'partitions' => [BinarySchema::TYPE_INT32],
        ];
    }
}
