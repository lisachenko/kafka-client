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
 * One group of an OffsetFetch request of version 8 (Kafka 3.0)
 *
 * <pre>
 *   OffsetFetchRequestGroup => group_id [topics]
 *     group_id => COMPACT_STRING
 *     topics   => name [partition_indexes]    -- NULLABLE
 *       name             => COMPACT_STRING
 *       partition_indexes => partition
 *         partition => INT32
 * </pre>
 *
 * **Version 8 moved the group id and the topic array of the request into an array of these entries**, so that one
 * request can ask for the committed offsets of several groups at once: `OffsetFetchRequest.json` @ 3.0.2 keeps
 * `GroupId` and `Topics` at the versions `0-7` and adds `Groups` at `8+`, with one `RequireStable` for the whole
 * batch behind it. The entry carries the very same two fields the versions below had at the top level, which is
 * why the topic array of this structure is nullable here as well: a `null` - the compact `00` - asks for every
 * topic-partition this group has a committed offset for, an empty array names no topic at all.
 *
 * The field names follow the classes of the line: the key is the `groupId` of every other group DTO of this
 * package, and the topic array keeps the name `topicPartitions` of
 * {@see \Protocol\Kafka\Protocol\Request\OffsetFetchRequest}, whose field it was until version 7.
 *
 * @see docs/protocol/3.9.md, section "OffsetFetch API (key 9, v0 to v8)"
 */
class OffsetFetchRequestGroup implements BinarySchemaInterface
{
    /**
     * Partitions whose offsets are asked for, indexed by the topic they belong to, or null for every topic
     *
     * @var array<string, PartitionsForTopic>|null
     */
    public readonly ?array $topicPartitions;

    /**
     * @param string $groupId The group to fetch the offsets of
     * @param array<string, list<int>|PartitionsForTopic>|null $topicPartitions Partitions to fetch, per topic, or
     *        null to ask for every topic-partition this group has committed an offset for
     */
    public function __construct(
        /**
         * The group to fetch the offsets of
         */
        public readonly string $groupId,
        ?array $topicPartitions = null
    ) {
        if ($topicPartitions === null) {
            $this->topicPartitions = null;

            return;
        }

        $packed = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $packed[$topic] = $partitions instanceof PartitionsForTopic
                ? $partitions
                : new PartitionsForTopic((string) $topic, array_values($partitions));
        }
        $this->topicPartitions = $packed;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'groupId'         => BinarySchema::TYPE_STRING,
            'topicPartitions' => [
                'topic'                      => PartitionsForTopic::class,
                BinarySchema::FLAG_NULLABLE => true,
            ],
        ];
    }
}
