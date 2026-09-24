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

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One group of an OffsetFetch answer of version 10 (Kafka 4.2, KIP-848)
 *
 * <pre>
 *   OffsetFetchResponseGroup => group_id [topics] error_code
 *     group_id   => COMPACT_STRING
 *     topics     => OffsetFetchResponseTopic
 *     error_code => INT16
 * </pre>
 *
 * **Version 8 moved the topics and the group-level error code of the answer into an array of these entries**, one
 * per group of the request, and left the answer without a top-level error code altogether:
 * `OffsetFetchResponse.json` @ 3.0.2 keeps `Topics` and `ErrorCode` at the versions `0-7` and gives `Groups` at
 * `8+` a `groupId`, its own `Topics` and its own `ErrorCode`. The layout of a topic and of a partition inside it
 * did not change at all, so the entries are the {@see OffsetFetchResponseTopic} and
 * {@see OffsetFetchResponsePartition} of version 5, `committed_leader_epoch` included.
 *
 * What the error code reports is what the top-level one reported below version 8: the state of the **group** -
 * 15 (`GroupCoordinatorNotAvailable`), 16 (`NotCoordinatorForGroup`), 14 (`GroupLoadInProgress`) or 30
 * (`GroupAuthorizationFailed`) - and an entry that carries one has an empty topics array. A group the coordinator
 * does not know is **not** an error here either: it is answered with an empty topics array and the code 0.
 *
 * **Version 10 (Kafka 4.2, KIP-848) names every topic of the entry by its id**, so its topics are a list of
 * {@see OffsetFetchResponseTopic} entries that carry the `topic_id` and the empty name; {@see self::nameTopics()}
 * maps them back to names and indexes them by name, as the versions below do. A partition of an id the node does not
 * know carries the **100** `UNKNOWN_TOPIC_ID` and the committed offset -1, and a topic of an "every topic" request
 * whose id the node can not find any more is left out of the answer (`KafkaApis.fetchAllOffsetsForGroup` @ 4.3.1).
 * {@see OffsetFetchResponseGroupV8} is the entry of the versions 8 and 9, which names its topics.
 *
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The topic ids of OffsetFetch (v10, KIP-848)"
 */
class OffsetFetchResponseGroup implements BinarySchemaInterface
{
    /**
     * Version of the OffsetFetch API that this DTO is unpacked from
     */
    public const int VERSION = 10;

    /**
     * The group these offsets belong to
     */
    public string $groupId;

    /**
     * Committed offsets of this group, indexed by the topic name - a list in a version 10 answer until
     * {@see self::nameTopics()} names its entries
     *
     * @var array<array-key, OffsetFetchResponseTopic>
     */
    public array $topics = [];

    /**
     * Error of the group itself, which the coordinator reports instead of any topic at all
     */
    public int $errorCode = KafkaException::NO_ERROR;

    /**
     * Builds an entry from the parts an answer below version 8 carries at its top level
     *
     * @param array<string, OffsetFetchResponseTopic> $topics Committed offsets, indexed by the topic name
     */
    public static function of(string $groupId, array $topics, int $errorCode): self
    {
        $group            = new self();
        $group->groupId   = $groupId;
        $group->topics    = $topics;
        $group->errorCode = $errorCode;

        return $group;
    }

    /**
     * Returns the ids of the topics of a version 10 entry that are not named yet
     *
     * @return list<string> The 16 raw bytes of every id
     */
    public function unnamedTopicIds(): array
    {
        $topicIds = [];
        foreach ($this->topics as $topic) {
            if ($topic->topic === '' && !Uuid::isZero($topic->topicId)) {
                $topicIds[] = $topic->topicId;
            }
        }

        return $topicIds;
    }

    /**
     * Names the topics of a version 10 entry and indexes them by name, as the versions below index them
     *
     * An entry of a version below 10 is named and indexed already and is left alone. An id the map does not hold
     * keeps the text form of its uuid as its name, so that no committed offset of the answer goes missing.
     *
     * @param array<string, string> $topicNamesById Name of every topic, as the 16 raw bytes of its uuid => name
     */
    public function nameTopics(array $topicNamesById): void
    {
        if (static::VERSION < 10) {
            return;
        }

        $topics = [];
        foreach ($this->topics as $topic) {
            if ($topic->topic === '') {
                $topic->topic = $topicNamesById[$topic->topicId] ?? Uuid::toString($topic->topicId);
            }
            $topics[$topic->topic] = $topic;
        }
        $this->topics = $topics;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'groupId'   => BinarySchema::TYPE_STRING,
            // From version 10 the entries carry no name, so there is no field to index the array by
            'topics'    => static::VERSION >= 10
                ? [OffsetFetchResponseTopic::class]
                : ['topic' => OffsetFetchResponseTopicV5::class],
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
