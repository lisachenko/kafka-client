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
 * OffsetFetchResponseTopic DTO, version 10 of the OffsetFetch API
 *
 * <pre>
 *   OffsetFetchResponseTopic => topic_id [partition_responses]
 *     topic_id            => UUID       -- since version 10, in place of the name
 *     partition_responses => OffsetFetchResponsePartition
 *
 *   OffsetFetchResponseTopic (Version: 0 to 9) => topic [partition_responses]
 *     topic               => STRING
 * </pre>
 *
 * The entry itself did not change across the versions of the api - the topics array of a version 2 answer holds the
 * very same structures, only the group-level error code behind the array is new - but its **partitions** gained
 * the `committed_leader_epoch` of KIP-320 at version 5, so the class of a partition entry follows
 * {@see OffsetFetchResponseTopic::VERSION}, which {@see OffsetFetchResponseTopicV5} and
 * {@see OffsetFetchResponseTopicV0} lower.
 *
 * **Version 10 (Kafka 4.2, KIP-848) names the topic by its id**: `OffsetFetchResponse.json` @ 4.2.0 gives the `Name`
 * of a topic entry of a group the versions `8-9` and the new `TopicId` `10+`. {@see self::$topic} is empty in such an
 * entry until the client maps the id back to a name, see
 * {@see OffsetFetchResponseGroup::nameTopics()}; the partitions are those of version 8.
 *
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The topic ids of OffsetFetch (v10, KIP-848)"
 */
class OffsetFetchResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the OffsetFetch API that this DTO decodes an entry of
     */
    public const int VERSION = 10;

    /**
     * Name of the topic, the empty string in an entry of a version 10 answer until the client names it
     */
    public string $topic = '';

    /**
     * Id of the topic, the 16 raw bytes of its uuid, {@see Uuid::ZERO} below version 10
     *
     * @since Version 10 of protocol (Kafka 4.2, KIP-848)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Committed offset of each partition of this topic
     *
     * @var array<int, OffsetFetchResponsePartition>
     */
    public array $partitions;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        // KIP-848 replaced the name with the id in version 10
        $scheme = static::VERSION >= 10
            ? ['topicId' => BinarySchema::TYPE_UUID]
            : ['topic' => BinarySchema::TYPE_STRING];
        $scheme['partitions'] = ['partition' => static::partitionClass()];

        return $scheme;
    }

    /**
     * Returns the class of a partition entry for the version of the API that this class decodes
     *
     * @return class-string<OffsetFetchResponsePartition>
     */
    protected static function partitionClass(): string
    {
        return static::VERSION >= 5
            ? OffsetFetchResponsePartition::class
            : OffsetFetchResponsePartitionV0::class;
    }
}
