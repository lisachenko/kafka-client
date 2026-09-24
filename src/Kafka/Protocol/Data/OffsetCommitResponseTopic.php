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
 * OffsetCommitResponseTopic DTO, version 10 of the OffsetCommit API
 *
 * <pre>
 *   OffsetCommitResponseTopic => topic_id [partition_responses]
 *     topic_id            => UUID       -- since version 10, in place of the name
 *     partition_responses => OffsetCommitResponsePartition
 *
 *   OffsetCommitResponseTopic (Version: 0 to 9) => topic [partition_responses]
 *     topic               => STRING
 * </pre>
 *
 * `OffsetCommitResponse.json` @ 4.2.0 declares `Name` as `"versions": "0-9"` and `TopicId` as `"10+"`: an answer of
 * version 10 (Kafka 4.2, KIP-848) names every topic by the id the request named it with, and a partition of an id
 * the node does not know carries the **100** `UNKNOWN_TOPIC_ID`. {@see self::$topic} is empty in such an entry until
 * the client maps the id back to the name it committed for; {@see OffsetCommitResponseTopicV0} is the entry of the
 * versions 0 to 9.
 *
 * @see docs/protocol/4.3.md, section "OffsetCommit API (key 8, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The topic ids of OffsetCommit (v10, KIP-848)"
 */
class OffsetCommitResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the OffsetCommit API that this DTO is unpacked from
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
     * Result of committing the offset of each partition of this topic
     *
     * @var array<int, OffsetCommitResponsePartition>
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
        $scheme['partitions'] = ['partition' => OffsetCommitResponsePartition::class];

        return $scheme;
    }
}
