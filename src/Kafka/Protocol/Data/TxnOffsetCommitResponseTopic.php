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
 * The result of committing the offsets of one topic inside a transaction, i.e. one entry of the `topics` array,
 * version 6 of the TxnOffsetCommit API
 *
 * <pre>
 *   TxnOffsetCommitResponseTopic => topic_id [partitions]
 *     topic_id   => UUID                   -- since version 6, in place of the name
 *     partitions => TxnOffsetCommitResponsePartition
 *
 *   TxnOffsetCommitResponseTopic (Version: 0 to 5) => topic [partitions]
 *     topic      => STRING
 * </pre>
 *
 * `TxnOffsetCommitResponse.json` @ 4.4.0 declares `Name` as `"versions": "0-5"` and `TopicId` as `"6+"` - "Version 6
 * adds support for topic IDs and removes support for topic names" (KIP-1319) - so an answer of version 6 names every
 * topic by the id the request named it with, and a partition of an id the node does not know carries the **100**
 * `UNKNOWN_TOPIC_ID`. {@see self::$topic} is empty in such an entry until the client maps the id back to the name it
 * committed for ({@see \Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponse::topicsByName()});
 * {@see TxnOffsetCommitResponseTopicV0} is the entry of the versions 0 to 5.
 *
 * @see docs/protocol/4.4.md, section "TxnOffsetCommit API (key 28, v0 to v6)"
 * @see docs/protocol/4.4.md, section "What Kafka 4.4 changed here (KIP-1319): the version 6"
 */
class TxnOffsetCommitResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the TxnOffsetCommit API that this DTO is unpacked from
     */
    public const int VERSION = 6;

    /**
     * Name of the topic this entry belongs to, the empty string in an entry of a version 6 answer until the client
     * names it
     */
    public string $topic = '';

    /**
     * Id of the topic, the 16 raw bytes of its uuid, {@see Uuid::ZERO} below version 6
     *
     * @since Version 6 of protocol (Kafka 4.4, KIP-1319)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Result of every committed partition of this topic, indexed by the partition id
     *
     * @var array<int, TxnOffsetCommitResponsePartition>
     */
    public array $partitions;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        // KIP-1319 replaced the name with the id in version 6; the two never travel together
        $scheme = static::VERSION >= 6
            ? ['topicId' => BinarySchema::TYPE_UUID]
            : ['topic' => BinarySchema::TYPE_STRING];
        $scheme['partitions'] = ['partition' => TxnOffsetCommitResponsePartition::class];

        return $scheme;
    }
}
