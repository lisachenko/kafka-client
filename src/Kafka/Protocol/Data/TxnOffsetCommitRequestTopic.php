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
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One topic of a TxnOffsetCommit request, i.e. one entry of the `topics` array, version 6 of the api
 *
 * <pre>
 *   TxnOffsetCommitRequestTopic => topic_id [partitions]
 *     topic_id   => UUID                    -- since version 6, in place of the name
 *     partitions => TxnOffsetCommitRequestPartition
 *
 *   TxnOffsetCommitRequestTopic (Version: 0 to 5) => topic [partitions]
 *     topic      => STRING
 * </pre>
 *
 * The layout of a partition entry changed with the version 2 (KIP-320), so the class of the entries is derived from
 * {@see TxnOffsetCommitRequestTopic::VERSION}, which {@see TxnOffsetCommitRequestTopicV2} and
 * {@see TxnOffsetCommitRequestTopicV0} lower.
 *
 * **Version 6 (Kafka 4.4, KIP-1319) names the topic by its id**: `TxnOffsetCommitRequest.json` @ 4.4.0 declares
 * `Name` as `"versions": "0-5"` and the new `TopicId` as `"6+"` - "Version 6 adds support for topic IDs and removes
 * support for topic names" - so an entry of this class carries the 16 raw bytes of the uuid and no name on the wire.
 * {@see self::$topic} still holds the name the client committed for, which is how the answer is read back;
 * {@see TxnOffsetCommitRequestTopicV2} is the entry of the versions 2 to 5, which names the topic.
 *
 * @see docs/protocol/4.4.md, section "TxnOffsetCommit API (key 28, v0 to v6)"
 * @see docs/protocol/4.4.md, section "What Kafka 4.4 changed here (KIP-1319): the version 6"
 */
class TxnOffsetCommitRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the TxnOffsetCommit API that this DTO belongs to
     */
    public const int VERSION = 6;

    /**
     * Name of the topic whose offsets are committed
     *
     * The field is on the wire in the versions 0 to 5 only; an entry of version 6 keeps it for the client.
     */
    public string $topic = '';

    /**
     * Id of the topic, the 16 raw bytes of its uuid, {@see Uuid::ZERO} below version 6
     *
     * @since Version 6 of protocol (Kafka 4.4, KIP-1319)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Offsets of every partition of this topic, indexed by the partition id
     *
     * @var array<int, TxnOffsetCommitRequestPartition>
     */
    public array $partitions;

    /**
     * A plain integer or an {@see OffsetAndMetadata} becomes a partition DTO, an already built one is kept
     *
     * @param string $topic Name of the topic
     * @param array<int, int|OffsetAndMetadata|TxnOffsetCommitRequestPartition> $partitionOffsets Offset of every
     *        partition, indexed by the partition id
     * @param string $topicId Id of the topic, the 16 raw bytes of its uuid; what version 6 sends
     */
    public function __construct(string $topic, array $partitionOffsets, string $topicId = Uuid::ZERO)
    {
        $partitionClass = static::partitionClass();
        $partitions     = [];
        foreach ($partitionOffsets as $partition => $offset) {
            $partitions[$partition] = $offset instanceof TxnOffsetCommitRequestPartition
                ? $offset
                : new $partitionClass((int) $partition, $offset);
        }

        $this->topic      = $topic;
        $this->topicId    = $topicId;
        $this->partitions = $partitions;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        // KIP-1319 replaced the name with the id in version 6; the two never travel together
        $scheme = static::VERSION >= 6
            ? ['topicId' => BinarySchema::TYPE_UUID]
            : ['topic' => BinarySchema::TYPE_STRING];
        $scheme['partitions'] = ['partition' => static::partitionClass()];

        return $scheme;
    }

    /**
     * Returns the class of a partition entry for the version of the API that this DTO belongs to
     *
     * @return class-string<TxnOffsetCommitRequestPartition>
     */
    protected static function partitionClass(): string
    {
        return static::VERSION >= 2
            ? TxnOffsetCommitRequestPartition::class
            : TxnOffsetCommitRequestPartitionV0::class;
    }
}
