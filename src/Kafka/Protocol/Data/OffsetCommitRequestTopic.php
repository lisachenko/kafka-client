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
 * OffsetCommitRequestTopic DTO, version 10 of the OffsetCommit API
 *
 * <pre>
 *   OffsetCommitRequestTopic => topic_id [partitions]
 *     topic_id   => UUID                    -- since version 10, in place of the name
 *     partitions => OffsetCommitRequestPartition
 *
 *   OffsetCommitRequestTopic (Version: 0 to 9) => topic [partitions]
 *     topic      => STRING
 * </pre>
 *
 * The layout of a partition entry changes with the versions of the request, so the class of the entries is derived
 * from {@see OffsetCommitRequestTopic::VERSION}, which {@see OffsetCommitRequestTopicV6},
 * {@see OffsetCommitRequestTopicV2}, {@see OffsetCommitRequestTopicV1} and {@see OffsetCommitRequestTopicV0} lower.
 *
 * **Version 10 (Kafka 4.2, KIP-848) names the topic by its id**: `OffsetCommitRequest.json` @ 4.2.0 declares `Name`
 * as `"versions": "0-9"` and the new `TopicId` as `"10+"` - "Version 10 adds support for topic ids and removes
 * support for topic names (KIP-848)" - so an entry of this class carries the 16 raw bytes of the uuid and no name on
 * the wire. {@see self::$topic} still holds the name the client committed for, which is how the answer is read back;
 * {@see OffsetCommitRequestTopicV6} is the entry of the versions 6 to 9, which names the topic.
 *
 * @see docs/protocol/4.3.md, section "OffsetCommit API (key 8, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The topic ids of OffsetCommit (v10, KIP-848)"
 */
class OffsetCommitRequestTopic implements BinarySchemaInterface
{
    /**
     * Version of the OffsetCommit API that this DTO is packed for
     */
    public const int VERSION = 10;

    /**
     * Name of the topic
     *
     * The field is on the wire in the versions 0 to 9 only; an entry of version 10 keeps it for the client.
     */
    public string $topic = '';

    /**
     * Id of the topic, the 16 raw bytes of its uuid, {@see Uuid::ZERO} below version 10
     *
     * @since Version 10 of protocol (Kafka 4.2, KIP-848)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Offsets to commit, indexed by the partition they belong to.
     *
     * @var array<int, OffsetCommitRequestPartition>
     */
    public array $partitions;

    /**
     * A plain integer value is the offset alone, an {@see OffsetAndMetadata} carries the metadata that the broker
     * should keep next to it, and an already built partition DTO is taken as it is.
     *
     * @param string $topic      Name of the topic
     * @param array<int, int|OffsetAndMetadata|OffsetCommitRequestPartition> $partitions Offset for each partition
     * @param string $topicId    Id of the topic, the 16 raw bytes of its uuid; what version 10 sends
     */
    public function __construct(string $topic, array $partitions, string $topicId = Uuid::ZERO)
    {
        $this->topic      = $topic;
        $this->topicId    = $topicId;
        $partitionClass   = static::partitionClass();
        $packedPartitions = [];

        foreach ($partitions as $partition => $offset) {
            $packedPartitions[$partition] = match (true) {
                $offset instanceof OffsetCommitRequestPartition => $offset,
                $offset instanceof OffsetAndMetadata => new $partitionClass(
                    (int) $partition,
                    $offset->offset,
                    $offset->metadata,
                    OffsetCommitRequestPartition::BROKER_TIMESTAMP,
                    $offset->leaderEpoch ?? OffsetCommitRequestPartition::UNKNOWN_LEADER_EPOCH
                ),
                default => new $partitionClass((int) $partition, $offset),
            };
        }
        $this->partitions = $packedPartitions;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        // KIP-848 replaced the name with the id in version 10; the two never travel together
        $scheme = static::VERSION >= 10
            ? ['topicId' => BinarySchema::TYPE_UUID]
            : ['topic' => BinarySchema::TYPE_STRING];
        $scheme['partitions'] = ['partition' => static::partitionClass()];

        return $scheme;
    }

    /**
     * Returns the class of a partition entry for the version of the API that this class packs
     *
     * @return class-string<OffsetCommitRequestPartition>
     */
    protected static function partitionClass(): string
    {
        return match (true) {
            static::VERSION >= 6  => OffsetCommitRequestPartition::class,
            static::VERSION >= 2  => OffsetCommitRequestPartitionV2::class,
            static::VERSION === 1 => OffsetCommitRequestPartitionV1::class,
            default               => OffsetCommitRequestPartitionV0::class,
        };
    }
}
