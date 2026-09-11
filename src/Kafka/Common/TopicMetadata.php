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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Common;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Topic metadata DTO
 *
 * <pre>
 *   TopicMetadata => TopicErrorCode TopicName IsInternal [PartitionMetadata]
 *     TopicErrorCode => int16
 *     TopicName      => string
 *     IsInternal     => boolean
 * </pre>
 *
 * `TOPIC_METADATA_V1` in `MetadataResponse.java` @ 1.1.1: version 1 of the Metadata API (Kafka 0.10.0) inserted the
 * `IsInternal` flag between the topic name and its partitions, which {@see TopicMetadataV0} still lacks. A topic is
 * internal when Kafka itself keeps it - `Topic.isInternal` @ 1.1.1 knows two, `__consumer_offsets`, the log the
 * group coordinator stores the committed offsets in, and `__transaction_state`.
 *
 * The topic entry itself never changed again; what a version selects from here on is the shape of its partition
 * entries, which is what {@see self::partitionClass()} picks: version 5 of the api (Kafka 1.0, KIP-112/113)
 * appended `OfflineReplicas` to them, the versions 1 to 4 ({@see TopicMetadataV1}) do not carry it.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v11)"
 */
class TopicMetadata implements BinarySchemaInterface
{
    use RestorableTrait;

    /**
     * Version of the Metadata API that this entry is unpacked from
     */
    public const int VERSION = 10;

    /**
     * The error code for the given topic.
     *
     * A topic that was just auto-created is announced with error code 5 (LeaderNotAvailable) and an empty partition
     * list until the controller has elected the partition leaders.
     */
    public int $topicErrorCode = 0;

    /**
     * The name of the topic
     */
    public string $topic = '';

    /**
     * Id of the topic, the 16 raw bytes of the `uuid` of KIP-516 (Kafka 2.8).
     *
     * An id is given to a topic when it is created and it does **not** survive a delete: a topic that is deleted
     * and created again under the same name gets a new one, which is the whole point of KIP-516 - a broker that
     * missed the deletion can tell the two apart, while the name alone cannot. {@see Uuid} formats the bytes the
     * way a broker and `kafka-topics.sh --describe` print them, and {@see Uuid::ZERO} is both "this answer has
     * no id for the topic" and what every version below 10 leaves here.
     *
     * @since Version 10 of protocol (Kafka 2.8, KIP-516)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Whether the topic is considered a Kafka internal topic, null when the answer was a version 0 one.
     *
     * @since Version 1 of protocol
     */
    public ?bool $isInternal = null;

    /**
     * Metadata for each partition of the topic, indexed by the partition id.
     *
     * @var array<int, PartitionMetadata>
     */
    public array $partitions = [];

    /**
     * Operations the principal of this connection is authorized for on this topic, as the bitfield of KIP-430.
     *
     * {@see AclOperation} is both halves of the field: the codes and the two helpers that pack and unpack the
     * bitfield. {@see AclOperation::NOT_REQUESTED} (`Integer.MIN_VALUE`) is what a broker writes when the request
     * did not set `include_topic_authorized_operations`, and what every answer below version 8 leaves here - "you
     * did not ask", which is not the same as the bitfield 0, "you may do nothing".
     *
     * @since Version 8 of protocol (Kafka 2.3, KIP-430)
     */
    public int $authorizedOperations = AclOperation::NOT_REQUESTED;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'topicErrorCode' => BinarySchema::TYPE_INT16,
            'topic'          => BinarySchema::TYPE_STRING,
        ];
        // The topic id of KIP-516 sits between the name and `is_internal`, which is the field order of
        // `MetadataResponse.json` @ 2.8.2 and therefore the wire order
        if (static::VERSION >= 10) {
            $scheme['topicId'] = BinarySchema::TYPE_UUID;
        }
        if (static::VERSION >= 1) {
            $scheme['isInternal'] = BinarySchema::TYPE_BOOLEAN;
        }
        // A broker does not promise any ordering for the partitions, so they are indexed by their id: the
        // cluster looks a partition up by number, see Cluster::partition() and Cluster::leaderFor()
        $scheme['partitions'] = ['partitionId' => static::partitionClass()];
        if (static::VERSION >= 8) {
            $scheme['authorizedOperations'] = BinarySchema::TYPE_INT32;
        }

        return $scheme;
    }

    /**
     * Returns the class of a partition entry for the version of the API that this entry belongs to
     *
     * @return class-string<PartitionMetadata>
     */
    protected static function partitionClass(): string
    {
        return match (true) {
            static::VERSION >= 7 => PartitionMetadata::class,
            static::VERSION >= 5 => PartitionMetadataV5::class,
            default              => PartitionMetadataV0::class,
        };
    }
}
