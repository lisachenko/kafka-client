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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Common\Errors\UnknownTopicIdException;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV0;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV1;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV2;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV6;

/**
 * OffsetCommit, version 10: the offsets are stored in the `__consumer_offsets` topic of the cluster.
 *
 * This api saves out the consumer's position in the stream for one or more partitions. In the scala API this happens
 * when the consumer calls commit() or in the background if "autocommit" is enabled. This is the position the consumer
 * will pick up from if it crashes before its next commit().
 *
 * <pre>
 *   OffsetCommit Request (Version: 10) => group_id generation_id member_id group_instance_id [topics]
 *     topics => topic_id [partitions]      -- the topic id of KIP-848 in place of the name
 *       topic_id => UUID
 *
 *   OffsetCommit Request (Version: 7 to 9) => group_id generation_id member_id group_instance_id [topics]
 *     group_id          => STRING
 *     generation_id     => INT32
 *     member_id         => STRING
 *     group_instance_id => NULLABLE_STRING   -- since version 7
 *     topics            => topic [partitions]
 *       topic      => STRING
 *       partitions => partition offset leader_epoch metadata
 *         partition    => INT32
 *         offset       => INT64
 *         leader_epoch => INT32            -- since version 6
 *         metadata     => NULLABLE_STRING
 *
 *   OffsetCommit Request (Version: 2, 3 and 4) => group_id generation_id member_id retention_time [topics]
 *     retention_time => INT64              -- version 2 to 4 only
 * </pre>
 *
 * Version 2 replaced the per-partition `timestamp` of version 1 with one `retention_time` for the whole request
 * (`OFFSET_COMMIT_REQUEST_V2` in `Protocol.java` @ 0.11.0.3). With {@see self::DEFAULT_RETENTION_TIME} the broker
 * keeps the offsets for `offsets.retention.minutes`, otherwise for the given number of milliseconds counted from
 * the moment it received the commit, see `KafkaApis.handleOffsetCommitRequest`. A 2.8.2 broker still honours that
 * field at the versions 2 to 4 and writes the offset with the `__consumer_offsets` value schema **v1**, the only
 * one that has an `expire_timestamp`; a commit that leaves it at -1 is written with the value schema **v3**, which
 * has none at all (KIP-211).
 *
 * Version 3 (KIP-124, Kafka 0.11) left the request alone - `OFFSET_COMMIT_REQUEST_V3 = OFFSET_COMMIT_REQUEST_V2` -
 * and only added the leading `throttle_time_ms` to the answer ({@see OffsetCommitResponse}).
 *
 * Version 4 (KIP-219, Kafka 2.0) left it alone once more: `OffsetCommitRequest.json` @ 2.8.2 gives every field of
 * this frame the same `versions` for 2, 3 and 4, and the first change afterwards is the **removal** of
 * `retention_time` at version 5 (KIP-211, Kafka 2.1). What the higher number means is the throttling contract of
 * KIP-219: a broker that throttles a version 4 request sends the answer **first** and mutes the channel for the
 * delay afterwards, so a client that sends this version has to wait out `throttle_time_ms` itself. Version 4 is the
 * highest **non-flexible** version of the api - the versions 5 to 8 belong to the later releases of the 2.x major
 * and the version 9 to Kafka 3.6.
 *
 * The lower versions differ in their scheme, and a scheme is a static property of a class, so each of them has a
 * class of its own that only lowers {@see OffsetCommitRequest::VERSION}: {@see OffsetCommitRequestV3},
 * {@see OffsetCommitRequestV2}, {@see OffsetCommitRequestV1} and {@see OffsetCommitRequestV0}. Everything else -
 * the fields, the class names and the way the topic-partitions are packed - is shared.
 *
 * **Version 5 (Kafka 2.1, KIP-211) removes `retention_time` from the frame** - the field has the versions `2-4` in
 * `OffsetCommitRequest.json` @ 2.8.2, it is not sent as -1 - because the committed offsets of a group expire
 * `offsets.retention.minutes` after the **group** became empty from that release on, not a fixed time after each
 * commit. The `$retentionTime` a caller passes is therefore simply not written by the versions 5 and 6.
 *
 * **Version 6 (Kafka 2.1, KIP-320) gives every partition a `committed_leader_epoch`**, between the offset and the
 * metadata: the epoch of the leader the offset was read from, so that a consumer that resumes from it can be told
 * that the log was truncated behind its back (74 `FencedLeaderEpoch`, 75 `UnknownLeaderEpoch`). A client that does
 * not know the epoch sends {@see OffsetCommitRequestPartition::UNKNOWN_LEADER_EPOCH}, which is what an
 * {@see OffsetAndMetadata} without a `leaderEpoch` produces.
 *
 * **Version 7 (Kafka 2.3, KIP-345) inserted the nullable `group_instance_id` behind the member id**, so that a
 * *static* member commits under the identity its `group.instance.id` gives it; the coordinator refuses the commit
 * of a member whose instance id has been taken over by another consumer with 82 (`FencedInstanceId`). A dynamic
 * member sends `null` here, which is the frame of {@see OffsetCommitRequestV6} with one more field.
 *
 * **Version 9 (Kafka 3.6, KIP-848) is the version 8 frame with another number in its header** - "Version 9 is the
 * first version that can be used with the new consumer group protocol (KIP-848). The request is the same as
 * version 8" in `OffsetCommitRequest.json` @ 3.6.2 - and {@see OffsetCommitRequestV9} is that version, with
 * {@see OffsetCommitRequestV8} below it. What the number buys is a promise about the *answer*:
 * a commit of a group the coordinator does not know is refused **69** `GroupIdNotFound` instead of the **22**
 * `IllegalGeneration` the versions below it are answered, and a member of a KIP-848 group may be told **113**
 * `StaleMemberEpoch` - the code the same release added - when the epoch it commits with is behind the one the
 * coordinator holds. The same release renamed the second field of the frame from `generation_id` to
 * `generation_id_or_member_epoch`, "the generation of the group if using the generic group protocol or the member
 * epoch if using the consumer protocol": the same four bytes with a second meaning, so {@see self::$generationId}
 * keeps its published name and a classic member goes on writing its generation into it.
 *
 * **Version 10 (Kafka 4.2, KIP-848) names every topic by its id**, and this class is version 10:
 * `OffsetCommitRequest.json` @ 4.2.0, "Version 10 adds support for topic ids and removes support for topic names
 * (KIP-848)" - the `Name` of a topic entry is `"versions": "0-9"`, the new `TopicId` `"10+"`; Kafka 4.1 declared the
 * version as `latestVersionUnstable`, Kafka 4.2 made it stable. So a client can not commit for a topic whose id it
 * does not know: the `$topicIds` of the constructor are where it states them
 * ({@see \Protocol\Kafka\Common\Cluster::topicIdsOf()} is where it learns them), and a version 10 request without
 * the id of one of its topics is refused before it is built, with {@see UnknownTopicIdException}. The node answers a
 * topic id it does not know with the **100** `UNKNOWN_TOPIC_ID` on every partition of it. The group, the member and
 * the partitions did not change, and a classic group accepts version 10 as well as a KIP-848 one.
 * {@see OffsetCommitRequestV9} keeps the version that names the topics.
 *
 * @see docs/protocol/4.3.md, section "The member epoch of KIP-848 (v9)"
 * @see docs/protocol/4.3.md, section "OffsetCommit API (key 8, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The topic ids of OffsetCommit (v10, KIP-848)"
 */
class OffsetCommitRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::OFFSET_COMMIT;

    /**
     * Generation id for a consumer that is not a member of a group.
     *
     * A consumer that joined a group through the JoinGroup api (key 11, Kafka 0.9) has to commit with the generation
     * the coordinator assigned to it, otherwise the commit is refused with the error code 22 (IllegalGeneration).
     */
    public const int DEFAULT_GENERATION_ID = -1;

    /**
     * Consumer id of a consumer that is not a member of a group: empty, and never null.
     */
    public const string DEFAULT_MEMBER_NAME = '';

    /**
     * Asks the broker to keep the offsets for `offsets.retention.minutes` instead of a retention of its own.
     *
     * @since Version 2 of protocol
     */
    public const int DEFAULT_RETENTION_TIME = -1;

    /**
     * @inheritdoc
     */
    public const int VERSION = 10;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 8;

    /**
     * The first version that names a topic by its id, and by nothing else (Kafka 4.2, KIP-848)
     */
    public const int MIN_TOPIC_ID_VERSION = 10;

    /**
     * Offsets to commit, indexed by the topic they belong to - a list from version 10 on, whose entries carry no
     * name on the wire
     *
     * @var array<array-key, OffsetCommitRequestTopic>
     */
    protected readonly array $topicPartitions;

    /**
     * Id of every topic this request names, as topic name => the 16 raw bytes of its uuid (KIP-848)
     *
     * Version 10 names every topic by its id and by nothing else; every version below it ignores the map.
     *
     * @var array<string, string>
     */
    protected readonly array $topicIds;

    /**
     * A value of the `$topicPartitions` map is either a plain offset, an {@see OffsetAndMetadata} or an already
     * built {@see OffsetCommitRequestPartition}.
     *
     * @param string $consumerGroup   The consumer group id
     * @param int    $generationId    The generation of the group, {@see self::DEFAULT_GENERATION_ID} without one
     * @param string $memberName      The member id assigned by the coordinator, empty without one
     * @param int    $retentionTime   How long to keep the offsets, {@see self::DEFAULT_RETENTION_TIME} for the
     *                                retention configured on the broker
     * @param array<string, array<int, int|OffsetAndMetadata|OffsetCommitRequestPartition>> $topicPartitions Offsets
     * @param string $clientId        Unique client identifier
     * @param int    $correlationId   Correlated request id
     * @param string|null $groupInstanceId `group.instance.id` of a static member (KIP-345), null for a dynamic one
     * @param array<string, string> $topicIds Id of every topic, as name => the 16 raw bytes of its uuid
     *
     * @throws UnknownTopicIdException If a version 10 request names a topic whose id the caller did not state
     */
    public function __construct(
        /**
         * The consumer group id.
         */
        protected readonly string $consumerGroup,
        /**
         * The generation of the group, or the member epoch of a member of a KIP-848 group.
         *
         * The field is called `generation_id_or_member_epoch` from Kafka 3.6 on - the release that made the
         * version 9 of this api usable with the new consumer group protocol - and carries the generation of the
         * group for every classic member, which is what this client is.
         *
         * @since Version 1 of protocol
         */
        protected readonly int $generationId,
        /**
         * The member id assigned by the group coordinator.
         *
         * @since Version 1 of protocol
         */
        protected readonly string $memberName,
        /**
         * Time period in ms to retain the offset.
         *
         * @since Version 2 of protocol
         */
        protected readonly int $retentionTime,
        array $topicPartitions,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Unique identifier of this consumer instance, `group.instance.id`, null for a dynamic member.
         *
         * @since Version 7 of protocol
         */
        protected readonly ?string $groupInstanceId = null,
        /**
         * Id of every topic named above, as name => the 16 raw bytes of its uuid; **version 10 needs one per topic**
         * (KIP-848) and throws {@see UnknownTopicIdException} without it, every lower version ignores the map.
         */
        array $topicIds = []
    ) {
        $this->topicIds        = $topicIds;
        $topicClass            = static::topicClass();
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $topic = $partitions instanceof OffsetCommitRequestTopic ? $partitions->topic : (string) $topic;
            if (static::VERSION >= self::MIN_TOPIC_ID_VERSION) {
                // A version 10 entry carries no name at all, so the list it travels in is the only honest shape
                $packedTopicPartitions[] = new $topicClass(
                    $topic,
                    $partitions instanceof OffsetCommitRequestTopic ? $partitions->partitions : $partitions,
                    self::idOf($topicIds, $topic, $partitions)
                );
                continue;
            }
            $packedTopicPartitions[$topic] = $partitions instanceof OffsetCommitRequestTopic
                ? $partitions
                : new $topicClass($topic, $partitions);
        }
        $this->topicPartitions = $packedTopicPartitions;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'consumerGroup' => BinarySchema::TYPE_STRING,
        ];
        if (static::VERSION >= 1) {
            $body['generationId'] = BinarySchema::TYPE_INT32;
            $body['memberName']   = BinarySchema::TYPE_STRING;
        }
        if (static::VERSION >= 7) {
            $body['groupInstanceId'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        if (static::VERSION >= 2 && static::VERSION <= 4) {
            $body['retentionTime'] = BinarySchema::TYPE_INT64;
        }
        // From version 10 the entries carry no name, so there is no field to index the array by
        $body['topicPartitions'] = static::VERSION >= self::MIN_TOPIC_ID_VERSION
            ? [static::topicClass()]
            : ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the id of every topic this request names, as topic name => the 16 raw bytes of its uuid
     *
     * @return array<string, string>
     */
    public function getTopicIds(): array
    {
        return $this->topicIds;
    }

    /**
     * Returns the id of a topic from the map of the caller, or from a ready-made entry that carries one
     *
     * A version below 10 names its topics by name and never looks at the map; a version 10 frame can not name a
     * topic at all without its id, and a client that does not know it refreshes its metadata instead of guessing.
     *
     * @param array<string, string> $topicIds Id of every topic, as name => the 16 raw bytes of its uuid
     * @param mixed                 $entry    What the caller gave for the topic
     *
     * @throws UnknownTopicIdException If the id of the topic is not known
     */
    private static function idOf(array $topicIds, string $topic, mixed $entry): string
    {
        $topicId = $topicIds[$topic] ?? Uuid::ZERO;
        if (Uuid::isZero($topicId) && $entry instanceof OffsetCommitRequestTopic) {
            $topicId = $entry->topicId;
        }
        if (Uuid::isZero($topicId)) {
            throw new UnknownTopicIdException(
                [
                    'error' => 'An OffsetCommit request of version 10 names its topics by id (KIP-848), and this'
                        . ' client does not know the id of this one',
                    'topic' => $topic,
                ]
            );
        }

        return $topicId;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class sends
     *
     * @return class-string<OffsetCommitRequestTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 10 => OffsetCommitRequestTopic::class,
            static::VERSION >= 6  => OffsetCommitRequestTopicV6::class,
            static::VERSION >= 2  => OffsetCommitRequestTopicV2::class,
            static::VERSION === 1 => OffsetCommitRequestTopicV1::class,
            default               => OffsetCommitRequestTopicV0::class,
        };
    }
}
