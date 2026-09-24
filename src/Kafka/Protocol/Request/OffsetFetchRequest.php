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

use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\UnknownTopicIdException;
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetFetchRequestGroup;
use Protocol\Kafka\Protocol\Data\OffsetFetchRequestGroupV8;
use Protocol\Kafka\Protocol\Data\OffsetFetchRequestGroupV9;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;

/**
 * OffsetFetch, version 10: the offsets that consumer groups committed, read from `__consumer_offsets`
 *
 * This API reads back the offsets that were committed for a consumer group with the OffsetCommit API, so it has to
 * be sent to the coordinator of that group.
 *
 * <pre>
 *   OffsetFetch Request (Version: 2 to 7) => group_id [topics] require_stable
 *     group_id       => STRING
 *     topics         => topic [partitions]     -- NULLABLE since version 2
 *       topic      => STRING
 *       partitions => partition
 *         partition => INT32
 *     require_stable => BOOLEAN            -- since version 7
 *
 *   OffsetFetch Request (Version: 8)      => [groups] require_stable
 *     groups         => group_id [topics]  -- since version 8, in place of the two top-level fields
 *     require_stable => BOOLEAN            -- one flag for the whole batch
 *
 *   OffsetFetch Request (Version: 9)      => [groups] require_stable
 *     groups         => group_id member_id member_epoch [topics]   -- the two member fields since version 9
 *
 *   OffsetFetch Request (Version: 10)     => [groups] require_stable
 *     groups         => group_id member_id member_epoch [topics]
 *       topics => topic_id [partitions]    -- the topic id of KIP-848 in place of the name
 * </pre>
 *
 * Version 2 (KIP-88, Kafka 0.10.2) made the topic array **nullable**, and that is the only change of the request:
 * a `null` array - `ff ff ff ff` on the wire - asks the coordinator for every topic-partition the group has a
 * committed offset for, which is what an administrative tool needs and what {@see self::forAllTopics()} builds. An
 * **empty** array - `00 00 00 00` - is a different request that names no topic at all and is answered with an empty
 * response; the two must not be confused.
 *
 * Version 3 (KIP-124, Kafka 0.11) left the request untouched - `OFFSET_FETCH_REQUEST_V3 = OFFSET_FETCH_REQUEST_V2`
 * in `Protocol.java` @ 0.11.0.3 - and only added the leading `throttle_time_ms` to the answer, so
 * {@see OffsetFetchRequestV2} puts the same bytes on the wire and reads its answer with
 * {@see OffsetFetchResponseV2}.
 *
 * Version 4 (KIP-219, Kafka 2.0) changed neither half of the api: `OffsetFetchRequest.json` @ 2.8.2 introduces
 * nothing between the nullable topic array of version 2 and the `require_stable` of version 7, and the answer only
 * gains the `committed_leader_epoch` of version 5 (KIP-320, Kafka 2.1). Sending version 4 promises that this client
 * honours the `throttle_time_ms` of the answer itself, because a throttled 2.x broker answers **first** and mutes
 * the channel afterwards.
 *
 * **Version 5 (Kafka 2.1, KIP-320) changed the ANSWER alone**: every partition of it gained a
 * `committed_leader_epoch` behind the committed offset, the epoch an OffsetCommit v6 stored with it. The request
 * is byte for byte the one of version 2, so {@see OffsetFetchRequestV4} and {@see OffsetFetchRequestV3} send the
 * same body one and two api versions lower and only read their answers with the matching response class.
 *
 * **Version 7 (KIP-447, Kafka 2.5) appended the boolean `require_stable`** behind the topic array, the first
 * field the request gained since version 2. A `true` asks the coordinator to answer a partition whose last
 * offset commit belongs to a transaction that has **not been committed yet** with the retriable error code
 * **88** (`UnstableOffsetCommit`) instead of that offset, so that a consumer of a read-committed pipeline never
 * reads an offset the transaction may still roll back. `false` - the default, and the only behaviour of every
 * version below - answers the offset of the last commit whatever its transaction is doing.
 * {@see OffsetFetchRequestV6} is the same frame without the flag.
 *
 * **Version 8 (Kafka 3.0) asks for several groups at once.** `OffsetFetchRequest.json` @ 3.0.2 - *"Version 8 is
 * adding support for fetching offsets for multiple groups at a time"* - ends `GroupId` and `Topics` at the version
 * 7 and puts an array of {@see OffsetFetchRequestGroup} in their place, each entry with a group id and a topic
 * array of its own, while the single `require_stable` behind the array holds for the whole batch. The published
 * constructor keeps naming one group and sends it as a one-element batch; {@see self::forGroups()} builds the real
 * batch, and {@see OffsetFetchRequestV7} is the same lookup with one group at the top level.
 *
 * **An empty batch is refused here.** A 3.9.2 node answers `groups = []` with **nothing at all** - the handler
 * dies in `NoSuchElementException: key not found: null` and the connection is left without the answer it owes, so
 * every later request on it waits forever - and this client therefore never puts such a frame on the wire, see
 * {@see self::forGroups()}.
 *
 * **Version 9 (Kafka 3.7, KIP-848) named the member inside every group entry.**
 * `OffsetFetchRequest.json` @ 3.7.2 - *"Version 9 is the first version that can be used with the new consumer
 * group protocol (KIP-848). It adds the MemberId and MemberEpoch fields. Those are filled in and validated when
 * the new consumer protocol is used."* - puts a nullable `MemberId` (default `null`) and a `MemberEpoch` (int32,
 * default `-1`) behind the group id of every {@see OffsetFetchRequestGroup}, before its topic array. A classic
 * member and every administrative reader leave the two fields at their defaults; {@see self::forMember()} fills
 * them for a member of a KIP-848 group, which is answered the 25 `UnknownMemberId` or the 113 `StaleMemberEpoch`
 * when they do not match the coordinator. {@see OffsetFetchRequestV8} is the same batch without the two fields.
 *
 * **Version 10 (Kafka 4.2, KIP-848) names every topic of a group entry by its id**, and this class is version 10:
 * `OffsetFetchRequest.json` @ 4.2.0, "Version 10 adds support for topic ids and removes support for topic names
 * (KIP-848)" - the `Name` of a topic entry is `"versions": "8-9"`, the new `TopicId` `"10+"`; Kafka 4.1 declared
 * the version as `latestVersionUnstable`, Kafka 4.2 made it stable. The `$topicIds` of the constructor and of
 * {@see self::forGroups()} and {@see self::forMember()} are where a caller states the ids
 * ({@see \Protocol\Kafka\Common\Cluster::topicIdsOf()} is where it learns them), and a version 10 request that
 * names a topic without its id is refused before it is built, with {@see UnknownTopicIdException}. A group entry
 * that asks for **every** topic (`null`) needs no id at all, and its answer names every topic by id. The node
 * answers an id it does not know with the partition-level **100** `UNKNOWN_TOPIC_ID` and the committed offset -1.
 * {@see OffsetFetchRequestV9} keeps the version that names the topics.
 *
 * Versions 0 and 1 have no nullable array ({@see OffsetFetchRequestV1}, {@see OffsetFetchRequestV0}) and are
 * identical to each other on the wire: they only differ in where the broker reads the offsets from - ZooKeeper for
 * version 0, the `__consumer_offsets` topic of the cluster for version 1 and above. Asking those versions for all
 * topics is refused here with an {@see UnsupportedVersionException}, exactly as `OffsetFetchRequest.Builder.build()`
 * @ 0.11.0.3 does; sending a `-1` topic array with version 1 makes the broker close the connection.
 *
 * @see docs/protocol/4.3.md, sections "OffsetFetch API (key 9, v0 to v10)" and "Stable offsets and the 88 of
 *      KIP-447 (Kafka 2.5)"
 * @see docs/protocol/4.3.md, section "The member id and epoch of KIP-848 (v9)"
 * @see docs/protocol/4.3.md, section "The topic ids of OffsetFetch (v10, KIP-848)"
 */
class OffsetFetchRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::OFFSET_FETCH;

    /**
     * @inheritdoc
     */
    public const int VERSION = 10;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 6;

    /**
     * The first version that asks for several groups in one request (Kafka 3.0)
     */
    public const int MIN_BATCHED_VERSION = 8;

    /**
     * The first version whose group entry names the member id and the member epoch of KIP-848 (Kafka 3.7)
     */
    public const int MIN_MEMBER_VERSION = 9;

    /**
     * The first version whose group entry names a topic by its id, and by nothing else (Kafka 4.2, KIP-848)
     */
    public const int MIN_TOPIC_ID_VERSION = 10;

    /**
     * Partitions whose offsets are requested, indexed by the topic they belong to, or null for every topic
     *
     * Only the versions below 8 carry it; a batched request names the topics of each group of its batch instead.
     *
     * @var array<string, PartitionsForTopic>|null
     */
    protected readonly ?array $topicPartitions;

    /**
     * The groups whose offsets are requested, indexed by the group id
     *
     * @since Version 8 of protocol
     *
     * @var array<string, OffsetFetchRequestGroup>
     */
    protected readonly array $groups;

    /**
     * @param string $consumerGroup   Name of the consumer group
     * @param array<string, list<int>|PartitionsForTopic>|null $topicPartitions Partitions to fetch, per topic, or
     *        null to ask for every topic-partition the group has committed an offset for (version 2 and above)
     * @param string $clientId        Unique client identifier
     * @param int    $correlationId   Correlated request id
     * @param array<string, OffsetFetchRequestGroup>|null $groups The batch of version 8; null - the default -
     *        asks for `$consumerGroup` alone, see {@see self::forGroups()}
     * @param array<string, string> $topicIds Id of every topic named, as name => the 16 raw bytes of its uuid;
     *        **version 10 needs one per named topic** (KIP-848), every lower version ignores the map
     *
     * @throws UnknownTopicIdException If a version 10 request names a topic whose id the caller did not state
     */
    public function __construct(
        protected readonly string $consumerGroup,
        ?array $topicPartitions,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Whether the coordinator has to hold back an offset whose transaction has not been committed yet.
         *
         * `false` - the default and every version below 7 - answers the offset of the last commit, committed or
         * not; `true` (KIP-447, Kafka 2.5) makes the coordinator answer the partition with the **retriable** error
         * code 88 (`UnstableOffsetCommit`) instead, until the transaction that wrote the pending offset ends. One
         * flag holds for every group of a version 8 batch.
         *
         * @since Version 7 of protocol
         */
        protected readonly bool $requireStable = false,
        ?array $groups = null,
        array $topicIds = []
    ) {
        if ($topicPartitions === null) {
            if (static::VERSION < 2) {
                throw new UnsupportedVersionException(
                    [
                        'error'   => sprintf(
                            'The version %d of the OffsetFetch api can not ask for every topic of a group, '
                            . 'the nullable topic array arrived with the version 2 in Kafka 0.10.2',
                            static::VERSION
                        ),
                        'groupId' => $consumerGroup,
                    ]
                );
            }
            $this->topicPartitions = null;
        } else {
            $packedTopicPartitions = [];
            foreach ($topicPartitions as $topic => $partitions) {
                $packedTopicPartitions[$topic] = $partitions instanceof PartitionsForTopic
                    ? $partitions
                    : new PartitionsForTopic((string) $topic, array_values($partitions));
            }
            $this->topicPartitions = $packedTopicPartitions;
        }

        $packedGroups = [];
        foreach ($groups ?? [] as $groupId => $group) {
            $packedGroups[(string) $groupId] = static::packGroup((string) $groupId, $group, $topicIds);
        }
        $this->groups = $groups === null
            ? [$consumerGroup => static::packGroup($consumerGroup, $this->topicPartitions, $topicIds)]
            : $packedGroups;
        if (static::VERSION >= self::MIN_TOPIC_ID_VERSION) {
            self::assertTopicIds($this->groups);
        }

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds the request that asks for every topic-partition the group has a committed offset for (version 2)
     *
     * `OffsetFetchRequest.forAllPartitions()` @ 0.10.2.2 is the same shortcut.
     */
    public static function forAllTopics(
        string $consumerGroup,
        string $clientId = '',
        int $correlationId = 0,
        bool $requireStable = false
    ): static {
        return new static($consumerGroup, null, $clientId, $correlationId, $requireStable);
    }

    /**
     * Builds the batched request of version 8 (Kafka 3.0): the committed offsets of several groups at once
     *
     * The batch is a map of the group id to the partitions that group is asked for - a `null` value asks for every
     * topic-partition that group has a committed offset for, an empty array names no topic at all, exactly as the
     * single-group request does. The answer carries one entry per group, each with its own error code, see
     * {@see OffsetFetchResponse::groupOf()}.
     *
     * **An empty batch is refused.** A 3.9.2 node answers a `groups = []` frame with nothing at all - the request
     * dies in `NoSuchElementException: key not found: null` inside the broker and the connection is left owing an
     * answer that never comes, which strands every later request on it - so this client never sends one.
     *
     * @param array<string, array<string, list<int>|PartitionsForTopic>|null|OffsetFetchRequestGroup>
     *        $groupTopicPartitions Partitions to fetch per topic, per group; a `null` value asks for every topic
     *        of that group, and a ready-made {@see OffsetFetchRequestGroup} carries the member id and the member
     *        epoch of KIP-848 with it (version 9)
     * @param string $clientId      Unique client identifier
     * @param int    $correlationId Correlated request id
     * @param bool   $requireStable Whether the coordinator has to hold back the offsets of an open transaction,
     *        for every group of the batch (KIP-447)
     * @param array<string, string> $topicIds Id of every topic named, as name => the 16 raw bytes of its uuid, for
     *        version 10 (KIP-848)
     *
     * @throws InvalidRequestException If the batch is empty
     * @throws UnsupportedVersionException If a version below 8 is asked for more than one group
     * @throws UnknownTopicIdException If a version 10 request names a topic whose id the caller did not state
     */
    public static function forGroups(
        array $groupTopicPartitions,
        string $clientId = '',
        int $correlationId = 0,
        bool $requireStable = false,
        array $topicIds = []
    ): static {
        if ($groupTopicPartitions === []) {
            throw new InvalidRequestException(
                [
                    'error' => 'An OffsetFetch request has to name at least one group: a broker of Kafka 3.9.2 '
                        . 'answers an empty `groups` array with nothing at all and strands the connection',
                ]
            );
        }
        if (static::VERSION < self::MIN_BATCHED_VERSION && count($groupTopicPartitions) > 1) {
            throw new UnsupportedVersionException(
                [
                    'error' => sprintf(
                        'The version %d of the OffsetFetch api asks for one group per request, the `groups` '
                        . 'array arrived with the version %d in Kafka 3.0',
                        static::VERSION,
                        self::MIN_BATCHED_VERSION
                    ),
                    'groups' => implode(', ', array_keys($groupTopicPartitions)),
                ]
            );
        }

        $groups = [];
        foreach ($groupTopicPartitions as $groupId => $topicPartitions) {
            $groups[(string) $groupId] = static::packGroup((string) $groupId, $topicPartitions, $topicIds);
        }
        $firstGroup = array_key_first($groups);

        return new static(
            (string) $firstGroup,
            $groups[$firstGroup]->topicPartitions,
            $clientId,
            $correlationId,
            $requireStable,
            $groups,
            $topicIds
        );
    }

    /**
     * Builds the request of a member of a KIP-848 group, which names itself in its group entry (version 9)
     *
     * `OffsetFetchRequest.json` @ 3.7.2 added the nullable `MemberId` and the `MemberEpoch` to every entry of the
     * batch, "filled in and validated when the new consumer protocol is used": the coordinator looks the member
     * up in the group and answers the group-level **25** `UnknownMemberId` for an id it does not hold and the
     * **113** `StaleMemberEpoch` for an epoch that is not the one it holds, above and below it alike. A **classic**
     * group ignores both fields, and an entry that leaves them at `null` / `-1` is accepted by either kind of group
     * without a member lookup at all.
     *
     * @param string $groupId     The KIP-848 group to read the offsets of
     * @param array<string, list<int>|PartitionsForTopic>|null $topicPartitions Partitions to fetch, per topic, or
     *        null to ask for every topic-partition that group has committed an offset for
     * @param string $memberId    The member id the coordinator assigned to this member
     * @param int    $memberEpoch The member epoch the coordinator last answered this member with
     * @param string $clientId      Unique client identifier
     * @param int    $correlationId Correlated request id
     * @param bool   $requireStable Whether the coordinator has to hold back the offsets of an open transaction
     * @param array<string, string> $topicIds Id of every topic named, as name => the 16 raw bytes of its uuid, for
     *        version 10 (KIP-848)
     *
     * @throws UnsupportedVersionException If the version of this class has no group array at all
     * @throws UnknownTopicIdException If a version 10 request names a topic whose id the caller did not state
     */
    public static function forMember(
        string $groupId,
        ?array $topicPartitions,
        string $memberId,
        int $memberEpoch,
        string $clientId = '',
        int $correlationId = 0,
        bool $requireStable = false,
        array $topicIds = []
    ): static {
        if (static::VERSION < self::MIN_MEMBER_VERSION) {
            throw new UnsupportedVersionException(
                [
                    'error' => sprintf(
                        'The version %d of the OffsetFetch api can not name a member: the member id and the '
                        . 'member epoch of KIP-848 arrived with the version %d in Kafka 3.7',
                        static::VERSION,
                        self::MIN_MEMBER_VERSION
                    ),
                    'groupId' => $groupId,
                ]
            );
        }

        return static::forGroups(
            [$groupId => new OffsetFetchRequestGroup($groupId, $topicPartitions, $memberId, $memberEpoch)],
            $clientId,
            $correlationId,
            $requireStable,
            $topicIds
        );
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];

        if (static::VERSION < self::MIN_BATCHED_VERSION) {
            $topicPartitions = ['topic' => PartitionsForTopic::class];
            if (static::VERSION >= 2) {
                $topicPartitions[BinarySchema::FLAG_NULLABLE] = true;
            }

            $body['consumerGroup']   = BinarySchema::TYPE_STRING;
            $body['topicPartitions'] = $topicPartitions;
        } else {
            $body['groups'] = ['groupId' => static::groupClass()];
        }

        if (static::VERSION >= 7) {
            $body['requireStable'] = BinarySchema::TYPE_BOOLEAN;
        }

        return $header + $body;
    }

    /**
     * Builds the entry of one group in the class the version of this request declares
     *
     * A ready-made entry is rebuilt in that class, so that a batch whose entries name a member of KIP-848 can be
     * sent at a version that has no member fields: the two fields are simply not written then. The ids of the
     * request complete the ids a ready-made entry states itself (version 10).
     *
     * @param array<string, list<int>|PartitionsForTopic>|null|OffsetFetchRequestGroup $topicPartitions
     * @param array<string, string>                                                   $topicIds
     */
    private static function packGroup(
        string $groupId,
        array|null|OffsetFetchRequestGroup $topicPartitions,
        array $topicIds
    ): OffsetFetchRequestGroup {
        $groupClass = static::groupClass();
        if ($topicPartitions instanceof OffsetFetchRequestGroup) {
            $sameIds = $topicIds === [] || $topicIds + $topicPartitions->topicIds === $topicPartitions->topicIds;

            return $topicPartitions::class === $groupClass && $sameIds ? $topicPartitions : new $groupClass(
                $groupId,
                $topicPartitions->topicPartitions,
                $topicPartitions->memberId,
                $topicPartitions->memberEpoch,
                $topicPartitions->topicIds + $topicIds
            );
        }

        return new $groupClass($groupId, $topicPartitions, topicIds: $topicIds);
    }

    /**
     * Refuses a version 10 batch that names a topic without its id
     *
     * @param array<string, OffsetFetchRequestGroup> $groups
     *
     * @throws UnknownTopicIdException If a topic of an entry has no id
     */
    private static function assertTopicIds(array $groups): void
    {
        foreach ($groups as $groupId => $group) {
            foreach ($group->getTopics() ?? [] as $topic) {
                if (Uuid::isZero($topic->topicId)) {
                    throw new UnknownTopicIdException(
                        [
                            'error'   => 'An OffsetFetch request of version 10 names its topics by id (KIP-848),'
                                . ' and this client does not know the id of this one',
                            'groupId' => $groupId,
                            'topic'   => $topic->topic,
                        ]
                    );
                }
            }
        }
    }

    /**
     * Returns the id of every topic this request names, as topic name => the 16 raw bytes of its uuid
     *
     * @return array<string, string>
     */
    public function getTopicIds(): array
    {
        $topicIds = [];
        foreach ($this->groups as $group) {
            $topicIds += $group->topicIds;
        }

        return $topicIds;
    }

    /**
     * Returns the class of a group entry for the version of the API that this class sends
     *
     * @return class-string<OffsetFetchRequestGroup>
     */
    protected static function groupClass(): string
    {
        return match (true) {
            static::VERSION >= self::MIN_TOPIC_ID_VERSION => OffsetFetchRequestGroup::class,
            static::VERSION >= self::MIN_MEMBER_VERSION   => OffsetFetchRequestGroupV9::class,
            default                                       => OffsetFetchRequestGroupV8::class,
        };
    }
}
