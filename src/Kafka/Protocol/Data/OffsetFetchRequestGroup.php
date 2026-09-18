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
 * One group of an OffsetFetch request of version 8 (Kafka 3.0) and 9 (Kafka 3.7, KIP-848)
 *
 * <pre>
 *   OffsetFetchRequestGroup => group_id member_id member_epoch [topics]
 *     group_id     => COMPACT_STRING
 *     member_id    => COMPACT_NULLABLE_STRING   -- since version 9, null for a classic member
 *     member_epoch => INT32                     -- since version 9, -1 for a classic member
 *     topics       => name [partition_indexes]  -- NULLABLE
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
 * **Version 9 (Kafka 3.7, KIP-848) added the two member fields to this very entry**, and to no other place of
 * the request: `OffsetFetchRequest.json` @ 3.7.2 - *"Version 9 is the first version that can be used with the new
 * consumer group protocol (KIP-848). It adds the MemberId and MemberEpoch fields. Those are filled in and
 * validated when the new consumer protocol is used."* - puts a nullable `MemberId` (default `null`) and an
 * `MemberEpoch` (int32, default `-1`) behind the group id, before the topic array. A member of a **KIP-848**
 * group names itself with them and is answered the **25** `UnknownMemberId` or the **113** `StaleMemberEpoch`
 * when they do not match what the coordinator holds; a classic member, and every administrative reader, leaves
 * them at their defaults, which `ConsumerGroup::validateOffsetFetch` @ 3.9.2 accepts without looking a member up
 * at all ("When the member id is null and the member epoch is -1, the request either comes from the admin client
 * or from a client which does not provide them. In this case, the fetch request is accepted."). A **classic**
 * group ignores both fields whatever they hold. {@see OffsetFetchRequestGroupV8} is the same entry without them.
 *
 * The field names follow the classes of the line: the key is the `groupId` of every other group DTO of this
 * package, and the topic array keeps the name `topicPartitions` of
 * {@see \Protocol\Kafka\Protocol\Request\OffsetFetchRequest}, whose field it was until version 7.
 *
 * @see docs/protocol/3.9.md, section "OffsetFetch API (key 9, v0 to v9)"
 * @see docs/protocol/3.9.md, section "The member id and epoch of KIP-848 (v9)"
 */
class OffsetFetchRequestGroup implements BinarySchemaInterface
{
    /**
     * The member epoch of an entry that names no member of a KIP-848 group (`MemberEpoch` defaults to -1)
     */
    public const int NO_MEMBER_EPOCH = -1;

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
     * @param string|null $memberId    Member id of a KIP-848 member, null for a classic member (version 9)
     * @param int         $memberEpoch Member epoch of a KIP-848 member, -1 for a classic member (version 9)
     */
    public function __construct(
        /**
         * The group to fetch the offsets of
         */
        public readonly string $groupId,
        ?array $topicPartitions = null,
        /**
         * The member id the coordinator assigned to this member of a KIP-848 group, null for anyone else.
         *
         * @since Version 9 of protocol
         */
        public readonly ?string $memberId = null,
        /**
         * The member epoch of this member of a KIP-848 group, {@see self::NO_MEMBER_EPOCH} for anyone else.
         *
         * @since Version 9 of protocol
         */
        public readonly int $memberEpoch = self::NO_MEMBER_EPOCH
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
            'memberId'        => BinarySchema::TYPE_NULLABLE_STRING,
            'memberEpoch'     => BinarySchema::TYPE_INT32,
            'topicPartitions' => [
                'topic'                      => PartitionsForTopic::class,
                BinarySchema::FLAG_NULLABLE => true,
            ],
        ];
    }
}
