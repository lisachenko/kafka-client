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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ConsumerGroupHeartbeatTopicPartitions;

/**
 * ConsumerGroupHeartbeat, version 0: the whole membership protocol of KIP-848 in one request (ApiKey 68, Kafka 3.5)
 *
 * <pre>
 *   ConsumerGroupHeartbeat Request (Version: 0) => group_id member_id member_epoch instance_id rack_id
 *                                                  rebalance_timeout_ms [subscribed_topic_names] server_assignor
 *                                                  [topic_partitions]
 *     group_id               => COMPACT_STRING
 *     member_id              => COMPACT_STRING
 *     member_epoch           => INT32
 *     instance_id            => COMPACT_NULLABLE_STRING
 *     rack_id                => COMPACT_NULLABLE_STRING
 *     rebalance_timeout_ms   => INT32
 *     subscribed_topic_names => COMPACT_STRING             -- NULLABLE array
 *     server_assignor        => COMPACT_NULLABLE_STRING
 *     topic_partitions       => topic_id [partitions]      -- NULLABLE array
 *       topic_id   => UUID
 *       partitions => INT32
 * </pre>
 *
 * This one api replaces **four** of the classic protocol: JoinGroup (11), SyncGroup (14), Heartbeat (12) and
 * LeaveGroup (13). A member of the new consumer protocol sends nothing else to its coordinator - the first
 * heartbeat joins, every following one keeps the membership alive *and* carries the reconciliation, and the last
 * one leaves - and it never computes an assignment: the coordinator does that and answers with it
 * ({@see ConsumerGroupHeartbeatResponse}).
 *
 * **The `member_epoch` is the verb of the api** (`ConsumerGroupHeartbeatRequest.json` @ 3.9.2: *"The current
 * member epoch; 0 to join the group; -1 to leave the group; -2 to indicate that the static member will rejoin"*):
 *
 * * {@see self::JOIN_MEMBER_EPOCH} (**0**) joins or re-joins, and is what a member that was fenced sends again;
 * * the epoch the last answer carried keeps the membership;
 * * {@see self::LEAVE_MEMBER_EPOCH} (**-1**) leaves the group for good;
 * * {@see self::STATIC_LEAVE_MEMBER_EPOCH} (**-2**) is the leave of a **static** member that will come back: the
 *   coordinator keeps its `group.instance.id` and its assignment instead of giving them away.
 *
 * **Everything else is a delta.** The five nullable fields `instance_id`, `rack_id`, `subscribed_topic_names`,
 * `server_assignor` and `topic_partitions` mean *"unchanged since the last heartbeat"* when they are null, and
 * `rebalance_timeout_ms` says the same with **-1** ({@see self::UNCHANGED_REBALANCE_TIMEOUT_MS}). A client that
 * re-sends everything in every heartbeat is not refused, but it makes the coordinator recompute what it already
 * knows, so the three named constructors of this class encode the rules instead of leaving them to a caller:
 * {@see self::forJoin()} sends the full picture, {@see self::forHeartbeat()} sends nulls and only what really
 * changed, and {@see self::forLeave()} sends the epoch -1 with nothing else.
 *
 * **The join is the one frame with a rule of its own**: `topic_partitions` has to be the **empty array** and not
 * the null of the default. `GroupMetadataManager.throwIfConsumerGroupHeartbeatRequestIsInvalid` @ 3.9.2 refuses
 * a null one with **42** `InvalidRequest` and the message *"TopicPartitions must be empty when (re-)joining."*,
 * and it refuses a join that carries no `subscribed_topic_names` and no `rebalance_timeout_ms` the same way.
 *
 * The member id is the member's own: a KIP-848 consumer generates a **uuid** for itself and keeps it for its whole
 * life ({@see \Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator::newMemberId()}), which is the
 * opposite of the classic protocol, where the coordinator hands one out in the answer of a first JoinGroup. A
 * request that names none is still accepted and answered with a generated id, see the response class.
 *
 * @see docs/protocol/3.9.md, section "ConsumerGroupHeartbeat API (key 68, v0)"
 */
class ConsumerGroupHeartbeatRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::CONSUMER_GROUP_HEARTBEAT;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * The api is flexible from its first version: it was born after KIP-482 (Kafka 2.4)
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Member epoch of a heartbeat that joins or re-joins the group
     */
    public const int JOIN_MEMBER_EPOCH = 0;

    /**
     * Member epoch of a heartbeat that leaves the group for good
     */
    public const int LEAVE_MEMBER_EPOCH = -1;

    /**
     * Member epoch of the leave of a **static** member that announces it will rejoin (KIP-848, KIP-345)
     */
    public const int STATIC_LEAVE_MEMBER_EPOCH = -2;

    /**
     * Value of `rebalance_timeout_ms` that says "unchanged since the last heartbeat"
     */
    public const int UNCHANGED_REBALANCE_TIMEOUT_MS = -1;

    /**
     * Topics this member subscribed to, or null when the subscription did not change since the last heartbeat
     *
     * @var list<string>|null
     */
    protected readonly ?array $subscribedTopicNames;

    /**
     * Partitions this member owns, or null when they did not change since the last heartbeat
     *
     * @var list<ConsumerGroupHeartbeatTopicPartitions>|null
     */
    protected readonly ?array $topicPartitions;

    /**
     * @param string                        $groupId              Group this member belongs to
     * @param string                        $memberId             Member id, the uuid the member generated itself
     * @param int                           $memberEpoch          Epoch of the member, see the class docblock
     * @param list<string>|null             $subscribedTopicNames Subscription, null for "unchanged"
     * @param array<string, list<int>>|null $topicPartitions      Owned partitions as topic id => partitions, null
     *        for "unchanged", the **empty array** for a (re-)join
     * @param int                           $rebalanceTimeoutMs   `max.poll.interval.ms`, -1 for "unchanged"
     * @param string|null                   $instanceId           `group.instance.id` of a static member
     * @param string|null                   $rackId               `client.rack` of the member (KIP-881)
     * @param string|null                   $serverAssignor       Assignor the coordinator shall run, null for its
     *        own choice
     * @param string                        $clientId             An identifier of the client
     * @param int                           $correlationId        A value the broker passes back unmodified
     */
    public function __construct(
        /**
         * Group this member belongs to
         */
        protected readonly string $groupId,
        /**
         * Member id of this member, which it keeps for its whole life
         */
        protected readonly string $memberId,
        /**
         * Current epoch of the member, 0 to join, -1 to leave and -2 for the leave of a static member
         */
        protected readonly int $memberEpoch,
        ?array $subscribedTopicNames = null,
        ?array $topicPartitions = null,
        /**
         * How long the coordinator waits for this member to revoke its partitions, -1 for "unchanged"
         */
        protected readonly int $rebalanceTimeoutMs = self::UNCHANGED_REBALANCE_TIMEOUT_MS,
        /**
         * `group.instance.id` of a static member, null when it is not one or when it did not change
         */
        protected readonly ?string $instanceId = null,
        /**
         * Rack of the member (KIP-881), null when it has none or when it did not change
         */
        protected readonly ?string $rackId = null,
        /**
         * Server-side assignor to use, null to let the coordinator pick one
         */
        protected readonly ?string $serverAssignor = null,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->subscribedTopicNames = $subscribedTopicNames === null ? null : array_values($subscribedTopicNames);

        if ($topicPartitions === null) {
            $this->topicPartitions = null;
        } else {
            $entries = [];
            foreach ($topicPartitions as $topicId => $partitions) {
                $entries[] = new ConsumerGroupHeartbeatTopicPartitions((string) $topicId, array_values($partitions));
            }
            $this->topicPartitions = $entries;
        }

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds the heartbeat that (re-)joins a group: the epoch 0, the whole picture and an EMPTY partition array
     *
     * The empty array is not the same as the null of the default: the coordinator refuses a (re-)join whose
     * `topic_partitions` is null with the 42 and the message "TopicPartitions must be empty when (re-)joining."
     *
     * @param list<string> $subscribedTopicNames Topics this member subscribes to, which a join has to name
     * @param int          $rebalanceTimeoutMs   `max.poll.interval.ms` of this member
     * @param string|null  $instanceId           `group.instance.id` of a static member
     * @param string|null  $rackId               `client.rack` of the member
     * @param string|null  $serverAssignor       Assignor the coordinator shall run, null for its own choice
     */
    public static function forJoin(
        string $groupId,
        string $memberId,
        array $subscribedTopicNames,
        int $rebalanceTimeoutMs,
        ?string $instanceId = null,
        ?string $rackId = null,
        ?string $serverAssignor = null,
        string $clientId = '',
        int $correlationId = 0
    ): self {
        return new self(
            $groupId,
            $memberId,
            self::JOIN_MEMBER_EPOCH,
            $subscribedTopicNames,
            [],
            $rebalanceTimeoutMs,
            $instanceId,
            $rackId,
            $serverAssignor,
            $clientId,
            $correlationId
        );
    }

    /**
     * Builds the heartbeat of a member that is in its group: the epoch it holds and only what really changed
     *
     * Everything a caller leaves out is `null` - "unchanged since the last heartbeat" - which is what the steady
     * state of a KIP-848 member looks like on the wire: the group id, the member id, its epoch and five nulls.
     *
     * @param int                           $memberEpoch          Epoch of the last answer of the coordinator
     * @param list<string>|null             $subscribedTopicNames New subscription, null when it did not change
     * @param array<string, list<int>>|null $topicPartitions      Partitions the member owns now, as topic id =>
     *        partitions, null when they did not change; this is how a member **acknowledges** an assignment
     * @param int                           $rebalanceTimeoutMs   New rebalance timeout, -1 when it did not change
     */
    public static function forHeartbeat(
        string $groupId,
        string $memberId,
        int $memberEpoch,
        ?array $subscribedTopicNames = null,
        ?array $topicPartitions = null,
        int $rebalanceTimeoutMs = self::UNCHANGED_REBALANCE_TIMEOUT_MS,
        ?string $serverAssignor = null,
        string $clientId = '',
        int $correlationId = 0
    ): self {
        return new self(
            $groupId,
            $memberId,
            $memberEpoch,
            $subscribedTopicNames,
            $topicPartitions,
            $rebalanceTimeoutMs,
            null,
            null,
            $serverAssignor,
            $clientId,
            $correlationId
        );
    }

    /**
     * Builds the heartbeat that takes the member out of its group again
     *
     * The epoch is -1 for a dynamic member and **-2** for a static one that announces it will come back, which is
     * what `$rejoining` asks for; everything else is null, because nothing of it is read any more.
     *
     * @param bool $rejoining Whether this is the leave of a static member that will rejoin (the epoch -2)
     */
    public static function forLeave(
        string $groupId,
        string $memberId,
        bool $rejoining = false,
        ?string $instanceId = null,
        string $clientId = '',
        int $correlationId = 0
    ): self {
        return new self(
            $groupId,
            $memberId,
            $rejoining ? self::STATIC_LEAVE_MEMBER_EPOCH : self::LEAVE_MEMBER_EPOCH,
            null,
            null,
            self::UNCHANGED_REBALANCE_TIMEOUT_MS,
            $instanceId,
            null,
            null,
            $clientId,
            $correlationId
        );
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'groupId'              => BinarySchema::TYPE_STRING,
            'memberId'             => BinarySchema::TYPE_STRING,
            'memberEpoch'          => BinarySchema::TYPE_INT32,
            'instanceId'           => BinarySchema::TYPE_NULLABLE_STRING,
            'rackId'               => BinarySchema::TYPE_NULLABLE_STRING,
            'rebalanceTimeoutMs'   => BinarySchema::TYPE_INT32,
            'subscribedTopicNames' => [BinarySchema::TYPE_STRING, BinarySchema::FLAG_NULLABLE => true],
            'serverAssignor'       => BinarySchema::TYPE_NULLABLE_STRING,
            'topicPartitions'      => [
                ConsumerGroupHeartbeatTopicPartitions::class,
                BinarySchema::FLAG_NULLABLE => true,
            ],
        ];
    }

    /**
     * Returns the group this heartbeat belongs to
     */
    public function getGroupId(): string
    {
        return $this->groupId;
    }

    /**
     * Returns the member id this heartbeat names
     */
    public function getMemberId(): string
    {
        return $this->memberId;
    }

    /**
     * Returns the member epoch this heartbeat carries
     */
    public function getMemberEpoch(): int
    {
        return $this->memberEpoch;
    }

    /**
     * Returns the subscription this heartbeat names, null when it did not change since the last one
     *
     * @return list<string>|null
     */
    public function getSubscribedTopicNames(): ?array
    {
        return $this->subscribedTopicNames;
    }

    /**
     * Returns the partitions this heartbeat acknowledges, null when they did not change since the last one
     *
     * @return list<ConsumerGroupHeartbeatTopicPartitions>|null
     */
    public function getTopicPartitions(): ?array
    {
        return $this->topicPartitions;
    }
}
