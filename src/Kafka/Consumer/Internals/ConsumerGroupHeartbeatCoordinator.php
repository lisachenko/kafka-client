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

namespace Protocol\Kafka\Consumer\Internals;

use Closure;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\FencedMemberEpochException;
use Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException;
use Protocol\Kafka\Common\Errors\GroupLoadInProgressException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\StaleMemberEpochException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatRequest;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;

/**
 * Membership of one consumer group in the **new consumer protocol** of KIP-848 (Kafka 3.5).
 *
 * This is the second implementation of {@see ConsumerCoordinatorInterface}, the one a consumer configured with
 * `group.protocol = consumer` gets. It speaks a single api - ConsumerGroupHeartbeat, key 68 - where
 * {@see ConsumerCoordinator} speaks four, and the difference is not only the number of frames:
 *
 * ```
 *   ConsumerGroupHeartbeat(epoch 0, subscription, rebalance timeout, topic_partitions = [])  -> join
 *   ConsumerGroupHeartbeat(epoch, everything else null)                                      -> stay alive
 *   ConsumerGroupHeartbeat(epoch, topic_partitions = what I own now)                         -> acknowledge
 *   ConsumerGroupHeartbeat(epoch -1)                                                         -> leave
 * ```
 *
 * **The assignment comes from the coordinator.** No {@see \Protocol\Kafka\Consumer\PartitionAssignorInterface} is
 * used on this path, no {@see \Protocol\Kafka\Consumer\Subscription} is packed and no member is ever the leader of
 * its group: the `Assignment` of the answer *is* the assignment, and it names its topics by the 16 raw bytes of
 * their **topic id**, which this class resolves through {@see Cluster} and re-reads the metadata for when it meets
 * an id it does not know.
 *
 * **The reconciliation is incremental and it is driven by two fields.** The coordinator sends an `Assignment` only
 * when it *changed* - a `null` one means "nothing to do", and that is the answer of every steady-state heartbeat -
 * and it sends each change **once**, so a member that ignores an answer loses it. The member acknowledges by
 * echoing the partitions it owns in the `topic_partitions` of its next heartbeat, and only then does the
 * coordinator hand the partitions this member gave up to somebody else. What a rebalance listener sees is
 * therefore not "everything, then everything again" but exactly the partitions that moved, which
 * {@see self::partitionsToRevoke()} and {@see self::partitionsToAssign()} compute.
 *
 * **The interval is the coordinator's.** Every answer carries a `heartbeat_interval_ms`
 * (`group.consumer.heartbeat.interval.ms` of the broker, 5000 on the node of this line) and
 * {@see self::maybeHeartbeat()} honours that number, not the `heartbeat.interval.ms` of the consumer, which is
 * never sent anywhere in this protocol. The session timeout is the broker's as well.
 *
 * **The member id is the member's own**: a uuid it generates for itself and keeps for its whole life, where a
 * classic member is given one by the coordinator in the answer of its first JoinGroup. A member that is fenced -
 * **110** `FencedMemberEpoch`, **113** `StaleMemberEpoch` or **25** `UnknownMemberId` - drops its partitions and
 * joins again with the epoch 0 **and a fresh member id**, which is what {@see self::resetMembership()} does. The
 * two configuration errors of the api are not retried at all: **112** `UnsupportedAssignor` names a
 * `group.remote.assignor` the broker does not have, **111** `UnreleasedInstanceId` a `group.instance.id` another
 * member still holds, and neither of them gets better by trying again.
 *
 * The epoch this class reports as {@see self::getGenerationId()} is the **member epoch**, which is what an
 * OffsetCommit **v9** and an OffsetFetch **v9** of this member send in the field a classic member fills with its
 * generation id; the call sites of {@see \Protocol\Kafka\Consumer\KafkaConsumer} need no change for it.
 *
 * @see docs/protocol/3.9.md, section "ConsumerGroupHeartbeat API (key 68, v0)"
 * @see \Protocol\Kafka\Consumer\ConsumerConfig::GROUP_PROTOCOL
 */
final class ConsumerGroupHeartbeatCoordinator implements ConsumerCoordinatorInterface
{
    /**
     * How many times a fenced member joins again before the error of the coordinator reaches the caller
     */
    private const int MAX_REBALANCE_ATTEMPTS = 5;

    /**
     * How many acknowledging heartbeats one reconciliation may take before it is given up
     *
     * Every step is one round trip that answers a *changed* assignment, so a settled group needs one and a group
     * that keeps moving is a group this member cannot catch up with in a single poll(): the loop stops, the
     * membership stays, and the next poll() continues the reconciliation.
     */
    private const int MAX_RECONCILIATION_STEPS = 10;

    /**
     * Interval used until the first answer of the coordinator dictates one, in milliseconds
     */
    private const int DEFAULT_HEARTBEAT_INTERVAL_MS = 5000;

    /**
     * Member id of this consumer, which it generates for itself and keeps until it is fenced
     */
    private string $memberId = JoinGroupRequest::DEFAULT_MEMBER_ID;

    /**
     * Epoch of this member, {@see OffsetCommitRequest::DEFAULT_GENERATION_ID} for a consumer without membership
     */
    private int $memberEpoch = OffsetCommitRequest::DEFAULT_GENERATION_ID;

    /**
     * Partitions this member has acknowledged, as the raw 16 bytes of the topic id => its partitions
     *
     * @var array<string, list<int>>
     */
    private array $ownedPartitions = [];

    /**
     * Assignment the coordinator sent and this member has not acknowledged yet, null when there is none
     *
     * @var array<string, list<int>>|null
     */
    private ?array $targetPartitions = null;

    /**
     * Interval the coordinator dictates, in milliseconds
     */
    private int $heartbeatIntervalMs = self::DEFAULT_HEARTBEAT_INTERVAL_MS;

    /**
     * Subscription the coordinator knows of this member, null until it named one
     *
     * @var list<string>|null
     */
    private ?array $subscribedTopics = null;

    /**
     * Whether the next poll() has to reconcile before it fetches anything
     */
    private bool $rejoinNeeded = true;

    /**
     * Coordinator of the group, resolved on the first use and dropped when a broker refuses to be it
     */
    private ?Node $node = null;

    /**
     * Moment of the last heartbeat, in milliseconds
     */
    private int $lastHeartbeatMs = 0;

    /**
     * @param Client      $client             Low-level client that speaks the api
     * @param Cluster     $cluster            Metadata of the cluster, which resolves the topic ids of an assignment
     * @param string      $groupId            Name of the consumer group
     * @param int         $rebalanceTimeoutMs `max.poll.interval.ms`, the `rebalance_timeout_ms` of the join
     * @param int         $retryBackoffMs     `retry.backoff.ms`, waited before a rejoin
     * @param string|null $groupInstanceId    `group.instance.id` of a static member (KIP-345), null for a dynamic
     * @param string|null $serverAssignor     `group.remote.assignor`, null to let the coordinator choose
     * @param string|null $rackId             `client.rack` of this consumer, null when it names none
     */
    public function __construct(
        private readonly Client $client,
        private readonly Cluster $cluster,
        private readonly string $groupId,
        private readonly int $rebalanceTimeoutMs,
        private readonly int $retryBackoffMs = 100,
        private readonly ?string $groupInstanceId = null,
        private readonly ?string $serverAssignor = null,
        private readonly ?string $rackId = null
    ) {}

    /**
     * Returns the member id a KIP-848 consumer generates for itself, the uuid the Java client sends
     *
     * `ConsumerConfig` @ 3.9.2 gives every member of the new protocol a `Uuid.randomUuid().toString()` and keeps
     * it for the whole life of the consumer; the coordinator only generates one for a request that names none.
     */
    public static function newMemberId(): string
    {
        return sprintf(
            '%04x%04x-%04x-4%03x-%04x-%04x%04x%04x',
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF),
            random_int(0, 0x0FFF),
            random_int(0, 0x3FFF) | 0x8000,
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF)
        );
    }

    /**
     * @inheritdoc
     */
    public function getGroupInstanceId(): ?string
    {
        return $this->groupInstanceId;
    }

    /**
     * Tells whether this consumer is a static member of its group (KIP-345)
     */
    public function isStaticMember(): bool
    {
        return $this->groupInstanceId !== null;
    }

    /**
     * @inheritdoc
     */
    public function getMemberId(): string
    {
        return $this->memberId;
    }

    /**
     * Returns the **member epoch** of this consumer, which is the generation of the new protocol
     *
     * @inheritdoc
     */
    public function getGenerationId(): int
    {
        return $this->memberEpoch;
    }

    /**
     * @inheritdoc
     */
    public function isMember(): bool
    {
        return $this->memberId !== JoinGroupRequest::DEFAULT_MEMBER_ID
            && $this->memberEpoch > OffsetCommitRequest::DEFAULT_GENERATION_ID;
    }

    /**
     * @inheritdoc
     */
    public function needsRejoin(): bool
    {
        return $this->rejoinNeeded;
    }

    /**
     * @inheritdoc
     */
    public function requestRejoin(?string $reason = null): void
    {
        // The api has no `reason` field - KIP-800 belongs to JoinGroup and LeaveGroup alone - so the text is not
        // kept anywhere: there is nothing to send it in
        $this->rejoinNeeded = true;
    }

    /**
     * @inheritdoc
     */
    public function getNode(): Node
    {
        return $this->node ??= $this->client->getGroupCoordinator($this->groupId);
    }

    /**
     * @inheritdoc
     */
    public function invalidateNode(): void
    {
        $this->node = null;
    }

    /**
     * Returns the interval the coordinator dictated in its last answer, in milliseconds
     */
    public function getHeartbeatIntervalMs(): int
    {
        return $this->heartbeatIntervalMs;
    }

    /**
     * Only the partitions the target assignment of the coordinator no longer holds for this member
     *
     * @inheritdoc
     */
    public function partitionsToRevoke(array $ownedPartitions): array
    {
        if ($this->targetPartitions === null) {
            return [];
        }

        $target  = $this->resolveNames($this->targetPartitions);
        $revoked = [];
        foreach ($ownedPartitions as $topic => $partitions) {
            $kept = array_values(array_diff($partitions, $target[$topic] ?? []));
            if ($kept !== []) {
                $revoked[$topic] = $kept;
            }
        }

        return $revoked;
    }

    /**
     * Only the partitions that were really added, because the ones this member kept were never taken away
     *
     * @inheritdoc
     */
    public function partitionsToAssign(array $ownedPartitions, array $assignment): array
    {
        $added = [];
        foreach ($assignment as $topic => $partitions) {
            $new = array_values(array_diff($partitions, $ownedPartitions[$topic] ?? []));
            if ($new !== []) {
                $added[$topic] = $new;
            }
        }

        return $added;
    }

    /**
     * @inheritdoc
     */
    public function ensureActiveGroup(array $topics, Closure $partitionsResolver): array
    {
        for ($attempt = 1; ; $attempt++) {
            try {
                return $this->reconcile(array_values($topics));
            } catch (
                FencedMemberEpochException
                | StaleMemberEpochException
                | UnknownMemberIdException $exception
            ) {
                // The coordinator fenced this member: it gives its partitions up and joins as a NEW member
                $this->resetMembership();
            } catch (
                GroupCoordinatorNotAvailableException
                | NotCoordinatorForGroupException
                | GroupLoadInProgressException $exception
            ) {
                $this->invalidateNode();
            }

            if ($attempt >= self::MAX_REBALANCE_ATTEMPTS) {
                throw $exception;
            }
            usleep($this->retryBackoffMs * 1000);
        }
    }

    /**
     * @inheritdoc
     */
    public function maybeHeartbeat(int $nowMs): bool
    {
        if (!$this->isMember() || $this->rejoinNeeded) {
            return false;
        }
        if ($nowMs - $this->lastHeartbeatMs < $this->heartbeatIntervalMs) {
            return false;
        }

        try {
            $this->apply($this->client->consumerGroupHeartbeat(
                $this->getNode(),
                $this->groupId,
                $this->memberId,
                $this->memberEpoch
            ));
        } catch (FencedMemberEpochException | StaleMemberEpochException | UnknownMemberIdException) {
            $this->resetMembership();
        } catch (GroupCoordinatorNotAvailableException | NotCoordinatorForGroupException) {
            $this->invalidateNode();
            $this->rejoinNeeded = true;
        }

        return true;
    }

    /**
     * Takes this member out of its group with the heartbeat of the epoch -1, or -2 for a static one
     *
     * A **static** member (KIP-345) announces that it will come back: the epoch
     * {@see ConsumerGroupHeartbeatRequest::STATIC_LEAVE_MEMBER_EPOCH} makes the coordinator keep its instance id
     * and its assignment for the session timeout instead of handing them to somebody else, which is what the
     * classic protocol expresses by sending no LeaveGroup at all.
     *
     * The `reason` of KIP-800 has no field in this api and is ignored.
     *
     * @inheritdoc
     */
    public function leaveGroup(?string $reason = null): void
    {
        if (!$this->isMember()) {
            return;
        }

        $memberId  = $this->memberId;
        $rejoining = $this->isStaticMember();
        $this->resetMembership();

        try {
            $this->client->leaveConsumerGroup(
                $this->getNode(),
                $this->groupId,
                $memberId,
                $rejoining,
                $this->groupInstanceId
            );
        } catch (UnknownMemberIdException | FencedMemberEpochException | StaleMemberEpochException) {
            // The coordinator has already forgotten this member, which is exactly what was asked for
        }
    }

    /**
     * Forgets the membership, so that the next reconciliation joins as a **new** member
     *
     * The member id is thrown away with the epoch: a member the coordinator fenced may not come back under the
     * same id, `ConsumerGroupHeartbeatRequest.json` @ 3.9.2 gives every join a member id of its own.
     */
    public function resetMembership(): void
    {
        $this->memberId         = JoinGroupRequest::DEFAULT_MEMBER_ID;
        $this->memberEpoch      = OffsetCommitRequest::DEFAULT_GENERATION_ID;
        $this->ownedPartitions  = [];
        $this->targetPartitions = null;
        $this->subscribedTopics = null;
        $this->rejoinNeeded     = true;
    }

    /**
     * One pass of the protocol: the join if there is none, then the acknowledgements until nothing changes
     *
     * @param list<string> $topics Subscription of this member
     *
     * @return array<string, list<int>> Partitions of this member, by topic name
     */
    private function reconcile(array $topics): array
    {
        if (!$this->isMember()) {
            $this->join($topics);
        } elseif ($this->subscribedTopics !== $topics) {
            // A subscription that changed travels in an ordinary heartbeat: there is nothing to re-join for
            $this->apply($this->client->consumerGroupHeartbeat(
                $this->getNode(),
                $this->groupId,
                $this->memberId,
                $this->memberEpoch,
                $topics
            ));
            $this->subscribedTopics = $topics;
        }

        for ($step = 0; $step < self::MAX_RECONCILIATION_STEPS && $this->targetPartitions !== null; $step++) {
            $target                 = $this->targetPartitions;
            $this->targetPartitions = null;
            if (self::isSameAssignment($target, $this->ownedPartitions)) {
                continue;
            }
            $this->ownedPartitions = $target;

            // The acknowledgement: the coordinator hands what this member gave up to somebody else only once it
            // has seen the partitions the member really owns now
            $this->apply($this->client->consumerGroupHeartbeat(
                $this->getNode(),
                $this->groupId,
                $this->memberId,
                $this->memberEpoch,
                null,
                $this->ownedPartitions
            ));
        }

        $this->rejoinNeeded = false;

        return $this->resolveNames($this->ownedPartitions);
    }

    /**
     * Sends the heartbeat that joins the group: a fresh member id, the epoch 0 and the whole picture
     *
     * @param list<string> $topics Subscription of this member
     */
    private function join(array $topics): void
    {
        $this->memberId         = self::newMemberId();
        $this->ownedPartitions  = [];
        $this->targetPartitions = null;

        $this->apply($this->client->joinConsumerGroup(
            $this->getNode(),
            $this->groupId,
            $this->memberId,
            $topics,
            $this->rebalanceTimeoutMs,
            $this->groupInstanceId,
            $this->rackId,
            $this->serverAssignor
        ));

        $this->subscribedTopics = $topics;
    }

    /**
     * Reads what an answer of the coordinator changed: the member id, the epoch, the interval and the assignment
     */
    private function apply(ConsumerGroupHeartbeatResponse $response): void
    {
        $this->lastHeartbeatMs = (int) (microtime(true) * 1e3);

        // A member that named no id of its own is given one here, and a member that named one is answered its own
        if ($response->memberId !== null && $response->memberId !== JoinGroupRequest::DEFAULT_MEMBER_ID) {
            $this->memberId = $response->memberId;
        }
        $this->memberEpoch = $response->memberEpoch;
        if ($response->heartbeatIntervalMs > 0) {
            $this->heartbeatIntervalMs = $response->heartbeatIntervalMs;
        }

        // A null assignment means "nothing changed since the last answer"; an assignment with an EMPTY array
        // means "you own nothing", and the two must not be confused
        if ($response->assignment === null) {
            return;
        }

        $target = $response->assignment->partitionsByTopicId();
        if (self::isSameAssignment($target, $this->ownedPartitions)) {
            return;
        }

        $this->targetPartitions = $target;
        $this->rejoinNeeded     = true;
    }

    /**
     * Turns the topic ids of an assignment into topic names, refreshing the metadata for an id it does not know
     *
     * The api names its topics by the 16 raw bytes of their id and by nothing else, and a consumer that has just
     * subscribed may hold metadata that predates the topic it was assigned - a topic that was created while the
     * group existed, or one this consumer never asked about. One reload of the cluster metadata is paid for such
     * an id; an id that is still unknown afterwards is left out, because there is no name to report it under.
     *
     * @param array<string, list<int>> $partitionsByTopicId
     *
     * @return array<string, list<int>>
     */
    private function resolveNames(array $partitionsByTopicId): array
    {
        $names = $this->namesOf($partitionsByTopicId);
        if (count($names) === count($partitionsByTopicId)) {
            return $names;
        }

        $this->cluster->reload();

        return $this->namesOf($partitionsByTopicId);
    }

    /**
     * Names the topics of an assignment out of the metadata this client holds, leaving out what it cannot name
     *
     * @param array<string, list<int>> $partitionsByTopicId
     *
     * @return array<string, list<int>>
     */
    private function namesOf(array $partitionsByTopicId): array
    {
        $names = [];
        foreach ($partitionsByTopicId as $topicId => $partitions) {
            $name = $this->cluster->topicNameById((string) $topicId);
            if ($name !== null) {
                $names[$name] = $partitions;
            }
        }

        return $names;
    }

    /**
     * Tells whether two assignments name the same partitions, whatever order they arrived in
     *
     * @param array<string, list<int>> $left
     * @param array<string, list<int>> $right
     */
    private static function isSameAssignment(array $left, array $right): bool
    {
        return self::normalized($left) === self::normalized($right);
    }

    /**
     * Sorts an assignment by topic id and partition, and drops the topics without a partition
     *
     * @param array<string, list<int>> $assignment
     *
     * @return array<string, list<int>>
     */
    private static function normalized(array $assignment): array
    {
        $normalized = [];
        foreach ($assignment as $topicId => $partitions) {
            if ($partitions === []) {
                continue;
            }
            sort($partitions);
            $normalized[(string) $topicId] = array_values($partitions);
        }
        ksort($normalized);

        return $normalized;
    }
}
