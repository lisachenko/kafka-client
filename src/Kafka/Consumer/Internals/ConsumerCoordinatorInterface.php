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
use Protocol\Kafka\Common\Node;

/**
 * Membership of one consumer group, as {@see \Protocol\Kafka\Consumer\KafkaConsumer} uses it.
 *
 * There are **two** membership protocols in Kafka 3.9, and this is everything the consumer needs of either of
 * them:
 *
 * * {@see ConsumerCoordinator} speaks the **classic** one (JoinGroup, SyncGroup, Heartbeat, LeaveGroup), where the
 *   assignment is computed by the leader of the group and a rebalance revokes everything before it starts;
 * * {@see ConsumerGroupHeartbeatCoordinator} speaks the **new consumer protocol** of KIP-848 (the single
 *   ConsumerGroupHeartbeat, key 68), where the coordinator computes the assignment and a member gives up only the
 *   partitions it really loses.
 *
 * `group.protocol` of {@see \Protocol\Kafka\Consumer\ConsumerConfig} picks one, and the consumer itself contains
 * no branch of the two protocols: the difference between "revoke everything, then take the new assignment" and
 * "revoke what is gone, then take what is new" is expressed by {@see self::partitionsToRevoke()} and
 * {@see self::partitionsToAssign()}, which every implementation answers for its own protocol.
 *
 * The vocabulary is the classic one wherever both protocols have the concept, because these names are published:
 * {@see self::getGenerationId()} is the **member epoch** of a KIP-848 member, which is what an OffsetCommit v9 or
 * an OffsetFetch v9 of that member sends in the field a classic member fills with its generation.
 *
 * @see docs/protocol/3.9.md, section "Group membership protocol (keys 11 to 14)"
 * @see docs/protocol/3.9.md, section "ConsumerGroupHeartbeat API (key 68, v0)"
 */
interface ConsumerCoordinatorInterface
{
    /**
     * Returns the `group.instance.id` of this member, null for a dynamic one (KIP-345)
     */
    public function getGroupInstanceId(): ?string;

    /**
     * Returns the member id of this consumer, an empty string for a consumer that is not a member of its group
     */
    public function getMemberId(): string;

    /**
     * Returns the generation of the group - the **member epoch** in the new protocol - -1 without a membership
     */
    public function getGenerationId(): int;

    /**
     * Tells whether this consumer holds a membership of its group at all
     */
    public function isMember(): bool;

    /**
     * Tells whether the membership has to be established again before the consumer may fetch
     */
    public function needsRejoin(): bool;

    /**
     * Asks for a rebalance on the next poll(), e.g. because the subscription changed
     *
     * @param string|null $reason Why this member has to rejoin, null to keep the reason of the last request
     */
    public function requestRejoin(?string $reason = null): void;

    /**
     * Returns the broker that coordinates this group, looking it up once and keeping it
     */
    public function getNode(): Node;

    /**
     * Forgets the cached coordinator, so that the next request looks it up again
     */
    public function invalidateNode(): void;

    /**
     * Partitions this member has to hand back before the assignment that follows, out of the ones it owns
     *
     * The **classic** protocol answers everything: a rebalance of the eager protocol starts by giving the whole
     * assignment up, and the JoinGroup is only sent afterwards. The **KIP-848** protocol answers only the
     * partitions the target assignment of the coordinator no longer holds for this member, which is the
     * incremental revoke of the new protocol, and the empty array while nothing has to be given up at all.
     *
     * @param array<string, list<int>> $ownedPartitions Partitions of this consumer right now, by topic
     *
     * @return array<string, list<int>> Partitions to revoke, by topic
     */
    public function partitionsToRevoke(array $ownedPartitions): array;

    /**
     * Partitions a rebalance listener is told about after {@see self::ensureActiveGroup()} answered
     *
     * The **classic** protocol answers the whole new assignment - a member of the eager protocol has just been
     * given everything it owns - and the **KIP-848** one only the partitions that were really added, because the
     * ones it kept were never taken away.
     *
     * @param array<string, list<int>> $ownedPartitions Partitions the consumer owned before the rebalance
     * @param array<string, list<int>> $assignment      Partitions it owns now
     *
     * @return array<string, list<int>> Partitions to announce as assigned, by topic
     */
    public function partitionsToAssign(array $ownedPartitions, array $assignment): array;

    /**
     * Runs the membership protocol until this member holds an assignment of the current generation.
     *
     * @param list<string>                                    $topics             Subscription of this member
     * @param Closure(list<string>): array<string, list<int>> $partitionsResolver Partition ids of the topics the
     *        members of the group subscribed to, which only the leader of a classic generation asks for
     *
     * @return array<string, list<int>> Partitions of this member, by topic; empty when it got none
     */
    public function ensureActiveGroup(array $topics, Closure $partitionsResolver): array;

    /**
     * Sends a heartbeat once the interval of the protocol has elapsed since the last one
     *
     * @param int $nowMs Current time in milliseconds
     *
     * @return bool Whether a heartbeat was sent
     */
    public function maybeHeartbeat(int $nowMs): bool;

    /**
     * Removes this member from the group, which makes the group rebalance right away
     *
     * @param string|null $reason Why this member leaves; the new protocol has no field for it and ignores it
     */
    public function leaveGroup(?string $reason = null): void;

    /**
     * Forgets the member id and the generation, so that the next rebalance joins as a new member
     */
    public function resetMembership(): void;
}
