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
use Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException;
use Protocol\Kafka\Common\Errors\GroupLoadInProgressException;
use Protocol\Kafka\Common\Errors\IllegalGenerationException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\RebalanceInProgressException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\PartitionAssignorInterface;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\Protocol\Data\JoinGroupResponseMember;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;

/**
 * Membership of one consumer group: the member id, the generation and the rebalance that produces them.
 *
 * This is the state machine of `AbstractCoordinator`/`ConsumerCoordinator` of the Java client of 0.9.0.1, reduced
 * to what a client without threads can do:
 *
 * ```
 *   GroupCoordinator  -> the broker that coordinates the group, cached until it refuses a request
 *   JoinGroup         -> member id and generation; the leader also receives the metadata of every member
 *      leader:   run the assignor over the subscriptions -> SyncGroup with one assignment per member
 *      follower: SyncGroup with an empty assignment      -> its own share, once the leader has published one
 *   Heartbeat         -> every `heartbeat.interval.ms`, driven by KafkaConsumer::poll()
 *   LeaveGroup        -> on unsubscribe()/close(), so that the group rebalances right away
 * ```
 *
 * The error codes of the protocol are what drives it: 27 (RebalanceInProgress) and 22 (IllegalGeneration) ask for
 * a rejoin with the member id of the previous generation, 25 (UnknownMemberId) for a rejoin without one, and 15/16
 * (GroupCoordinatorNotAvailable, NotCoordinatorForGroup) for another coordinator lookup.
 *
 * Every JoinGroup carries the `rebalance_timeout` of Kafka 0.10.1 - `max.poll.interval.ms` - which is how long the
 * coordinator waits for this member to rejoin a rebalance. Nothing else of KIP-62 applies to a client without
 * threads: the Java consumer leaves the group by itself when the application does not come back to poll() in time,
 * this one simply stops sending heartbeats and is dropped when its session timeout expires.
 *
 * @see docs/protocol/0.10.2.md, sections "Group membership protocol (keys 11 to 14)" and "Consumer group protocol"
 * @see \Protocol\Kafka\Consumer\KafkaConsumer::poll()
 */
final class ConsumerCoordinator
{
    /**
     * Protocol type of a consumer group, the only one Kafka itself defines
     */
    public const string PROTOCOL_TYPE = 'consumer';

    /**
     * How many times a rebalance is restarted before the error of the coordinator is reported to the caller
     *
     * A rebalance that is interrupted by another one - a member joining or leaving while this one syncs - is not a
     * failure, the group simply moved on and the member has to join again; the same holds for a coordinator that
     * moved to another broker. The attempts are bounded so that a client can never spin forever.
     */
    private const int MAX_REBALANCE_ATTEMPTS = 5;

    /**
     * Member id of this consumer, {@see JoinGroupRequest::DEFAULT_MEMBER_ID} until the coordinator assigned one
     */
    private string $memberId = JoinGroupRequest::DEFAULT_MEMBER_ID;

    /**
     * Generation of the group, {@see OffsetCommitRequest::DEFAULT_GENERATION_ID} for a consumer without membership
     */
    private int $generationId = OffsetCommitRequest::DEFAULT_GENERATION_ID;

    /**
     * Whether the next poll() has to run a rebalance before it fetches anything
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
     * @param Client                     $client              Low-level client that speaks the four apis
     * @param string                     $groupId             Name of the consumer group
     * @param PartitionAssignorInterface $assignor            Assignor this member offers as its group protocol
     * @param int                        $heartbeatIntervalMs `heartbeat.interval.ms`, the poll-driven interval
     * @param int                        $retryBackoffMs      `retry.backoff.ms`, waited before a rebalance retry
     * @param int|null                   $rebalanceTimeoutMs  `max.poll.interval.ms`, the `rebalance_timeout` of the
     *        JoinGroup v1 request, null to let the client take it from its own configuration
     */
    public function __construct(
        private readonly Client $client,
        private readonly string $groupId,
        private readonly PartitionAssignorInterface $assignor,
        private readonly int $heartbeatIntervalMs,
        private readonly int $retryBackoffMs = 100,
        private readonly ?int $rebalanceTimeoutMs = null
    ) {}

    /**
     * Returns the member id the coordinator assigned, an empty string for a consumer that is not a member
     */
    public function getMemberId(): string
    {
        return $this->memberId;
    }

    /**
     * Returns the generation of the group, -1 for a consumer that is not a member of one
     */
    public function getGenerationId(): int
    {
        return $this->generationId;
    }

    /**
     * Tells whether this consumer holds a member id of the group
     */
    public function isMember(): bool
    {
        return $this->memberId !== JoinGroupRequest::DEFAULT_MEMBER_ID;
    }

    /**
     * Tells whether the membership has to be established again before the consumer may fetch
     */
    public function needsRejoin(): bool
    {
        return $this->rejoinNeeded;
    }

    /**
     * Asks for a rebalance on the next poll(), e.g. because the subscription changed
     */
    public function requestRejoin(): void
    {
        $this->rejoinNeeded = true;
    }

    /**
     * Returns the broker that coordinates this group, looking it up once and keeping it
     */
    public function getNode(): Node
    {
        return $this->node ??= $this->client->getGroupCoordinator($this->groupId);
    }

    /**
     * Forgets the cached coordinator, so that the next request looks it up again
     */
    public function invalidateNode(): void
    {
        $this->node = null;
    }

    /**
     * Runs the membership protocol until this member holds an assignment of the current generation.
     *
     * @param list<string>                                     $topics             Subscription of this member
     * @param Closure(list<string>): array<string, list<int>>  $partitionsResolver Partition ids of the topics the
     *        members of the group subscribed to, which only the leader of the generation asks for
     *
     * @return array<string, list<int>> Partitions of this member, by topic; empty when it got none
     *
     * @throws KafkaException when the coordinator kept refusing the rebalance
     */
    public function ensureActiveGroup(array $topics, Closure $partitionsResolver): array
    {
        for ($attempt = 1; ; $attempt++) {
            try {
                return $this->rebalance($topics, $partitionsResolver);
            } catch (RebalanceInProgressException | IllegalGenerationException $exception) {
                // Another member joined or left while this one was syncing: the generation is gone, join again
                $this->rejoinNeeded = true;
            } catch (UnknownMemberIdException $exception) {
                // The coordinator does not know this member any more, so it has to join without a member id
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
     * Sends a heartbeat once `heartbeat.interval.ms` has elapsed since the last one.
     *
     * PHP has no background thread, so this is called from the poll loop; the answer of the coordinator is what
     * tells the member that the group is rebalancing (27), that its generation is over (22) or that it was dropped
     * after a missed session timeout (25). All three are answered with a rejoin on this very poll, they are not
     * reported to the application.
     *
     * @param int $nowMs Current time in milliseconds
     *
     * @return bool Whether a heartbeat was sent
     */
    public function maybeHeartbeat(int $nowMs): bool
    {
        if (!$this->isMember() || $this->rejoinNeeded) {
            return false;
        }
        if ($nowMs - $this->lastHeartbeatMs < $this->heartbeatIntervalMs) {
            return false;
        }

        $this->lastHeartbeatMs = $nowMs;

        try {
            $this->client->heartbeat($this->getNode(), $this->groupId, $this->memberId, $this->generationId);
        } catch (RebalanceInProgressException | IllegalGenerationException) {
            $this->rejoinNeeded = true;
        } catch (UnknownMemberIdException) {
            $this->resetMembership();
        } catch (GroupCoordinatorNotAvailableException | NotCoordinatorForGroupException $exception) {
            $this->invalidateNode();
            $this->rejoinNeeded = true;
        }

        return true;
    }

    /**
     * Removes this member from the group, which makes the group rebalance right away
     *
     * A member the coordinator does not know any more - it was dropped after a missed session timeout, or the
     * whole group is gone - answers 25 (UnknownMemberId), which is the state this method wants to reach anyway and
     * is therefore not reported.
     */
    public function leaveGroup(): void
    {
        if (!$this->isMember()) {
            return;
        }

        $memberId = $this->memberId;
        $this->resetMembership();

        try {
            $this->client->leaveGroup($this->getNode(), $this->groupId, $memberId);
        } catch (UnknownMemberIdException) {
            // The coordinator has already forgotten this member, which is exactly what was asked for
        }
    }

    /**
     * Forgets the member id and the generation, so that the next rebalance joins as a new member
     */
    public function resetMembership(): void
    {
        $this->memberId     = JoinGroupRequest::DEFAULT_MEMBER_ID;
        $this->generationId = OffsetCommitRequest::DEFAULT_GENERATION_ID;
        $this->rejoinNeeded = true;
    }

    /**
     * One pass of the protocol: JoinGroup, the assignment of the leader, SyncGroup.
     *
     * @param list<string>                                    $topics             Subscription of this member
     * @param Closure(list<string>): array<string, list<int>> $partitionsResolver Partitions of the group's topics
     *
     * @return array<string, list<int>> Partitions of this member, by topic
     */
    private function rebalance(array $topics, Closure $partitionsResolver): array
    {
        $node         = $this->getNode();
        $subscription = $this->assignor->subscription($topics);

        $joinResponse = $this->client->joinGroup(
            $node,
            $this->groupId,
            $this->memberId,
            self::PROTOCOL_TYPE,
            [$this->assignor->name() => $subscription->pack()],
            $this->rebalanceTimeoutMs
        );

        $this->memberId     = $joinResponse->memberId;
        $this->generationId = $joinResponse->generationId;

        $groupAssignments = [];
        if ($joinResponse->memberId === $joinResponse->leaderId) {
            $groupAssignments = $this->assignPartitions($joinResponse->members, $partitionsResolver);
        }

        $syncResponse = $this->client->syncGroup(
            $node,
            $this->groupId,
            $this->memberId,
            $this->generationId,
            $groupAssignments
        );

        $this->rejoinNeeded    = false;
        $this->lastHeartbeatMs = (int) (microtime(true) * 1e3);

        return self::readAssignment($syncResponse->memberAssignment);
    }

    /**
     * Computes the assignment of the whole generation, which only the leader of the group does
     *
     * The `members` array of a JoinGroup answer comes out of the internal map of the coordinator and carries no
     * order at all, while both built-in assignors are defined on sorted member ids, so it is sorted here before
     * the assignor sees it - a PHP leader hands out the partitions a Java leader of the same generation would.
     *
     * @param array<string, JoinGroupResponseMember>          $members            Members and their metadata
     * @param Closure(list<string>): array<string, list<int>> $partitionsResolver Partitions of the group's topics
     *
     * @return array<string, string> Packed {@see MemberAssignment} of every member, by member id
     */
    private function assignPartitions(array $members, Closure $partitionsResolver): array
    {
        $subscriptions   = [];
        $subscribedTopics = [];
        foreach ($members as $memberId => $member) {
            $memberSubscription           = Subscription::unpack($member->metadata);
            $subscriptions[$memberId]     = $memberSubscription;
            $subscribedTopics             = array_merge($subscribedTopics, $memberSubscription->topics);
        }
        ksort($subscriptions);

        $partitionsPerTopic = $partitionsResolver(array_values(array_unique($subscribedTopics)));

        $groupAssignments = [];
        foreach ($this->assignor->assign($partitionsPerTopic, $subscriptions) as $memberId => $assignment) {
            $groupAssignments[$memberId] = $assignment->pack();
        }

        // A member the leader did not mention receives an empty assignment from the coordinator anyway, but the
        // Java leader publishes one for every member of the generation and so does this one
        foreach (array_keys($subscriptions) as $memberId) {
            $groupAssignments[$memberId] ??= new MemberAssignment()->pack();
        }

        return $groupAssignments;
    }

    /**
     * Decodes the share of this member out of the SyncGroup answer
     *
     * An error answer of the api carries an empty byte array, and so does the answer of a member that the leader
     * did not mention on a coordinator that predates `GroupCoordinator.doSyncGroup()` filling those in; both mean
     * "no partitions" and must not be decoded as a structure, {@see MemberAssignment::unpack()} would fail on them.
     *
     * @return array<string, list<int>>
     */
    private static function readAssignment(string $memberAssignment): array
    {
        if ($memberAssignment === '') {
            return [];
        }

        return MemberAssignment::unpack($memberAssignment)->partitions();
    }
}
