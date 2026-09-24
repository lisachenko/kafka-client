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

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\FencedMemberEpochException;
use Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException;
use Protocol\Kafka\Common\Errors\GroupIdNotFoundException;
use Protocol\Kafka\Common\Errors\GroupLoadInProgressException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Protocol\Request\ShareGroupHeartbeatResponse;

/**
 * Membership of one share group, spoken with ShareGroupHeartbeat (key 76, KIP-932)
 *
 * `ShareMembershipManager` and `ShareHeartbeatRequestManager` of the Java client @ 4.3.1, without the thread they run
 * on: {@see \Protocol\Kafka\Consumer\KafkaShareConsumer::poll()} calls {@see self::poll()}, which joins, heartbeats
 * and reads the assignment in the caller's thread.
 *
 * ```
 *   ShareGroupHeartbeat(member id, epoch 0, subscription)   -> join: the epoch, the interval, an assignment
 *   ShareGroupHeartbeat(member id, epoch, nulls)            -> stay alive: a changed assignment, or null
 *   ShareGroupHeartbeat(member id, epoch -1)                -> leave
 * ```
 *
 * **A share member owns nothing.** The assignment names partitions the member reads *next to* the others - the
 * `simple` assignor of the coordinator hands every partition of a subscribed topic to every member while the group
 * is smaller than the partitions are many - and nothing is revoked or acknowledged: there is no reconciliation, no
 * rebalance listener and no owned partitions in the api. The assignment of an answer replaces the one before; a null
 * one means "unchanged".
 *
 * **The member id is the member's own**, generated once with {@see ConsumerGroupHeartbeatCoordinator::newMemberId()}
 * - the `Uuid.randomUuid().toString()` of `AbstractMembershipManager` - and kept for the life of the consumer: a
 * member the coordinator fenced (**110** `FencedMemberEpoch`, **25** `UnknownMemberId`, and the **69**
 * `GroupIdNotFound` of a group that was deleted under it) joins again with the epoch 0 under the same id.
 *
 * **The interval is the coordinator's**, `group.share.heartbeat.interval.ms` (5000 on the node of this line), sent in
 * every answer. The coordinator assigns the partitions of a fresh member on a *later* heartbeat, once it has
 * initialised their share state (measured: the join answers a present but empty assignment, the partitions follow
 * 0.5 to 1.5 seconds later); until the member has partitions it heartbeats every
 * {@see self::UNASSIGNED_HEARTBEAT_INTERVAL_MS} instead, so that the first poll() does not wait a whole interval for
 * them. The session timeout is the broker's, `group.share.session.timeout.ms` (45000): a consumer that does not poll
 * for that long is dropped from its group, and its next poll() joins it again.
 *
 * @see docs/protocol/4.3.md, section "The share consumer (KIP-932)"
 * @see docs/protocol/4.3.md, section "ShareGroupHeartbeat API (key 76, v1)"
 */
final class ShareMembershipManager
{
    /**
     * Heartbeat interval of a member that has no partition yet, in milliseconds
     */
    public const int UNASSIGNED_HEARTBEAT_INTERVAL_MS = 500;

    /**
     * Interval used until the first answer of the coordinator dictates one, in milliseconds
     */
    private const int DEFAULT_HEARTBEAT_INTERVAL_MS = 5000;

    /**
     * Epoch of this member, 0 while it is not in its group
     */
    private int $memberEpoch = 0;

    /**
     * Assignment of the last answer that carried one, as the raw 16 bytes of the topic id => its partitions
     *
     * @var array<string, list<int>>
     */
    private array $assignment = [];

    /**
     * Subscription the coordinator knows of this member, null until it named one
     *
     * @var list<string>|null
     */
    private ?array $subscribedTopics = null;

    /**
     * Interval the coordinator dictates, in milliseconds
     */
    private int $heartbeatIntervalMs = self::DEFAULT_HEARTBEAT_INTERVAL_MS;

    /**
     * Moment of the last heartbeat, in milliseconds
     */
    private int $lastHeartbeatMs = 0;

    /**
     * Coordinator of the group, resolved on the first use and dropped when a broker refuses to be it
     */
    private ?Node $node = null;

    /**
     * @param Client      $client   Low-level client that speaks the api
     * @param Cluster     $cluster  Metadata of the cluster, which names the topic ids of an assignment
     * @param string      $groupId  Name of the share group
     * @param string      $memberId Member id of this consumer, kept for its whole life
     * @param string|null $rackId   `client.rack` of this consumer, null when it names none
     */
    public function __construct(
        private readonly Client $client,
        private readonly Cluster $cluster,
        private readonly string $groupId,
        private readonly string $memberId,
        private readonly ?string $rackId = null
    ) {}

    /**
     * Returns the member id of this consumer
     */
    public function memberId(): string
    {
        return $this->memberId;
    }

    /**
     * Returns the epoch of this member, 0 while it is not in its group
     */
    public function memberEpoch(): int
    {
        return $this->memberEpoch;
    }

    /**
     * Tells whether this member is in its group
     */
    public function isMember(): bool
    {
        return $this->memberEpoch > 0;
    }

    /**
     * Returns the interval the coordinator dictated in its last answer, in milliseconds
     */
    public function heartbeatIntervalMs(): int
    {
        return $this->heartbeatIntervalMs;
    }

    /**
     * Joins the group, sends the subscription that changed, or heartbeats when the interval has elapsed
     *
     * A fenced member joins again on the next call; a coordinator that moved is looked up again on the next call.
     * Every other error of the coordinator reaches the caller.
     *
     * @param int          $nowMs  Current time, in milliseconds
     * @param list<string> $topics Subscription of this member
     */
    public function poll(int $nowMs, array $topics): void
    {
        try {
            if (!$this->isMember()) {
                $this->apply($this->client->joinShareGroup(
                    $this->node(),
                    $this->groupId,
                    $this->memberId,
                    $topics,
                    $this->rackId
                ));
                $this->subscribedTopics = $topics;
            } elseif ($this->subscribedTopics !== $topics) {
                // A subscription that changed travels in an ordinary heartbeat: there is nothing to re-join for
                $this->apply($this->client->shareGroupHeartbeat(
                    $this->node(),
                    $this->groupId,
                    $this->memberId,
                    $this->memberEpoch,
                    $topics
                ));
                $this->subscribedTopics = $topics;
            } elseif ($nowMs - $this->lastHeartbeatMs >= $this->currentIntervalMs()) {
                $this->apply($this->client->shareGroupHeartbeat(
                    $this->node(),
                    $this->groupId,
                    $this->memberId,
                    $this->memberEpoch
                ));
            }
        } catch (FencedMemberEpochException | UnknownMemberIdException | GroupIdNotFoundException) {
            // The coordinator fenced this member, or forgot the group: it joins again under the same id
            $this->resetMembership();
        } catch (
            GroupCoordinatorNotAvailableException
            | NotCoordinatorForGroupException
            | GroupLoadInProgressException
        ) {
            $this->node = null;
        }
    }

    /**
     * Returns the partitions assigned to this member, by topic name
     *
     * The api names a topic by the 16 raw bytes of its id alone; an id the metadata of this client does not know yet
     * - a topic created after the last refresh - costs one reload of the metadata, and an id that is still unknown
     * afterwards is left out until the next call.
     *
     * @return array<string, list<int>> Topic name => partitions
     */
    public function assignedPartitions(): array
    {
        $names = $this->namesOf($this->assignment);
        if (count($names) !== count($this->assignment)) {
            $this->cluster->reload();
            $names = $this->namesOf($this->assignment);
        }

        return $names;
    }

    /**
     * Takes this member out of its group with the heartbeat of the epoch -1
     *
     * A member the coordinator has forgotten already has nothing to leave, which is not an error.
     */
    public function leaveGroup(): void
    {
        if (!$this->isMember()) {
            return;
        }
        $this->resetMembership();

        try {
            $this->client->leaveShareGroup($this->node(), $this->groupId, $this->memberId);
        } catch (UnknownMemberIdException | FencedMemberEpochException | GroupIdNotFoundException) {
            // The coordinator has already forgotten this member, which is exactly what was asked for
        }
    }

    /**
     * Forgets the membership, so that the next poll joins again with the epoch 0 under the same member id
     */
    public function resetMembership(): void
    {
        $this->memberEpoch      = 0;
        $this->assignment       = [];
        $this->subscribedTopics = null;
    }

    /**
     * Returns the coordinator of the group, looked up on the first use
     */
    private function node(): Node
    {
        return $this->node ??= $this->client->getGroupCoordinator($this->groupId);
    }

    /**
     * Returns the interval of the next heartbeat: the coordinator's, or the short one of a member without partitions
     */
    private function currentIntervalMs(): int
    {
        if ($this->assignment === []) {
            return min(self::UNASSIGNED_HEARTBEAT_INTERVAL_MS, $this->heartbeatIntervalMs);
        }

        return $this->heartbeatIntervalMs;
    }

    /**
     * Reads what an answer of the coordinator changed: the epoch, the interval and the assignment
     */
    private function apply(ShareGroupHeartbeatResponse $response): void
    {
        $this->lastHeartbeatMs = (int) (microtime(true) * 1e3);
        $this->memberEpoch     = $response->memberEpoch;
        if ($response->heartbeatIntervalMs > 0) {
            $this->heartbeatIntervalMs = $response->heartbeatIntervalMs;
        }

        // A null assignment means "nothing changed"; an assignment with an EMPTY array means "no partition"
        if ($response->assignment !== null) {
            $assignment = [];
            foreach ($response->assignment->partitionsByTopicId() as $topicId => $partitions) {
                if ($partitions !== []) {
                    $assignment[(string) $topicId] = array_values(array_map(intval(...), $partitions));
                }
            }
            $this->assignment = $assignment;
        }
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
}
