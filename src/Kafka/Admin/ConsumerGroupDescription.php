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

namespace Protocol\Kafka\Admin;

use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Protocol\Data\ConsumerGroupDescribedGroup;

/**
 * One group of the **new consumer protocol**, as {@see AdminClient::describeConsumerGroups()} reports it
 *
 * `ConsumerGroupDescription` of the Java admin client @ 3.9.2, filled from a ConsumerGroupDescribe answer (key
 * 69). It is the counterpart of the {@see \Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata} that
 * {@see AdminClient::describeGroups()} answers for a **classic** group, and the two apis never describe the same
 * group: a classic group asked of key 69 is a **69** `GroupIdNotFound`, and a KIP-848 group asked of key 15 is
 * one as well.
 *
 * What it carries that a classic description cannot:
 *
 * * the **two epochs of the group** - {@see self::$groupEpoch}, bumped whenever the input of the assignment
 *   changed, and {@see self::$assignmentEpoch}, the group epoch the current target assignment was computed for;
 * * the **name of the server-side assignor** the coordinator ran, which is a broker-side choice in this protocol;
 * * per member the subscription as plain topic names and **both** assignments, see
 *   {@see ConsumerGroupMemberDescription}.
 *
 * The **state** is one of the `STATE_*` constants of {@see ConsumerGroupDescribedGroup}: `Empty`, `Assigning`,
 * `Reconciling`, `Stable` or `Dead`. There is no `PreparingRebalance` and no `CompletingRebalance` in this
 * protocol - nothing stops the world - and {@see self::isStable()} is the question a tool really asks.
 *
 * @see docs/protocol/4.3.md, section "ConsumerGroupDescribe API (key 69, v0 and v1)"
 */
final class ConsumerGroupDescription
{
    /**
     * @param string                                     $groupId         Name of the group
     * @param string                                     $state           State of the group
     * @param int                                        $groupEpoch      Epoch of the group
     * @param int                                        $assignmentEpoch Epoch the target assignment belongs to
     * @param string                                     $assignorName    Server-side assignor of the group
     * @param array<string, ConsumerGroupMemberDescription> $members      Members, indexed by their member id
     * @param int                                        $authorizedOperations Acl bit field of KIP-430
     */
    public function __construct(
        public readonly string $groupId,
        public readonly string $state,
        public readonly int $groupEpoch,
        public readonly int $assignmentEpoch,
        public readonly string $assignorName,
        public readonly array $members,
        public readonly int $authorizedOperations = AclOperation::NOT_REQUESTED
    ) {}

    /**
     * Builds the description out of one entry of a ConsumerGroupDescribe answer
     */
    public static function fromDescribedGroup(ConsumerGroupDescribedGroup $group): self
    {
        $members = [];
        foreach ($group->members as $memberId => $member) {
            $members[(string) $memberId] = ConsumerGroupMemberDescription::fromMember($member);
        }

        return new self(
            $group->groupId,
            $group->groupState,
            $group->groupEpoch,
            $group->assignmentEpoch,
            $group->assignorName,
            $members,
            $group->authorizedOperations
        );
    }

    /**
     * Tells whether every member of the group owns exactly what the coordinator wants it to own
     */
    public function isStable(): bool
    {
        return $this->state === ConsumerGroupDescribedGroup::STATE_STABLE;
    }

    /**
     * Returns the partitions of the whole group, by topic name, as its members really own them
     *
     * @return array<string, list<int>>
     */
    public function assignment(): array
    {
        $assignment = [];
        foreach ($this->members as $member) {
            foreach ($member->assignment as $topic => $partitions) {
                $assignment[$topic] = array_merge($assignment[$topic] ?? [], $partitions);
            }
        }
        foreach ($assignment as $topic => $partitions) {
            sort($partitions);
            $assignment[$topic] = array_values($partitions);
        }

        return $assignment;
    }

    /**
     * Returns the operations the principal of the connection may perform on this group
     *
     * @return list<int>
     */
    public function authorizedOperations(): array
    {
        return AclOperation::fromBitField($this->authorizedOperations);
    }
}
