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

use Protocol\Kafka\Protocol\Data\ConsumerGroupDescribeMember;

/**
 * One member of a KIP-848 consumer group, as {@see AdminClient::describeConsumerGroups()} reports it
 *
 * `MemberDescription` of the Java admin client @ 3.9.2 as the new consumer protocol fills it: next to the member
 * id, the instance id and the host of a classic member it carries the **member epoch** in place of a generation,
 * the subscription as plain topic names - a classic DescribeGroups answer carries the packed metadata of the
 * assignor instead - and **two** assignments, the one the member owns and the one it is meant to own.
 *
 * While {@see self::$assignment} and {@see self::$targetAssignment} differ, this member is still in the middle of
 * a reconciliation: it has been told to give partitions up or to take partitions over and has not acknowledged
 * the change yet. {@see self::isReconciled()} is that question.
 *
 * @see docs/protocol/3.9.md, section "ConsumerGroupDescribe API (key 69, v0)"
 */
final class ConsumerGroupMemberDescription
{
    /**
     * @param string                   $memberId             Member id, the uuid the consumer generated for itself
     * @param string|null              $instanceId           `group.instance.id` of a static member
     * @param string|null              $rackId               `client.rack` of the member (KIP-881)
     * @param int                      $memberEpoch          Epoch this member has caught up with
     * @param string                   $clientId             `client.id` of the consumer
     * @param string                   $clientHost           Host it connected from
     * @param list<string>             $subscribedTopicNames Topics it subscribed to by name
     * @param string|null              $subscribedTopicRegex Pattern it subscribed with, null when it named topics
     * @param array<string, list<int>> $assignment           Partitions it owns, by topic name
     * @param array<string, list<int>> $targetAssignment     Partitions it is meant to own, by topic name
     */
    public function __construct(
        public readonly string $memberId,
        public readonly ?string $instanceId,
        public readonly ?string $rackId,
        public readonly int $memberEpoch,
        public readonly string $clientId,
        public readonly string $clientHost,
        public readonly array $subscribedTopicNames,
        public readonly ?string $subscribedTopicRegex,
        public readonly array $assignment,
        public readonly array $targetAssignment
    ) {}

    /**
     * Builds the description out of the member entry of a ConsumerGroupDescribe answer
     */
    public static function fromMember(ConsumerGroupDescribeMember $member): self
    {
        return new self(
            $member->memberId,
            $member->instanceId,
            $member->rackId,
            $member->memberEpoch,
            $member->clientId,
            $member->clientHost,
            $member->subscribedTopicNames,
            $member->subscribedTopicRegex,
            $member->assignment->partitionsByTopic(),
            $member->targetAssignment->partitionsByTopic()
        );
    }

    /**
     * Tells whether this member owns exactly what the coordinator wants it to own
     */
    public function isReconciled(): bool
    {
        return self::normalized($this->assignment) === self::normalized($this->targetAssignment);
    }

    /**
     * Tells whether this member is a static one (KIP-345)
     */
    public function isStatic(): bool
    {
        return $this->instanceId !== null;
    }

    /**
     * Sorts an assignment by topic and partition so that two of them can be compared
     *
     * @param array<string, list<int>> $assignment
     *
     * @return array<string, list<int>>
     */
    private static function normalized(array $assignment): array
    {
        $normalized = [];
        foreach ($assignment as $topic => $partitions) {
            sort($partitions);
            $normalized[$topic] = array_values($partitions);
        }
        ksort($normalized);

        return $normalized;
    }
}
