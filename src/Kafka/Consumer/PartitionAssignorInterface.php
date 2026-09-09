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

namespace Protocol\Kafka\Consumer;

/**
 * Custom partition assignment for the group management of {@see KafkaConsumer}.
 *
 * Members of a consumer group subscribe to the topics they are interested in and forward their subscriptions to the
 * broker that serves as the group coordinator. The coordinator selects one member to perform the group assignment
 * and propagates the subscriptions of all members to it; that member calls {@see assign()} and publishes the result
 * with a SyncGroup request, from which every other member picks its own share up.
 *
 * Every member of a group has to use the same assignor: {@see name()} is what the JoinGroup request advertises as
 * the group protocol, and a coordinator that cannot find a protocol all members support answers the join with the
 * error 23 (InconsistentGroupProtocol).
 *
 * In some cases it is useful to forward additional metadata to the assignor of the leader. For this, an
 * implementation can put anything into the `userData` of the {@see Subscription} that {@see subscription()} returns -
 * a rack id, the number of cpus of the machine hosting the member, the assignment of the previous generation.
 *
 * Unlike the `PartitionAssignor` of the Java client, the assignment does not receive the cluster metadata but the
 * partition counts of the subscribed topics that the caller has already read from it, so that an assignor is a pure
 * function of its arguments and can be unit tested without a broker.
 *
 * @see \Protocol\Kafka\Consumer\AbstractPartitionAssignor for the base class of the built-in assignors
 * @see docs/protocol/0.9.0.md, section "Consumer group protocol (protocol_type = consumer)"
 */
interface PartitionAssignorInterface
{
    /**
     * Returns the unique wire name of this assignor, e.g. `range` or `roundrobin`
     *
     * The name is what the members of a group agree on: it goes into the `protocol_name` of the JoinGroup request
     * and the coordinator names it back in the JoinGroup response and in DescribeGroups.
     */
    public function name(): string;

    /**
     * Returns the metadata of the local member for the given topics, the `member_metadata` of a JoinGroup request
     *
     * @param list<string> $topics Topics that {@see KafkaConsumer::subscribe()} was called with
     */
    public function subscription(array $topics): Subscription;

    /**
     * Performs the group assignment for every member of the group
     *
     * @param array<string, int|list<int>> $partitionsPerTopic Number of partitions, or the partition ids themselves,
     *                                                         for each subscribed topic that the cluster metadata
     *                                                         knows; a topic that is missing here is skipped
     * @param array<string, Subscription>  $subscriptions      Subscription of every member, indexed by its member id
     *
     * @return array<string, MemberAssignment> Assignment for every member of the input, indexed by its member id
     */
    public function assign(array $partitionsPerTopic, array $subscriptions): array;
}
