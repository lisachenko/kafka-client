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

/**
 * A partition reassignment that is in progress, as {@see AdminClient::listPartitionReassignments()} reports it
 *
 * `org.apache.kafka.clients.admin.PartitionReassignment` @ 2.8.2 (Kafka 2.4, KIP-455): the replica set the
 * partition has **right now**, which during a reassignment is the union of the old and the new one, and the two
 * halves that are moving.
 *
 * The target replica set of the reassignment is therefore `replicas` without `removingReplicas`, and the set it
 * started from is `replicas` without `addingReplicas`; {@see self::getTargetReplicas()} and
 * {@see self::getOriginalReplicas()} do that arithmetic.
 *
 * @see docs/protocol/2.8.md, section "ListPartitionReassignments API (key 46, v0)"
 */
final class PartitionReassignment
{
    /**
     * @param string    $topic            Name of the topic
     * @param int       $partition        Index of the partition
     * @param list<int> $replicas         Broker ids the partition lives on at this moment
     * @param list<int> $addingReplicas   Broker ids that are being added
     * @param list<int> $removingReplicas Broker ids that are being removed
     */
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
        public readonly array $replicas,
        public readonly array $addingReplicas,
        public readonly array $removingReplicas
    ) {}

    /**
     * Returns the replica set the partition will have when the reassignment is complete
     *
     * @return list<int>
     */
    public function getTargetReplicas(): array
    {
        return array_values(array_diff($this->replicas, $this->removingReplicas));
    }

    /**
     * Returns the replica set the partition had before the reassignment started
     *
     * @return list<int>
     */
    public function getOriginalReplicas(): array
    {
        return array_values(array_diff($this->replicas, $this->addingReplicas));
    }
}
