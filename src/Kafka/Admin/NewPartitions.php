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

use InvalidArgumentException;

/**
 * The partitions a topic should have after a {@see AdminClient::createPartitions()} call (Kafka 1.0, KIP-195)
 *
 * `org.apache.kafka.clients.admin.NewPartitions` @ 1.1.1, with its two factory methods and no public constructor:
 * `increaseTo(totalCount)` leaves the placement of the new partitions to the controller, and
 * `increaseTo(totalCount, newAssignments)` names the brokers of every partition that is added.
 *
 * <code>
 *   $admin->createPartitions(['events' => NewPartitions::increaseTo(5)]);
 *   $admin->createPartitions(['events' => NewPartitions::increaseTo(5, [[0], [1]])]);
 * </code>
 *
 * `totalCount` is what the topic should have **afterwards**, not the number of partitions to add - the api can only
 * grow a topic, and a count that is not above the current one is answered with the error code 37. `assignments` has
 * therefore one entry per ADDED partition (`totalCount` minus the current count), each of them as many broker ids as
 * the replication factor of the topic; a mismatch is the error code 39.
 *
 * @see docs/protocol/2.8.md, section "CreatePartitions API (key 37, v0 and v1)"
 */
final class NewPartitions
{
    /**
     * @param int                  $totalCount  Number of partitions the topic should have afterwards
     * @param list<list<int>>|null $assignments Brokers of every added partition, null for the controller's choice
     */
    private function __construct(
        public readonly int $totalCount,
        public readonly ?array $assignments = null
    ) {}

    /**
     * Asks for a topic of `$totalCount` partitions, with the placement of the new ones left to the controller
     *
     * @param int                  $totalCount     Number of partitions the topic should have afterwards
     * @param list<list<int>>|null $newAssignments Brokers of every added partition, null for the controller's choice
     *
     * @throws InvalidArgumentException If an assignment list is given but holds no broker for a partition
     */
    public static function increaseTo(int $totalCount, ?array $newAssignments = null): self
    {
        if ($newAssignments !== null) {
            foreach ($newAssignments as $index => $replicas) {
                if ($replicas === []) {
                    throw new InvalidArgumentException(
                        "The assignment of the added partition {$index} names no broker at all"
                    );
                }
            }
            $newAssignments = array_values(array_map(array_values(...), $newAssignments));
        }

        return new self($totalCount, $newAssignments);
    }
}
