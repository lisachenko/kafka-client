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
 * The replica set a partition should end up on after an {@see AdminClient::alterPartitionReassignments()} call
 *
 * `org.apache.kafka.clients.admin.NewPartitionReassignment` @ 2.8.2 (Kafka 2.4, KIP-455). The Java client wraps
 * the target replicas in an `Optional`, where the **empty** optional means "cancel the reassignment of this
 * partition"; this client uses a plain `null` for that, so a call reads:
 *
 * <code>
 *   $admin->alterPartitionReassignments([
 *       'events' => [
 *           0 => new NewPartitionReassignment([2, 3]),   // move partition 0 to the brokers 2 and 3
 *           1 => null,                                   // cancel whatever partition 1 is doing
 *       ],
 *   ]);
 * </code>
 *
 * The list is the **whole** replica set the partition should have afterwards, in order - the first broker is the
 * preferred leader - and not the brokers to add; the controller works out what to add and what to remove. An empty
 * list is refused by the broker with the error code 39, so it is refused here.
 *
 * @see docs/protocol/2.8.md, section "AlterPartitionReassignments API (key 45, v0)"
 */
final class NewPartitionReassignment
{
    /**
     * Broker ids the partition should live on afterwards, in order
     *
     * @var list<int>
     */
    public readonly array $targetReplicas;

    /**
     * @param list<int> $targetReplicas Broker ids the partition should live on afterwards
     *
     * @throws InvalidArgumentException If the list is empty, which the broker answers with the error code 39
     */
    public function __construct(array $targetReplicas)
    {
        if ($targetReplicas === []) {
            throw new InvalidArgumentException(
                'A partition reassignment names the whole target replica set and can not be empty; '
                . 'pass null instead to cancel a reassignment that is in progress'
            );
        }

        $this->targetReplicas = array_values(array_map(intval(...), $targetReplicas));
    }
}
