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

namespace Protocol\Kafka\Protocol\Request;

/**
 * AlterPartitionReassignments answer of version 0, the frame of Kafka 2.4 (KIP-455)
 *
 * The throttle time, the top-level error and the result of every partition, without the
 * `allow_replication_factor_change` the version 1 of Kafka 4.1 repeats from the request.
 *
 * @see docs/protocol/4.3.md, section "AlterPartitionReassignments API (key 45, v0 and v1)"
 */
final class AlterPartitionReassignmentsResponseV0 extends AlterPartitionReassignmentsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
