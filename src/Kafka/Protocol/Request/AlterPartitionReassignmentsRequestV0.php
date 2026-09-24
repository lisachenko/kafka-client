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
 * AlterPartitionReassignments request of version 0, the frame of Kafka 2.4 (KIP-455)
 *
 * The timeout and the topics, without the `allow_replication_factor_change` Kafka 4.1 put between them: a version
 * 0 request always allows a partition to change its replication factor, which is the default **true** of the field
 * in the version 1. The `$allowReplicationFactorChange` argument of the constructor is therefore not written.
 *
 * @see docs/protocol/4.3.md, section "AlterPartitionReassignments API (key 45, v0 and v1)"
 */
final class AlterPartitionReassignmentsRequestV0 extends AlterPartitionReassignmentsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
