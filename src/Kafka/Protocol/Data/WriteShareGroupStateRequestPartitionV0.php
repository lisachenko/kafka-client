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

namespace Protocol\Kafka\Protocol\Data;

/**
 * One partition of a WriteShareGroupState request of version 0 (Kafka 4.1, KIP-932): the state without the count
 *
 * <pre>
 *   PartitionData => Partition StateEpoch LeaderEpoch StartOffset [StateBatches]
 * </pre>
 *
 * Version 1 (Kafka 4.2, KIP-1226) put the `DeliveryCompleteCount` of {@see WriteShareGroupStateRequestPartition}
 * between the start offset and the batches; this is the entry of the version below it, whose
 * {@see WriteShareGroupStateRequestPartition::$deliveryCompleteCount} never reaches the wire. The share coordinator
 * keeps the -1 of the default for a state written with it.
 *
 * @see docs/protocol/4.3.md, section "WriteShareGroupState API (key 85, v0 and v1)"
 */
final class WriteShareGroupStateRequestPartitionV0 extends WriteShareGroupStateRequestPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
