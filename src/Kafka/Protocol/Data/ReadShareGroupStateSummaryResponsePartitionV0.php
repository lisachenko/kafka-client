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
 * The summary of one share partition in a ReadShareGroupStateSummary answer of version 0 (Kafka 4.1, KIP-932)
 *
 * <pre>
 *   PartitionResult => Partition ErrorCode ErrorMessage StateEpoch LeaderEpoch StartOffset
 * </pre>
 *
 * The entry of the version below the `DeliveryCompleteCount` that version 1 (Kafka 4.2, KIP-1226) appended to
 * {@see ReadShareGroupStateSummaryResponsePartition}; its
 * {@see ReadShareGroupStateSummaryResponsePartition::$deliveryCompleteCount} keeps the -1 of the default, which is
 * "not known".
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupStateSummary API (key 87, v0 and v1)"
 */
final class ReadShareGroupStateSummaryResponsePartitionV0 extends ReadShareGroupStateSummaryResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
