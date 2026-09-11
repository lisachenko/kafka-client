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
 * One partition of a Fetch response of the version 4
 *
 * <pre>
 *   FetchResponsePartition => Partition ErrorCode HighwaterMarkOffset LastStableOffset [AbortedTransactions]
 *                             RecordSetSize RecordSet
 * </pre>
 *
 * Version 4 knows the `LastStableOffset` and the `AbortedTransactions` of the transactional protocol, but not yet
 * the `LogStartOffset` that version 5 inserted between the two, so this class only lowers the version constant
 * that {@see FetchResponsePartition::getScheme()} follows; `$logStartOffset` keeps its default of -1, "the broker
 * reported none".
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v7)"
 */
final class FetchResponsePartitionV4 extends FetchResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
