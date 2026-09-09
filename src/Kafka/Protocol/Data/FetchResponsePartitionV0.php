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
 * One partition of a Fetch response of the versions 0 to 3
 *
 * <pre>
 *   FetchResponsePartition => Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet
 * </pre>
 *
 * The partition entry did not change at all between the versions 0 and 3 - `LastStableOffset`,
 * `AbortedTransactions` (v4) and `LogStartOffset` (v5) all belong to Kafka 0.11 - so this class only lowers the
 * version constant that {@see FetchResponsePartition::getScheme()} follows. The record set of such an answer is a
 * message set of the format v1 (a v2/v3 request) or v0 (a v0/v1 request), because the broker converts a record
 * batch down for every client that asks below version 4.
 *
 * @see docs/protocol/0.11.0.md, section "Fetch API (key 1, v0 to v5)"
 */
final class FetchResponsePartitionV0 extends FetchResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
