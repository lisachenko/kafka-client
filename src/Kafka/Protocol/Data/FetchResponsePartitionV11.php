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
 * Partition entry of a Fetch answer of version 11
 *
 * The entry of version 11 (Kafka 2.3, KIP-392), i.e. the one with the `preferred_read_replica` and **without**
 * the three tagged fields that version 12 (Kafka 2.7) added to a partition - the diverging epoch of KIP-595, the
 * current leader and the snapshot id of KIP-630, see {@see FetchResponsePartition}. A version 11 answer is not
 * flexible and has no tagged-field section to carry them in.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v12)"
 */
final class FetchResponsePartitionV11 extends FetchResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 11;
}
