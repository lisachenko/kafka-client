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
 * One partition of a DescribeShareGroupOffsets answer of version 0 (Kafka 4.1, KIP-932): the start offset, no lag
 *
 * Version 1 (Kafka 4.2, KIP-1226) put the `lag` between the leader epoch and the error code of every partition
 * ({@see DescribeShareGroupOffsetsResponsePartition}); this is the entry below it. Its
 * {@see DescribeShareGroupOffsetsResponsePartition::$lag} stays at the -1 of an unknown lag.
 *
 * @see docs/protocol/4.3.md, section "The share-partition lag of KIP-1226 (v1)"
 */
final class DescribeShareGroupOffsetsResponsePartitionV0 extends DescribeShareGroupOffsetsResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
