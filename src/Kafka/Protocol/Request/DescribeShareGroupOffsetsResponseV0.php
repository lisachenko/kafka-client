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
 * DescribeShareGroupOffsets answer of version 0 (Kafka 4.1, KIP-932): the start offsets, without the lag
 *
 * The frame of {@see DescribeShareGroupOffsetsResponse} with eight bytes less in every partition entry: version 1
 * (Kafka 4.2, KIP-1226) put the `lag` behind the leader epoch, and this is the answer below it, whose groups are
 * {@see \Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsResponseGroupV0} entries.
 *
 * @see docs/protocol/4.3.md, section "The share-partition lag of KIP-1226 (v1)"
 */
final class DescribeShareGroupOffsetsResponseV0 extends DescribeShareGroupOffsetsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
