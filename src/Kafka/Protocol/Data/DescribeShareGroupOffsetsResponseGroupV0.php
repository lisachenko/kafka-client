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
 * One share group of a DescribeShareGroupOffsets answer of version 0 (Kafka 4.1, KIP-932)
 *
 * The fields of {@see DescribeShareGroupOffsetsResponseGroup}, with the topic entries of the version 0
 * ({@see DescribeShareGroupOffsetsResponseTopicV0}), whose partitions carry no `lag`.
 *
 * @see docs/protocol/4.3.md, section "The share-partition lag of KIP-1226 (v1)"
 */
final class DescribeShareGroupOffsetsResponseGroupV0 extends DescribeShareGroupOffsetsResponseGroup
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
