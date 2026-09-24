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
 * DescribeLogDirs answer of version 4, the frame before the cordon flag of KIP-1066
 *
 * The version 4 of Kafka 3.3 appended the `total_bytes` and `usable_bytes` of KIP-827 to every directory entry and
 * the version 5 of Kafka 4.3 the `is_cordoned` flag behind them ({@see DescribeLogDirsResponse}), which is the last
 * field of a directory entry. This class keeps the entry without it - its directories are read through
 * {@see \Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseLogDirV4} - and {@see DescribeLogDirsResponseV3}
 * keeps the frame that has no volume sizes either.
 *
 * @see docs/protocol/4.3.md, section "DescribeLogDirs API (key 35, v0 to v5)"
 */
final class DescribeLogDirsResponseV4 extends DescribeLogDirsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
