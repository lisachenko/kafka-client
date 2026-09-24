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
 * DescribeLogDirs answer of version 3, the frame before the two sizes of KIP-827
 *
 * The version 3 of Kafka 3.2 added the top-level error code and the version 4 of Kafka 3.3 the `total_bytes` and
 * `usable_bytes` of the **volume** a log directory sits on ({@see DescribeLogDirsResponse}), which are the last
 * two fields of a directory entry, behind its topics. This class keeps the entry without them - its directories
 * are read through {@see \Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseLogDirV3} - and
 * {@see DescribeLogDirsResponseV2} keeps the frame that has no top-level error code either.
 *
 * @see docs/protocol/4.3.md, section "DescribeLogDirs API (key 35, v0 to v5)"
 */
final class DescribeLogDirsResponseV3 extends DescribeLogDirsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
