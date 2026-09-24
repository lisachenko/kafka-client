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
 * One log directory of a DescribeLogDirs answer of version 4, i.e. with the two sizes of KIP-827 and no cordon flag
 *
 * Kafka 4.3 appended `is_cordoned` to the directory entry ({@see DescribeLogDirsResponseLogDir}), behind the
 * `total_bytes` and `usable_bytes` of Kafka 3.3; this class is the entry of the version 4, which ends with the two
 * sizes and leaves {@see DescribeLogDirsResponseLogDir::$isCordoned} at the `false` of the default.
 * {@see DescribeLogDirsResponseLogDirV3} is the entry without the two sizes either.
 *
 * @see docs/protocol/4.3.md, section "DescribeLogDirs API (key 35, v0 to v5)"
 */
final class DescribeLogDirsResponseLogDirV4 extends DescribeLogDirsResponseLogDir
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
