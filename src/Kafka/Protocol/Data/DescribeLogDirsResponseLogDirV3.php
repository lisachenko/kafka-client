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
 * One log directory of a DescribeLogDirs answer of the versions 0 to 3, i.e. without the two sizes of KIP-827
 *
 * Kafka 3.3 appended `total_bytes` and `usable_bytes` to the directory entry ({@see DescribeLogDirsResponseLogDir}),
 * behind its topic array; this class is the entry of every version below 4, where the two fields stay at the
 * {@see DescribeLogDirsResponseLogDir::UNKNOWN_BYTES} of a directory the broker did not measure.
 *
 * @see docs/protocol/3.9.md, section "DescribeLogDirs API (key 35, v0 to v4)"
 */
final class DescribeLogDirsResponseLogDirV3 extends DescribeLogDirsResponseLogDir
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
