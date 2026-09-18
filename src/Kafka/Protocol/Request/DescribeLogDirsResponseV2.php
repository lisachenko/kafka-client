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
 * DescribeLogDirs answer of version 2, the frame before the top-level error code of Kafka 3.2
 *
 * The version 2 of Kafka 2.6 is the first flexible one of the api and adds no field to the version 1; the version
 * 3 of Kafka 3.2 put an `int16` error code of the whole request between `throttle_time_ms` and `log_dirs`
 * ({@see DescribeLogDirsResponse}), so the scheme of this class drops that field again and the refusal a version 3
 * answers with 31 is, here, the empty `log_dirs` array alone. The versions 0 and 1 are the same fields in the
 * encoding before KIP-482 ({@see DescribeLogDirsResponseV1}).
 *
 * @see docs/protocol/3.9.md, section "DescribeLogDirs API (key 35, v0 to v3)"
 */
final class DescribeLogDirsResponseV2 extends DescribeLogDirsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
