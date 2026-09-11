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
 * DescribeLogDirs request of version 1, the body of version 2 in the encoding before KIP-482
 *
 * Kafka 2.6 made the version 2 the first **flexible** one of this api and changed no field, so this class only
 * lowers the version constant: the version 1 is the frame a broker below Kafka 2.6 speaks, and the one Kafka 2.0
 * added for KIP-219 - `DESCRIBE_LOG_DIRS_REQUEST_V1 = DESCRIBE_LOG_DIRS_REQUEST_V0` in `Protocol.java` @ 2.0.1,
 * so {@see DescribeLogDirsRequestV0} is these very bytes with a lower version field.
 *
 * @see docs/protocol/2.8.md, section "DescribeLogDirs API (key 35, v0 to v2)"
 */
final class DescribeLogDirsRequestV1 extends DescribeLogDirsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
