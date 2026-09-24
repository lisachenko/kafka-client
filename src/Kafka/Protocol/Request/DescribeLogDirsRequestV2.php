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
 * DescribeLogDirs request of version 2, the first flexible one of the api (Kafka 2.6, KIP-482)
 *
 * Kafka 3.2 added the version 3 and changed no field of the request - "Version 3 is the same as version 2 (new
 * field in response)" of `DescribeLogDirsRequest.json` @ 3.2.3 - so this class only lowers the version constant:
 * the version 2 is what a broker below Kafka 3.2 is asked with, and its answer has no top-level error code
 * ({@see DescribeLogDirsResponseV2}).
 *
 * @see docs/protocol/4.3.md, section "DescribeLogDirs API (key 35, v0 to v5)"
 */
final class DescribeLogDirsRequestV2 extends DescribeLogDirsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
