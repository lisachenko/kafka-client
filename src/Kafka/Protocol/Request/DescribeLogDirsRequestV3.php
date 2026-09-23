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
 * DescribeLogDirs request of version 3, the version that asks for the top-level error code alone (Kafka 3.2)
 *
 * Kafka 3.3 added the version 4 and changed no field of the request either - "Version 4 is the same as version 2
 * (new fields in response)" of `DescribeLogDirsRequest.json` @ 3.3.2 - so this class only lowers the version
 * constant: the version 3 is what a broker below Kafka 3.3 is asked with, and its answer carries no total and no
 * usable bytes per directory ({@see DescribeLogDirsResponseV3}).
 *
 * @see docs/protocol/4.3.md, section "DescribeLogDirs API (key 35, v0 to v4)"
 */
final class DescribeLogDirsRequestV3 extends DescribeLogDirsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
