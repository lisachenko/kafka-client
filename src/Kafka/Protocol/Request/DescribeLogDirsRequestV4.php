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
 * DescribeLogDirs request of version 4, the version that asks for the two volume sizes of KIP-827 (Kafka 3.3)
 *
 * Kafka 4.3 added the version 5 and changed no field of the request - "Version 5 is the same as version 2 (new
 * fields in response)" of `DescribeLogDirsRequest.json` @ 4.3.1 - so this class only lowers the version constant:
 * the version 4 is what a broker below Kafka 4.3 is asked with, and its answer carries no cordon flag per
 * directory ({@see DescribeLogDirsResponseV4}).
 *
 * @see docs/protocol/4.3.md, section "DescribeLogDirs API (key 35, v0 to v5)"
 */
final class DescribeLogDirsRequestV4 extends DescribeLogDirsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
