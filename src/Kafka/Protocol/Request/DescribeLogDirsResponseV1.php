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
 * DescribeLogDirs answer of version 1, the body of version 2 in the encoding before KIP-482
 *
 * The version 2 of Kafka 2.6 is the first flexible one of the api and adds no field, so this class only lowers the
 * version constant. The answer of the versions 0 and 1 is one and the same frame; what the version 1 changed is
 * *when* a throttled answer arrives, not what is in it (KIP-219).
 *
 * @see docs/protocol/2.8.md, section "DescribeLogDirs API (key 35, v0 to v2)"
 */
final class DescribeLogDirsResponseV1 extends DescribeLogDirsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
