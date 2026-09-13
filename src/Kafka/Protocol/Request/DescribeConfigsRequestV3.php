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
 * DescribeConfigs request of version 3, the body of version 4 in the encoding before KIP-482
 *
 * Kafka 2.8 made the version 4 the first **flexible** one of this api ("Version 4 enables flexible versions" in
 * `DescribeConfigsRequest.json` @ 2.8.2) and changed no field, so this class only lowers the version constant: the version
 * 3 is the frame a broker below Kafka 2.8 speaks.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 to v4)"
 */
final class DescribeConfigsRequestV3 extends DescribeConfigsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
