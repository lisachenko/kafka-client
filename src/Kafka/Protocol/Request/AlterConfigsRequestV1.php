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
 * AlterConfigs request of version 1, the body of version 2 in the encoding before KIP-482
 *
 * Kafka 2.8 made the version 2 the first **flexible** one of this api ("Version 2 enables flexible versions" in
 * `AlterConfigsRequest.json` @ 2.8.2) and changed no field, so this class only lowers the version constant: the version
 * 1 is the frame a broker below Kafka 2.8 speaks.
 *
 * @see docs/protocol/2.8.md, section "AlterConfigs API (key 33, v0 to v2)"
 */
final class AlterConfigsRequestV1 extends AlterConfigsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
