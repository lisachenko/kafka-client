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
 * InitProducerId answer of version 3, the frame of version 4 with a lower version field
 *
 * The version 4 of KIP-588 adds no field: the same four values, with the error code 90 `ProducerFenced` where a
 * version 3 answer carries the 47.
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0 to v4)"
 */
final class InitProducerIdResponseV3 extends InitProducerIdResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
