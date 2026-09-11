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
 * InitProducerId request of version 3, the frame of version 4 with a lower version field
 *
 * "Version 4 adds the support for new error code PRODUCER_FENCED" (`InitProducerIdRequest.json` @ 2.7.2): the
 * version Kafka 2.7 added with KIP-588 carries the same four fields, and what it changes is the **answer** of a
 * producer whose epoch the coordinator has left behind - the code **90** `ProducerFenced` instead of the 47
 * `InvalidProducerEpoch` the version 3 is answered with.
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0 to v4)"
 */
final class InitProducerIdRequestV3 extends InitProducerIdRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
