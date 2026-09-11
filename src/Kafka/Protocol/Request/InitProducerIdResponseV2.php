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
 * InitProducerId response of version 2 (Kafka 2.4), byte for byte the answer of version 3
 *
 * <pre>
 *   InitProducerId Response (Version: 2 and 3) => throttle_time_ms error_code producer_id producer_epoch
 * </pre>
 *
 * KIP-360 changed the REQUEST alone, so the two versions share this frame; what differs is the meaning of the
 * answer to a request that carried a producer id of its own.
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0 to v4)"
 */
final class InitProducerIdResponseV2 extends InitProducerIdResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
