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
 * InitProducerId, version 1: the KIP-219 bump of Kafka 2.0 (ApiKey 22)
 *
 * <pre>
 *   InitProducerId Request (Version: 1) => transactional_id transaction_timeout_ms
 * </pre>
 *
 * The last version of this api in the plain encoding: version 2 (Kafka 2.4) is the first flexible one and is what
 * {@see InitProducerIdRequest} sends. Version 1 itself is the frame of version 0 with a higher number in the
 * header, which is the client's promise that it honours a `throttle_time_ms` itself.
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0 to v2)"
 */
final class InitProducerIdRequestV1 extends InitProducerIdRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
