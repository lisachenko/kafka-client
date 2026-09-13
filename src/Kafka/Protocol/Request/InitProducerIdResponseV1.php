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
 * The producer id of a KIP-219 request, version 1 (ApiKey 22)
 *
 * <pre>
 *   InitProducerId Response (Version: 1) => throttle_time_ms error_code producer_id producer_epoch
 * </pre>
 *
 * The same four fields as version 0 and version 2; version 1 only says that the broker may answer before it
 * throttles, and version 2 writes the very same fields in the flexible encoding
 * ({@see InitProducerIdResponse}).
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0 to v4)"
 */
final class InitProducerIdResponseV1 extends InitProducerIdResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
