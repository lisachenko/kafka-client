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
 * InitProducerId request of version 4, the frame of version 5 with a lower version field
 *
 * "Verison 5 adds support for new error code TRANSACTION_ABORTABLE (KIP-890)" - the typo is the comment of
 * `InitProducerIdRequest.json` @ 3.8.1 - and that is the whole of the version: the same four fields in the same
 * flexible encoding, and a promise that the sender understands the error code **120**. The version 4 of KIP-588
 * is what a client sends to a broker below Kafka 3.8.
 *
 * @see docs/protocol/4.3.md, section "InitProducerId API (key 22, v0 to v5)"
 */
final class InitProducerIdRequestV4 extends InitProducerIdRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
