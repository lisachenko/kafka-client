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
 * ControlledShutdown request of version 2 (Kafka 2.2, KIP-380), the body of version 3 before KIP-482
 *
 * <pre>
 *   ControlledShutdown Request (Version: 2) => broker_id broker_epoch
 * </pre>
 *
 * KIP-380 gave the version 2 the `broker_epoch` and KIP-482 made the version 3 of Kafka 2.4 the flexible one; the
 * two int fields are the same, so only the encoding of the header and the trailing tagged-field section differ.
 *
 * @see docs/protocol/2.8.md, section "ControlledShutdown API (key 7, v0 to v3)"
 */
final class ControlledShutdownRequestV2 extends ControlledShutdownRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
