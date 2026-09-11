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
 * The produce API, version 7
 *
 * The body of version 7 (Kafka 2.1, KIP-110) is the body of version 3, byte for byte, and it is the first
 * version that may carry a zstd-compressed record set. Version 8 (Kafka 2.4, KIP-467) sends the very same body
 * once more and states that the client understands the record errors of the answer, see {@see ProduceRequest}.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v9)"
 */
final class ProduceRequestV7 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
