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
 * EndTxn request of version 3, the frame of version 4 with a lower version field
 *
 * "Version 4 adds support for new error code TRANSACTION_ABORTABLE (KIP-890)" (`EndTxnRequest.json` @ 3.8.1):
 * the transactional id, the producer id, the epoch and the one boolean of the result, unchanged since Kafka 0.11
 * and written in the flexible encoding since the version 3. The version 3 is what a client sends to a broker
 * below Kafka 3.8.
 *
 * @see docs/protocol/3.9.md, section "EndTxn API (key 26, v0 to v4)"
 */
final class EndTxnRequestV3 extends EndTxnRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
