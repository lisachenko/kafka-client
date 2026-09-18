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
 * AddOffsetsToTxn request of version 3, the frame of version 4 with a lower version field
 *
 * "Version 4 adds support for new error code TRANSACTION_ABORTABLE (KIP-890)" (`AddOffsetsToTxnRequest.json` @
 * 3.8.1) and not a field moves: the four values of Kafka 0.11 in the flexible encoding the version 3 brought.
 * The version 3 is what a client sends to a broker below Kafka 3.8.
 *
 * @see docs/protocol/3.9.md, section "AddOffsetsToTxn API (key 25, v0 to v4)"
 */
final class AddOffsetsToTxnRequestV3 extends AddOffsetsToTxnRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
