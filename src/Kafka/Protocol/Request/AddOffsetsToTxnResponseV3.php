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
 * AddOffsetsToTxn answer of version 3, the frame of version 4 with a lower version field
 *
 * "Version 4 adds support for new error code TRANSACTION_ABORTABLE (KIP-890)" (`AddOffsetsToTxnResponse.json` @
 * 3.8.1): the throttle time and the one error code of the whole request, in the same 16 bytes. A 3.9.2 node never
 * writes the 120 here - this request adds a partition to the transaction instead of writing into one, so no
 * verification happens, and the two versions answer the same codes.
 *
 * @see docs/protocol/3.9.md, section "AddOffsetsToTxn API (key 25, v0 to v4)"
 */
final class AddOffsetsToTxnResponseV3 extends AddOffsetsToTxnResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
