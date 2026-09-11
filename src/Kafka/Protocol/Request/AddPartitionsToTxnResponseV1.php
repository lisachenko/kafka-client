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
 * AddPartitionsToTxn answer of version 1, the frame of version 2 with a lower version field
 *
 * The version 2 of KIP-588 adds no field: what it changes is the error code a fenced producer is answered with -
 * the **90** `ProducerFenced` instead of the 47 `InvalidProducerEpoch`.
 *
 * @see docs/protocol/2.8.md, section "AddPartitionsToTxn API (key 24, v0 to v2)"
 */
final class AddPartitionsToTxnResponseV1 extends AddPartitionsToTxnResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
