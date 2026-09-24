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
 * AddPartitionsToTxn answer of version 4 (Kafka 3.5, KIP-890), the batched answer before the code 120
 *
 * "Version 5 adds support for new error code TRANSACTION_ABORTABLE" (`AddPartitionsToTxnResponse.json` @ 3.8.1):
 * the top-level error code and the `results_by_transaction` array of the version 4 keep every byte they had. A
 * 3.9.2 node answers the **120** of a `verify_only` for a partition the transaction does not hold at **both**
 * versions - the code is the coordinator's, not the version's, and the version 4 predates it.
 *
 * @see docs/protocol/4.3.md, section "AddPartitionsToTxn API (key 24, v0 to v5)"
 */
final class AddPartitionsToTxnResponseV4 extends AddPartitionsToTxnResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
