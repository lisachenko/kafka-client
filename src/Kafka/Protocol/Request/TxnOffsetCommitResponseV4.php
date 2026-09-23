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
 * TxnOffsetCommit answer of version 4, the frame of version 5 with a lower version field
 *
 * "Version 5 is the same with version 3 (KIP-890)" (`TxnOffsetCommitResponse.json` @ 4.0.0): the throttle time and
 * one error code per partition. At the version 4 a commit whose `__consumer_offsets` partition the transaction does
 * not hold is answered the **120** `TransactionAbortable`; the version 5 enrols that partition instead.
 *
 * @see docs/protocol/4.3.md, section "TxnOffsetCommit API (key 28, v0 to v5)"
 */
final class TxnOffsetCommitResponseV4 extends TxnOffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
