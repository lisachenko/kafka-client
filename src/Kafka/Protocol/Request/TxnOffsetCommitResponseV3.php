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
 * TxnOffsetCommit answer of version 3, the frame of version 4 with a lower version field
 *
 * "Version 4 adds support for new error code TRANSACTION_ABORTABLE (KIP-890)" (`TxnOffsetCommitResponse.json` @
 * 3.8.1): the throttle time and the per-partition error codes, unchanged. The codes 22, 25 and 82 of the version
 * 3 stay the ones the group coordinator really answers here. What this version does **not** get is the
 * **120** `TransactionAbortable`: a commit whose `__consumer_offsets` partition is not part of the open
 * transaction is answered the **48** `InvalidTxnState` at this version and the 120 at the version 4, because
 * `AddPartitionsToTxnManager` @ 3.9.2 maps the code back "for backward compatibility with clients" whose
 * version does not promise to understand it.
 *
 * @see docs/protocol/4.3.md, section "TxnOffsetCommit API (key 28, v0 to v5)"
 */
final class TxnOffsetCommitResponseV3 extends TxnOffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
