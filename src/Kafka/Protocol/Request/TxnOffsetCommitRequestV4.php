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
 * TxnOffsetCommit request of version 4, the frame of version 5 with a lower version field
 *
 * "Version 5 is the same as version 4 (KIP-890)" (`TxnOffsetCommitRequest.json` @ 4.0.0): the transactional id,
 * the group, the producer id and epoch, the membership of KIP-447 and the offsets. The version 4 of Kafka 3.8 is the
 * commit of a transaction of the protocol **v1**: the group coordinator only *verifies* that the
 * `__consumer_offsets` partition of the group is part of the transaction - an {@see AddOffsetsToTxnRequest} has to
 * have put it there - and answers the **120** `TransactionAbortable` when it is not. It is the version the Java
 * `TxnOffsetCommitRequest.Builder` @ 4.0.0 caps at (`LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`) on a cluster that
 * does not finalize `transaction.version` 2, and the highest one a 3.x node serves.
 *
 * @see docs/protocol/4.3.md, section "TxnOffsetCommit API (key 28, v0 to v5)"
 */
final class TxnOffsetCommitRequestV4 extends TxnOffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
