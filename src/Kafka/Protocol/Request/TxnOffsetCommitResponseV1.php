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
 * TxnOffsetCommit response of version 1 (Kafka 2.0), the frame every version of this api has
 *
 * <pre>
 *   TxnOffsetCommit Response (Version: 1) => throttle_time_ms [topics]
 * </pre>
 *
 * `TXN_OFFSET_COMMIT_RESPONSE_V1 = TXN_OFFSET_COMMIT_RESPONSE_V0` in `Protocol.java` @ 2.0.1, and the version 2 of
 * Kafka 2.1 kept it as well: the `committed_leader_epoch` of KIP-320 is a field of the request alone. This class
 * only lowers the version constant, so that an answer of version 1 is read through the class of its own version.
 *
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0 to v2)"
 */
final class TxnOffsetCommitResponseV1 extends TxnOffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
