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
 * TxnOffsetCommit response of version 0 (Kafka 0.11), the frame of version 1 with a lower version field
 *
 * <pre>
 *   TxnOffsetCommit Response (Version: 0) => throttle_time_ms [topics]
 * </pre>
 *
 * `TXN_OFFSET_COMMIT_RESPONSE_V1 = TXN_OFFSET_COMMIT_RESPONSE_V0` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see TxnOffsetCommitResponse::getScheme()} follows.
 *
 * The answer of version 1 is this one, sent for a request that carried the higher version field; what
 * KIP-219 changed is the MOMENT it arrives - a throttled client of version 1 is answered first and muted
 * afterwards, and waits `throttle_time_ms` out itself.
 *
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0 to v2)"
 */
final class TxnOffsetCommitResponseV0 extends TxnOffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
