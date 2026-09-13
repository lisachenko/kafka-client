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
 * TxnOffsetCommit response of version 2 (Kafka 2.1), byte for byte the answer of version 3
 *
 * <pre>
 *   TxnOffsetCommit Response (Version: 0 to 2) => throttle_time_ms [topics]
 * </pre>
 *
 * KIP-447 changed the request alone, and the version 3 writes these very fields in the flexible encoding.
 *
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0 to v3)"
 */
final class TxnOffsetCommitResponseV2 extends TxnOffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
