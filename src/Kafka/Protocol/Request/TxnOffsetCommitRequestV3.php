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
 * TxnOffsetCommit request of version 3, the frame of version 4 with a lower version field
 *
 * "Version 4 adds support for new error code TRANSACTION_ABORTABLE (KIP-890)" (`TxnOffsetCommitRequest.json` @
 * 3.8.1): the version 3 of KIP-447 - the generation, the member id and the group instance id in front of the
 * topics, in the flexible encoding - is written byte for byte the same way, and the version alone says that the
 * sender understands the code 120. The version 3 is what a client sends to a broker below Kafka 3.8.
 *
 * @see docs/protocol/3.9.md, section "TxnOffsetCommit API (key 28, v0 to v4)"
 */
final class TxnOffsetCommitRequestV3 extends TxnOffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
