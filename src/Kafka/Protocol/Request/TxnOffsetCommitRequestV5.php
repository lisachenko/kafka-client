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
 * TxnOffsetCommit request of version 5 (Kafka 4.0, KIP-890 part 2): the last version that names its topics
 *
 * "Version 5 is the same as version 4 (KIP-890)" (`TxnOffsetCommitRequest.json` @ 4.0.0), and the version of the
 * transaction protocol **v2**: the group coordinator enrols the `__consumer_offsets` partition of the group into the
 * open transaction itself. Version 6 (Kafka 4.4, KIP-1319) replaced the name of every topic entry with its id
 * ({@see TxnOffsetCommitRequest}); this version is what a client of the protocol v2 sends for a topic whose id it
 * does not know (the Java `TxnOffsetCommitRequest.Builder.forTopicNames()` @ 4.4.0), and what a node below Kafka 4.4
 * serves. A group the coordinator does not know and a stale member epoch of KIP-848 are answered the **22**
 * `IllegalGeneration` here, where the version 6 answers the 69 and the 113.
 *
 * @see docs/protocol/4.4.md, section "TxnOffsetCommit API (key 28, v0 to v6)"
 * @see docs/protocol/4.4.md, section "What Kafka 4.4 changed here (KIP-1319): the version 6"
 */
final class TxnOffsetCommitRequestV5 extends TxnOffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
