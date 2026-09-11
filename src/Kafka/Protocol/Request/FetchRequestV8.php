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
 * Fetch API (key 1), version 8
 *
 * <pre>
 *   FetchRequest (Version: 8) => ReplicaId MaxWaitTime MinBytes MaxBytes IsolationLevel SessionId Epoch
 *                                [TopicName [Partition FetchOffset LogStartOffset MaxBytes]]
 *                                [TopicName [Partition]]
 * </pre>
 *
 * The body of version 8 (Kafka 2.0, KIP-219) is the body of version 7, byte for byte, and it is the last version
 * whose partition entry carries **no** `current_leader_epoch`: version 9 (Kafka 2.1, KIP-320) inserted that field
 * between the partition index and the fetch offset, see {@see FetchRequest}. A client that fetches with this
 * version is therefore never fenced on its metadata - it can read past a leader change without noticing it - which
 * is exactly what KIP-320 set out to fix.
 *
 * What version 8 does state is the throttling of KIP-219, see {@see FetchRequestV7} for what changed there.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v11)" and "The leader epoch (KIP-320)"
 */
final class FetchRequestV8 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
