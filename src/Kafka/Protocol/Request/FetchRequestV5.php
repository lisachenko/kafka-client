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
 * Fetch API (key 1), version 5
 *
 * <pre>
 *   FetchRequest (Version: 5) => ReplicaId MaxWaitTime MinBytes MaxBytes IsolationLevel
 *                                [TopicName [Partition FetchOffset LogStartOffset MaxBytes]]
 *     LogStartOffset => int64
 * </pre>
 *
 * Version 5 (Kafka 0.11.0, KIP-107) is the version that added the `LogStartOffset` of a partition entry - which
 * only a follower fills in - and the `LogStartOffset` of every partition of the answer. It is the highest version
 * of the 0.11 line and the last one before the two that Kafka 1.x added: {@see FetchRequestV6}, which is the same
 * frame with the error code 56, and {@see FetchRequest} (version 7), which carries the incremental fetch sessions
 * of KIP-227. This class only lowers the version constant that {@see FetchRequest::getScheme()} follows.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v11)"
 */
final class FetchRequestV5 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
