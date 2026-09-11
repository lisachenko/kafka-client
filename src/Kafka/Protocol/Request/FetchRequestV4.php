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
 * Fetch API (key 1), version 4
 *
 * <pre>
 *   FetchRequest (Version: 4) => ReplicaId MaxWaitTime MinBytes MaxBytes IsolationLevel
 *                                [TopicName [Partition FetchOffset MaxBytes]]
 *     IsolationLevel => int8
 * </pre>
 *
 * Version 4 (Kafka 0.11.0, KIP-98) is the first version that carries the `IsolationLevel` and the first one that
 * is answered with the log as it lies - a record batch of the message format v2. What it does not carry is the
 * `LogStartOffset` that version 5 added to a partition entry of the request and to every partition of the answer,
 * so this class only lowers the version constant that {@see FetchRequest::getScheme()} and
 * {@see FetchRequest::topicClass()} follow.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v7)"
 */
final class FetchRequestV4 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
