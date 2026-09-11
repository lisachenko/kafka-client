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
 * Fetch API (key 1), version 3
 *
 * <pre>
 *   FetchRequest (Version: 3) => ReplicaId MaxWaitTime MinBytes MaxBytes
 *                                [TopicName [Partition FetchOffset MaxBytes]]
 * </pre>
 *
 * Version 3 (Kafka 0.10.1, KIP-74) carries the request-level `MaxBytes` that bounds the whole answer, but neither
 * the `IsolationLevel` of version 4 nor the `LogStartOffset` of a version 5 partition entry, so this class only
 * lowers the version constant that {@see FetchRequest::getScheme()} and {@see FetchRequest::topicClass()} follow.
 *
 * A 0.11 broker answers it by converting the log **down to message format v1**: the batches of a record batch v2
 * log are unwrapped into one message per record, which loses the headers and the producer state of every one of
 * them, and a `LogAppendTime` batch is rebuilt with that append time stamped on every single message.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v10)" and "RecordBatch (message format v2)"
 */
final class FetchRequestV3 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
