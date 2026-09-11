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
 * Fetch response object (key 1), version 4
 *
 * <pre>
 *   FetchResponse (Version: 4) => ThrottleTimeMs [TopicName [Partition ErrorCode HighwaterMarkOffset
 *                                                            LastStableOffset [AbortedTransactions]
 *                                                            RecordSetSize RecordSet]]
 * </pre>
 *
 * Version 4 (Kafka 0.11.0, KIP-98) is the first version that is answered with the log as it lies - record batches
 * of the message format v2, with the headers, the producer ids and the transaction flags - and the first one whose
 * partition entries carry the `LastStableOffset` and the nullable `AbortedTransactions` array. The
 * `LogStartOffset` that version 5 inserted between the two does not exist here, so this class only lowers the
 * version constant that {@see FetchResponse::getScheme()} and {@see FetchResponse::topicClass()} follow.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v8)"
 */
final class FetchResponseV4 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
