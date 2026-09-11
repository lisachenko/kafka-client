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
 * Fetch response object (key 1), version 5
 *
 * <pre>
 *   FetchResponse (Version: 5) => ThrottleTimeMs [TopicName [Partition ErrorCode HighwaterMarkOffset
 *                                                            LastStableOffset LogStartOffset
 *                                                            [AbortedTransactions] RecordSetSize RecordSet]]
 *     LogStartOffset => int64
 * </pre>
 *
 * Version 5 (KIP-107, Kafka 0.11.0) is the version that inserted `LogStartOffset` between the `LastStableOffset`
 * and the `AbortedTransactions` of every partition entry, and it is the highest version of the 0.11 line. The two
 * fields that version 7 (KIP-227) added behind the throttle time - the top-level error code and the session id -
 * are not on the wire here, so this class only lowers the version constant that {@see FetchResponse::getScheme()}
 * and {@see FetchResponse::topicClass()} follow.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v10)"
 */
final class FetchResponseV5 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
