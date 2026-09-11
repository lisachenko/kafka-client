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
 * Fetch response object (key 1), version 7
 *
 * <pre>
 *   FetchResponse (Version: 7) => ThrottleTimeMs ErrorCode SessionId
 *                                 [TopicName [Partition ErrorCode HighwaterMarkOffset
 *                                             LastStableOffset LogStartOffset
 *                                             [AbortedTransactions] RecordSetSize RecordSet]]
 * </pre>
 *
 * The frame of version 7 (Kafka 1.1, KIP-227) is the frame of version 8, byte for byte - the top-level error code
 * and the session id of the fetch session, then the topics. The two versions differ only in what the client
 * promises about the throttle time of KIP-219, see {@see FetchResponse}.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v11)" and "Fetch sessions (v7, KIP-227)"
 */
final class FetchResponseV7 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
