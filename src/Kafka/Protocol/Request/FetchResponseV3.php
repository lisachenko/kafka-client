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
 * Fetch response object (key 1), version 3
 *
 * <pre>
 *   FetchResponse (Version: 3) => ThrottleTimeMs [TopicName [Partition ErrorCode HighwaterMarkOffset
 *                                                            MessageSetSize MessageSet]]
 * </pre>
 *
 * The frame of a version 3 answer is the frame of a version 1 and of a version 2 answer - `FETCH_RESPONSE_V3` is
 * `FETCH_RESPONSE_V2` is `FETCH_RESPONSE_V1` in `Protocol.java` @ 0.11.0.3 - so this class only lowers the version
 * constant to the version of the request it belongs to, which drops the `LastStableOffset`, the
 * `AbortedTransactions` and the `LogStartOffset` that the versions 4 and 5 added to every partition entry. A
 * version 3 answer of a log of the message format v2 carries the batches converted down to message format v1, one
 * message per record, see {@see FetchRequestV3}.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v10)"
 */
final class FetchResponseV3 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
