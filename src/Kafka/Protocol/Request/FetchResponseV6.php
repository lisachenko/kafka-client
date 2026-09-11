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
 * Fetch response object (key 1), version 6
 *
 * <pre>
 *   FetchResponse (Version: 6) => ThrottleTimeMs [TopicName [Partition ErrorCode HighwaterMarkOffset
 *                                                            LastStableOffset LogStartOffset
 *                                                            [AbortedTransactions] RecordSetSize RecordSet]]
 * </pre>
 *
 * `FETCH_RESPONSE_V6` is `FETCH_RESPONSE_V5` in `FetchResponse.schemaVersions()` @ 1.1.1: the answer of a version 6
 * request is the version 5 frame, without the top-level error code and the session id that version 7 (KIP-227)
 * inserted behind the throttle time. What version 6 changes is not the frame but the error codes a partition entry
 * may carry: a client that asked with version 6 or higher receives **56** `KAFKA_STORAGE_ERROR` where a lower one
 * receives 6 `NOT_LEADER_FOR_PARTITION`.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v8)"
 */
final class FetchResponseV6 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
