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
 * Produce response object, version 4
 *
 * <pre>
 *   ProduceResponse (Version: 4) => [TopicName [Partition ErrorCode Offset LogAppendTime]] ThrottleTime
 * </pre>
 *
 * `PRODUCE_RESPONSE_V4` is `PRODUCE_RESPONSE_V3` is `PRODUCE_RESPONSE_V2` in
 * `ProduceResponse.schemaVersions()` @ 1.1.1: the answer of a version 4 request is the version 2 frame, without the
 * `LogStartOffset` that version 5 appended to every partition entry. What version 4 changes is not the frame but
 * the error codes the broker may put into it: a client that asked with version 4 or higher receives **56**
 * `KAFKA_STORAGE_ERROR` where a lower one receives 6 `NOT_LEADER_FOR_PARTITION`.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v8)"
 */
final class ProduceResponseV4 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
