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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\FetchResponseTopic;

/**
 * Fetch response object (key 1), version 3
 *
 * <pre>
 *   FetchResponse (Version: 3) => ThrottleTimeMs [TopicName [Partition ErrorCode HighwaterMarkOffset
 *                                                            MessageSetSize MessageSet]]
 *     ThrottleTimeMs      => int32
 *     TopicName           => string
 *     Partition           => int32
 *     ErrorCode           => int16
 *     HighwaterMarkOffset => int64
 *     MessageSetSize      => int32
 * </pre>
 *
 * Version 1 of the API added `ThrottleTimeMs` **before** the topics array - the opposite end of the response from
 * where the Produce API put it (`FETCH_RESPONSE_V1` in `Protocol.java` @ 0.10.2.2, `FetchResponse.readFrom` in
 * `kafka/api/FetchResponse.scala`, which reads the field only when the request version is greater than 0). It is
 * the number of milliseconds the broker delayed the request because the client exceeded its fetch quota; a broker
 * without quotas - the default - always answers 0.
 *
 * The versions 2 and 3 did not change the frame at all (`FETCH_RESPONSE_V3` is `FETCH_RESPONSE_V2` is
 * `FETCH_RESPONSE_V1`): version 2 only tells the broker that the client understands message format v1, so the
 * message sets of the answer are no longer converted down to format v0, and version 3 only added the request-level
 * `MaxBytes`, see {@see FetchRequest}. What the answer of every version does have to match is the *version of the
 * request it belongs to*, which is why {@see FetchResponseV2}, {@see FetchResponseV1} and {@see FetchResponseV0}
 * exist - the last one is the only frame that really differs, it has no `ThrottleTimeMs` at all.
 *
 * The `LastStableOffset`, `LogStartOffset` and `AbortedTransactions` fields of the later protocol lines arrived with
 * Kafka 0.11.
 *
 * @see docs/protocol/0.11.0.md, section "Fetch API (key 1, v0 to v3)"
 */
class FetchResponse extends AbstractResponse
{
    /**
     * Version of the Fetch API that this class decodes the answer of
     */
    public const int VERSION = 3;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * Fetch result for each of the requested topics, indexed by the topic name
     *
     * @var array<string, FetchResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 1) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        $body['topics'] = ['topic' => FetchResponseTopic::class];

        return $header + $body;
    }
}
