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
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV0;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV4;

/**
 * Fetch response object (key 1), version 5
 *
 * <pre>
 *   FetchResponse (Version: 5) => ThrottleTimeMs [TopicName [Partition ErrorCode HighwaterMarkOffset
 *                                                            LastStableOffset LogStartOffset
 *                                                            [AbortedTransactions] RecordSetSize RecordSet]]
 *     ThrottleTimeMs      => int32
 *     TopicName           => string
 *     Partition           => int32
 *     ErrorCode           => int16
 *     HighwaterMarkOffset => int64
 *     LastStableOffset    => int64
 *     LogStartOffset      => int64
 *     AbortedTransactions => nullable [ProducerId int64 FirstOffset int64]
 *     RecordSetSize       => int32
 * </pre>
 *
 * Version 1 of the API added `ThrottleTimeMs` **before** the topics array - the opposite end of the response from
 * where the Produce API put its `ThrottleTime` (`FETCH_RESPONSE_V1` in `Protocol.java` @ 0.11.0.3,
 * `FetchResponse.readFrom` in `kafka/api/FetchResponse.scala`, which reads the field only when the request version
 * is greater than 0). It is the number of milliseconds the broker delayed the request because the client exceeded
 * its fetch quota; a broker without quotas - the default - always answers 0.
 *
 * The versions 2 and 3 did not change the frame at all (`FETCH_RESPONSE_V3` is `FETCH_RESPONSE_V2` is
 * `FETCH_RESPONSE_V1`): version 2 only tells the broker that the client understands message format v1, so the
 * message sets of the answer are no longer converted down to format v0, and version 3 only added the request-level
 * `MaxBytes`, see {@see FetchRequest}.
 *
 * **Version 4 (Kafka 0.11.0, KIP-98)** added `LastStableOffset` and the nullable `AbortedTransactions` array to
 * every partition entry and answers with the log as it lies, i.e. with record batches of the message format v2;
 * **version 5** (KIP-107) added `LogStartOffset` between the two, see
 * {@see \Protocol\Kafka\Protocol\Data\FetchResponsePartition}.
 *
 * What the answer of every version has to match is the *version of the request it belongs to*, which is why
 * {@see FetchResponseV4}, {@see FetchResponseV3}, {@see FetchResponseV2}, {@see FetchResponseV1} and
 * {@see FetchResponseV0} exist - the version constant selects both the fields of the answer and the class of a
 * partition entry.
 *
 * @see docs/protocol/0.11.0.md, section "Fetch API (key 1, v0 to v5)"
 */
class FetchResponse extends AbstractResponse
{
    /**
     * Version of the Fetch API that this class decodes the answer of
     */
    public const int VERSION = 5;

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
        $body['topics'] = ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class unpacks
     *
     * @return class-string<FetchResponseTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 5 => FetchResponseTopic::class,
            static::VERSION >= 4 => FetchResponseTopicV4::class,
            default              => FetchResponseTopicV0::class,
        };
    }
}
