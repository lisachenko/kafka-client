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
 * Fetch response object (key 1), version 7
 *
 * <pre>
 *   FetchResponse (Version: 7) => ThrottleTimeMs ErrorCode SessionId
 *                                 [TopicName [Partition ErrorCode HighwaterMarkOffset
 *                                             LastStableOffset LogStartOffset
 *                                             [AbortedTransactions] RecordSetSize RecordSet]]
 *     ThrottleTimeMs      => int32
 *     ErrorCode           => int16      -- since version 7
 *     SessionId           => int32      -- since version 7
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
 * where the Produce API put its `ThrottleTime` (`FETCH_RESPONSE_V1` in `FetchResponse.schemaVersions()` @ 1.1.1,
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
 * {@see \Protocol\Kafka\Protocol\Data\FetchResponsePartition}. **Version 6** (Kafka 1.0) is the version 5 frame
 * again and only states that the client understands the error code 56 ({@see FetchResponseV6}).
 *
 * **Version 7 (Kafka 1.1, KIP-227)** is the next one that changed the frame: it inserts a **top-level error code**
 * and the **session id** between the throttle time and the topics array, see {@see self::$errorCode} and
 * {@see self::$sessionId}. Every version below 7 leaves both at 0, which is exactly what a version 7 answer to a
 * session-less request reports as well.
 *
 * What the answer of every version has to match is the *version of the request it belongs to*, which is why
 * {@see FetchResponseV6}, {@see FetchResponseV5}, {@see FetchResponseV4}, {@see FetchResponseV3},
 * {@see FetchResponseV2}, {@see FetchResponseV1} and {@see FetchResponseV0} exist - the version constant selects
 * both the fields of the answer and the class of a partition entry.
 *
 * @see docs/protocol/1.1.md, sections "Fetch API (key 1, v0 to v7)" and "Fetch sessions (v7, KIP-227)"
 */
class FetchResponse extends AbstractResponse
{
    /**
     * Version of the Fetch API that this class decodes the answer of
     */
    public const int VERSION = 7;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * Error code of the fetch **session**, zero when the request had none or the session is intact.
     *
     * This is the only top-level error code the Fetch api has, and it is about the session alone: 70
     * `FetchSessionIdNotFoundException` for an incremental request whose session id the broker does not know, and
     * 71 `InvalidFetchSessionEpochException` for one whose epoch does not match the one the broker expects. Both
     * are retriable and both are answered with an **empty topics array**, so a client that receives one has to
     * start over with a full fetch, {@see FetchMetadata::nextCloseExisting()}.
     *
     * An answer below version 7 does not carry the field and leaves the 0, which is also what a version 7 answer
     * to a session-less request reports.
     *
     * @since Version 7 of protocol
     */
    public int $errorCode = 0;

    /**
     * Id of the fetch session this answer belongs to, 0 when there is none.
     *
     * A full fetch with the epoch 0 is answered with the id of the session the broker created for it; every
     * incremental fetch of that session is answered with the same id; a session-less request, a request that
     * closed its session, and every request whose session the broker could not create is answered with
     * {@see FetchMetadata::INVALID_SESSION_ID}, and so is a session error - `SessionErrorContext` @ 1.1.1 answers
     * the codes 70 and 71 with the session id 0, not with the id of the request.
     *
     * @since Version 7 of protocol
     */
    public int $sessionId = FetchMetadata::INVALID_SESSION_ID;

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
        if (static::VERSION >= 7) {
            $body['errorCode'] = BinarySchema::TYPE_INT16;
            $body['sessionId'] = BinarySchema::TYPE_INT32;
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
