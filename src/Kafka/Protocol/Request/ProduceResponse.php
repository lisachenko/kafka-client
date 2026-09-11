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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopic;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopicV0;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopicV2;

/**
 * Produce response object, version 6
 *
 * <pre>
 *   ProduceResponse (Version: 6) => [TopicName [Partition ErrorCode Offset LogAppendTime LogStartOffset]]
 *                                   ThrottleTime
 *     LogAppendTime  => int64
 *     LogStartOffset => int64
 *     ThrottleTime   => int32
 * </pre>
 *
 * Version 1 of the API added `ThrottleTime` **after** the topics array (`PRODUCE_RESPONSE_V1` in
 * `ProduceResponse.schemaVersions()` @ 1.1.1): the number of milliseconds the broker delayed this request because
 * the client exceeded its produce quota. A broker without quotas - the default, `quota.producer.default` is
 * unlimited - always answers 0.
 *
 * Version 2 (Kafka 0.10.0, message format v1) added `LogAppendTime` to every partition entry, in front of that
 * throttle time, see {@see ProduceResponsePartition::$logAppendTime}. **The versions 3 and 4 changed nothing at
 * all**: `PRODUCE_RESPONSE_V4` is `PRODUCE_RESPONSE_V3` is `PRODUCE_RESPONSE_V2`, so a broker really answers a
 * version 3 or a version 4 request with the version 2 frame, and {@see ProduceResponseV4},
 * {@see ProduceResponseV3} and {@see ProduceResponseV2} decode the very same bytes - they only differ in the
 * version of the request they belong to.
 *
 * **Version 5 (Kafka 1.0) is the next one that really changed the answer**: every partition entry gains
 * `LogStartOffset` behind its `LogAppendTime`, the earliest offset the log of that partition still holds
 * ({@see ProduceResponsePartition::$logStartOffset}). An idempotent producer needs it to tell a *spurious*
 * `OutOfOrderSequence` - its records fell below the log start offset and the broker forgot its producer state,
 * which arrives as the error code 59 `UNKNOWN_PRODUCER_ID` - from a real one. {@see ProduceResponseV1} and
 * {@see ProduceResponseV0} carry the two lower frames that really differ.
 *
 * **Version 6 (Kafka 2.0, KIP-219) changed the answer no more than 3 and 4 did**: `ProduceResponse.json` @ 2.8.2
 * carries no field of it, and {@see ProduceResponseV5} decodes the very same bytes. What version 6 states is that
 * the client understands **when** a throttled answer arrives: the broker sends it first, with the delay it is
 * about to impose in `ThrottleTime`, and mutes the channel for that long afterwards, so the client has to wait the
 * value out itself, see {@see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT}. A 2.8.2 broker answers every
 * version that way, the promise of the version notwithstanding.
 *
 * A request with `RequiredAcks = 0` is never answered at all, see {@see ProduceRequest::expectsResponse()}.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v6)"
 */
class ProduceResponse extends AbstractResponse
{
    /**
     * Version of the Produce API that this class decodes the answer of
     */
    public const int VERSION = 6;

    /**
     * Result for each topic of the request, indexed by the topic name
     *
     * @var array<string, ProduceResponseTopic>
     */
    public array $topics = [];

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTime = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'topics' => ['topic' => static::topicClass()],
        ];
        if (static::VERSION >= 1) {
            $body['throttleTime'] = BinarySchema::TYPE_INT32;
        }

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class unpacks
     *
     * @return class-string<ProduceResponseTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 5 => ProduceResponseTopic::class,
            static::VERSION >= 2 => ProduceResponseTopicV2::class,
            default              => ProduceResponseTopicV0::class,
        };
    }
}
