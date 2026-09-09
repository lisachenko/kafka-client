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

/**
 * Produce response object, version 2
 *
 * <pre>
 *   ProduceResponse (Version: 2) => [TopicName [Partition ErrorCode Offset LogAppendTime]] ThrottleTime
 *     LogAppendTime => int64
 *     ThrottleTime  => int32
 * </pre>
 *
 * Version 1 of the API added `ThrottleTime` **after** the topics array (`PRODUCE_RESPONSE_V1` in `Protocol.java`
 * @ 0.10.2.2): the number of milliseconds the broker delayed this request because the client exceeded its produce
 * quota. A broker without quotas - the default, `quota.producer.default` is unlimited - always answers 0.
 *
 * Version 2 (Kafka 0.10.0, message format v1) added `LogAppendTime` to every partition entry, in front of that
 * throttle time, see {@see ProduceResponsePartition::$logAppendTime}. Because the three versions have different
 * frames, each of them is a class of its own - {@see ProduceResponseV1} and {@see ProduceResponseV0} - and the
 * version constant of this class selects both the fields of the answer and the class of a partition entry.
 *
 * A request with `RequiredAcks = 0` is never answered at all, see {@see ProduceRequest::expectsResponse()}.
 *
 * @see docs/protocol/0.11.0.md, section "Produce API (key 0, v0, v1 and v2)"
 */
class ProduceResponse extends AbstractResponse
{
    /**
     * Version of the Produce API that this class decodes the answer of
     */
    public const int VERSION = 2;

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
        return static::VERSION >= 2 ? ProduceResponseTopic::class : ProduceResponseTopicV0::class;
    }
}
