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
use Protocol\Kafka\Protocol\Data\ProduceResponseTopic;

/**
 * Produce response object, version 1
 *
 * <pre>
 *   ProduceResponse (Version: 1) => [TopicName [Partition ErrorCode Offset]] ThrottleTime
 *     ThrottleTime => int32
 * </pre>
 *
 * Version 1 of the API added `ThrottleTime` **after** the topics array (`PRODUCE_RESPONSE_V1` in `Protocol.java`
 * @ 0.9.0.1, `ProducerResponse.writeTo` in `kafka/api/ProducerResponse.scala`): the number of milliseconds the
 * broker delayed this request because the client exceeded its produce quota. A broker without quotas - the default,
 * `quota.producer.default` is unlimited - always answers 0.
 *
 * The `LogAppendTime` of a partition entry arrived with version 2 (Kafka 0.10.0, message format v1) and does not
 * exist here. A request with `RequiredAcks = 0` is never answered at all, see
 * {@see ProduceRequest::expectsResponse()}.
 *
 * @see docs/protocol/0.10.2.md, section "Produce API (key 0, v0 and v1)"
 */
class ProduceResponse extends AbstractResponse
{
    /**
     * Version of the Produce API that this class decodes the answer of
     */
    public const int VERSION = 1;

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
            'topics' => ['topic' => ProduceResponseTopic::class],
        ];
        if (static::VERSION >= 1) {
            $body['throttleTime'] = BinarySchema::TYPE_INT32;
        }

        return $header + $body;
    }
}
