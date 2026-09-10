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
use Protocol\Kafka\Protocol\Data\DeleteTopicsResponseTopic;

/**
 * DeleteTopics response object, version 1 (key 20)
 *
 * <pre>
 *   DeleteTopics Response (Version: 1) => throttle_time_ms [topic_error_codes]
 *     throttle_time_ms => INT32     -- since version 1
 *     topic_error_codes => topic error_code
 *       topic      => STRING
 *       error_code => INT16
 * </pre>
 *
 * One entry per topic of the request, behind the `throttle_time_ms` that version 1 (KIP-124, Kafka 0.11) added;
 * {@see DeleteTopicsResponseV0} is the same array without it. The error codes a 0.11.0.3 controller reports here
 * are:
 *
 * | Code | Name                     | Meaning                                                                    |
 * |------|--------------------------|----------------------------------------------------------------------------|
 * | 0    | None                     | The topic was deleted within the `timeout` of the request                  |
 * | 3    | UnknownTopicOrPartition  | No topic of that name is in the metadata cache of the broker               |
 * | 7    | RequestTimedOut          | Marked for deletion, but not finished within `timeout` - a timeout of 0     |
 *                                    always gets it                                                             |
 * | 29   | TopicAuthorizationFailed | The client may describe the topic but not delete it                        |
 * | 41   | NotController            | The broker that was asked is not the active controller                     |
 *
 * @see docs/protocol/1.1.md, section "DeleteTopics API (key 20, v0 and v1)"
 */
class DeleteTopicsResponse extends AbstractResponse
{
    /**
     * Version of the DeleteTopics API that this class decodes the answer of
     */
    public const int VERSION = 1;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every topic of the request, indexed by the topic name
     *
     * @var array<string, DeleteTopicsResponseTopic>
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
        $body['topics'] = ['topic' => DeleteTopicsResponseTopic::class];

        return $header + $body;
    }
}
