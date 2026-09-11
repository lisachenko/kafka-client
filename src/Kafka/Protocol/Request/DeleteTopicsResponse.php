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
use Protocol\Kafka\Protocol\Data\DeleteTopicsResponseTopicV0;
use Protocol\Kafka\Protocol\Data\DeleteTopicsResponseTopicV5;

/**
 * DeleteTopics response object, version 3 (key 20)
 *
 * <pre>
 *   DeleteTopics Response (Version: 1 and 2) => throttle_time_ms [topic_error_codes]
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
 * **Kafka 2.0 added version 2** and changed nothing about the bytes: `DELETE_TOPICS_RESPONSE_V2 =
 * DELETE_TOPICS_RESPONSE_V1` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see DeleteTopicsResponseV1} is the same frame with the version field of Kafka 0.11.
 *
 *
 * **Kafka 2.1 added version 3**, whose frame is this one once more: what the version changes is the error CODE a
 * broker with `delete.topic.enable=false` writes into it - **73** `TOPIC_DELETION_DISABLED` for a version 3
 * client, the **42** `INVALID_REQUEST` of the lines below for every lower one
 * (`KafkaApis.handleDeleteTopicsRequest` @ 2.8.2). {@see DeleteTopicsResponseV2} is the same frame with the
 * version field of Kafka 2.0.
 *
 * **Kafka 2.4 added the version 4** (KIP-482): the same fields in the flexible encoding, with a tagged-field
 * section at the end of the body and of every topic result. {@see DeleteTopicsResponseV3} is the frame of Kafka 2.1.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v6)"
 */
class DeleteTopicsResponse extends AbstractResponse
{
    /**
     * Version of the DeleteTopics API that this class decodes the answer of
     */
    public const int VERSION = 6;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 4;

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
        $body['topics'] = ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the api that this class unpacks
     *
     * @return class-string<DeleteTopicsResponseTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 6 => DeleteTopicsResponseTopic::class,
            static::VERSION >= 5 => DeleteTopicsResponseTopicV5::class,
            default              => DeleteTopicsResponseTopicV0::class,
        };
    }
}
