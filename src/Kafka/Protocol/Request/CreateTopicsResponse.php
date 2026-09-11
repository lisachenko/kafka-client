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
use Protocol\Kafka\Protocol\Data\CreateTopicsResponseTopic;
use Protocol\Kafka\Protocol\Data\CreateTopicsResponseTopicV0;
use Protocol\Kafka\Protocol\Data\CreateTopicsResponseTopicV1;

/**
 * CreateTopics response object, version 4 (key 19)
 *
 * <pre>
 *   CreateTopics Response (Version: 2, 3 and 4) => throttle_time_ms [topic_errors]
 *     throttle_time_ms => INT32     -- since version 2
 *     topic_errors => topic error_code error_message
 *       topic         => STRING
 *       error_code    => INT16
 *       error_message => NULLABLE_STRING
 * </pre>
 *
 * Version 2 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the array and left the entries alone;
 * {@see CreateTopicsResponseV1} is the same array without it, and {@see CreateTopicsResponseV0} the bare
 * `topic error_code` entries of version 0. There is no top-level error code in any version: the answer carries one
 * entry per topic of the request. The error codes a 0.11.0.3 controller reports here are:
 *
 * | Code | Name                       | Meaning                                                                   |
 * |------|----------------------------|---------------------------------------------------------------------------|
 * | 0    | None                       | The topic exists now, with every partition created                        |
 * | 7    | RequestTimedOut            | Accepted, but not finished within `timeout` - a timeout of 0 always gets it|
 * | 36   | TopicAlreadyExists         | A topic of that name already exists                                       |
 * | 37   | InvalidPartitions          | `num_partitions` is 0 or below (and not -1)                               |
 * | 38   | InvalidReplicationFactor   | `replication_factor` is 0, below -1, or larger than the number of brokers |
 * | 39   | InvalidReplicaAssignment   | The explicit assignment is inconsistent or incomplete                     |
 * | 40   | InvalidConfig              | A topic-level option is unknown or its value can not be parsed            |
 * | 41   | NotController              | The broker that was asked is not the active controller                    |
 * | 42   | InvalidRequest             | The topic appears twice in one request, or partitions/factor were combined
 *                                     with an explicit assignment                                               |
 * | 44   | PolicyViolation            | A `create.topic.policy.class.name` on the broker refused the topic        |
 *
 * **Kafka 2.0 added version 3** and changed nothing about the bytes: `CREATE_TOPICS_RESPONSE_V3 =
 * CREATE_TOPICS_RESPONSE_V2` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see CreateTopicsResponseV2} is the same frame with the version field of Kafka 0.11.
 *
 * **Kafka 2.4 added version 4** (KIP-464) and gave the ANSWER nothing: the note of `CreateTopicsResponse.json`
 * @ 2.8.2 is about the request, and the next field of the answer - the `topic_configs` of KIP-525 - arrives with
 * the flexible version 5. {@see CreateTopicsResponseV3} is the same frame with the version field of Kafka 2.0.
 *
 * **Kafka 2.4 added the version 5** (KIP-482 and KIP-525): it is the first **flexible** version of the api, and
 * every topic result of it also carries the partition count, the replication factor and the whole configuration
 * the new topic ended up with, plus the tagged field 0 `topic_config_error_code` for the case in which the broker
 * could not read that configuration back. {@see CreateTopicsResponseV4} is the frame of Kafka 2.4 without any of it.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v5)"
 */
class CreateTopicsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 5;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 2 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every topic of the request, indexed by the topic name
     *
     * @var array<string, CreateTopicsResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 2) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        $body['topics'] = ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class unpacks
     *
     * @return class-string<CreateTopicsResponseTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 5 => CreateTopicsResponseTopic::class,
            static::VERSION >= 1 => CreateTopicsResponseTopicV1::class,
            default              => CreateTopicsResponseTopicV0::class,
        };
    }
}
