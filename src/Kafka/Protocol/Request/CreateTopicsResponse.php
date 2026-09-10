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

/**
 * CreateTopics response object, version 2 (key 19)
 *
 * <pre>
 *   CreateTopics Response (Version: 2) => throttle_time_ms [topic_errors]
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
 * @see docs/protocol/1.1.md, section "CreateTopics API (key 19, v0, v1 and v2)"
 */
class CreateTopicsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

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
        return static::VERSION >= 1 ? CreateTopicsResponseTopic::class : CreateTopicsResponseTopicV0::class;
    }
}
