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
use Protocol\Kafka\Protocol\Data\CreatePartitionsResponseTopic;

/**
 * CreatePartitions response object, version 0 (key 37, Kafka 1.0)
 *
 * <pre>
 *   CreatePartitions Response (Version: 0) => throttle_time_ms [topic_errors]
 *     throttle_time_ms => INT32
 *     topic_errors => topic error_code error_message
 *       topic         => STRING
 *       error_code    => INT16
 *       error_message => NULLABLE_STRING
 * </pre>
 *
 * There is no top-level error code: the answer carries one entry per topic of the request, in the order the
 * controller walked its map. The error codes a 1.1.1 controller reports here are:
 *
 * | Code | Name                     | Meaning                                                                     |
 * |------|--------------------------|-----------------------------------------------------------------------------|
 * | 0    | None                     | The topic has the requested number of partitions now (or would have)        |
 * | 3    | UnknownTopicOrPartition  | The cluster does not have that topic                                        |
 * | 7    | RequestTimedOut          | Accepted, but not finished within `timeout` - a timeout of 0 always gets it |
 * | 37   | InvalidPartitions        | The count is not above the current partition count of the topic            |
 * | 39   | InvalidReplicaAssignment | The assignment does not match the added partitions or the replication factor|
 * | 41   | NotController            | The broker that was asked is not the active controller                      |
 * | 42   | InvalidRequest           | The topic appears twice in the request, or a partition reassignment is running |
 * | 44   | PolicyViolation          | A `create.topic.policy.class.name` on the broker refused the new count       |
 *
 * @see docs/protocol/2.8.md, section "CreatePartitions API (key 37, v0)"
 */
class CreatePartitionsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every topic of the request, indexed by the topic name
     *
     * @var array<string, CreatePartitionsResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'topics'         => ['topic' => CreatePartitionsResponseTopic::class],
        ];
    }
}
