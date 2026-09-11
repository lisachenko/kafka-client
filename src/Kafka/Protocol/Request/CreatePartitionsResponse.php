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
 * CreatePartitions response object, version 1 (key 37, Kafka 1.0)
 *
 * <pre>
 *   CreatePartitions Response (Version: 0 and 1) => throttle_time_ms [topic_errors]
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
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `CREATE_PARTITIONS_RESPONSE_V1 =
 * CREATE_PARTITIONS_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see CreatePartitionsResponseV0} is the same frame with the version field of Kafka 1.0.
 *
 * **Kafka 2.5 added the version 2** (KIP-482), the same fields in the flexible encoding: every string and array of
 * the frame is compact, the header carries a tag buffer and every structure ends in one. Not a field changed.
 *
 * @see docs/protocol/2.8.md, section "CreatePartitions API (key 37, v0 to v2)"
 */
class CreatePartitionsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 2;

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
