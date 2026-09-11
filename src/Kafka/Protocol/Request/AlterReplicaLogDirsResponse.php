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
use Protocol\Kafka\Protocol\Data\AlterReplicaLogDirsResponseTopic;

/**
 * AlterReplicaLogDirs response object, version 1 (key 34)
 *
 * <pre>
 *   AlterReplicaLogDirs Response (Version: 0 and 1) => throttle_time_ms [topics]
 *     throttle_time_ms => INT32
 *     topics           => topic [partitions]
 *       topic      => STRING
 *       partitions => partition error_code
 *         partition  => INT32
 *         error_code => INT16
 * </pre>
 *
 * The api was born in Kafka 1.0, after KIP-124 made `throttle_time_ms` the first field of every new answer, and it
 * has **no top-level error code**: every failure belongs to a single replica.
 *
 * Every replica of the request gets an entry, whether or not this broker has it, and the error code 0 only means
 * that the move was **accepted** - the copy runs in the background and is watched with DescribeLogDirs. The codes a
 * 1.1.1 broker reports here are listed on
 * {@see \Protocol\Kafka\Protocol\Data\AlterReplicaLogDirsResponsePartition}.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `ALTER_REPLICA_LOG_DIRS_RESPONSE_V1 =
 * ALTER_REPLICA_LOG_DIRS_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see AlterReplicaLogDirsResponseV0} is the same frame with the version field of Kafka 1.0.
 *
 * @see docs/protocol/2.8.md, section "AlterReplicaLogDirs API (key 34, v0 and v1)"
 */
class AlterReplicaLogDirsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every requested topic, indexed by the topic name
     *
     * @var array<string, AlterReplicaLogDirsResponseTopic>
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
            'topics'         => ['topic' => AlterReplicaLogDirsResponseTopic::class],
        ];
    }
}
