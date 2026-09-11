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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The result of moving the replicas of one topic, i.e. one entry of the `topics` array
 *
 * <pre>
 *   AlterReplicaLogDirsResponseTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => AlterReplicaLogDirsResponsePartition
 * </pre>
 *
 * `ALTER_REPLICA_LOG_DIRS_RESPONSE_V0` in `AlterReplicaLogDirsResponse.schemaVersions()` @ 1.1.1. The answer is
 * grouped by topic, not by directory: the request says where each replica should go, and the answer only says what
 * became of each replica, so the destination is not repeated. Every replica of the request gets an entry, including
 * the ones that are not on this broker at all.
 *
 * @see docs/protocol/2.8.md, section "AlterReplicaLogDirs API (key 34, v0)"
 */
class AlterReplicaLogDirsResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic this entry belongs to
     */
    public string $topic;

    /**
     * Result of every requested replica of this topic, indexed by the partition id
     *
     * @var array<int, AlterReplicaLogDirsResponsePartition>
     */
    public array $partitions;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => AlterReplicaLogDirsResponsePartition::class],
        ];
    }
}
