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
 * One topic of a log directory of an AlterReplicaLogDirs request, i.e. one entry of its `topics` array
 *
 * <pre>
 *   AlterReplicaLogDirsRequestTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => INT32
 * </pre>
 *
 * `ALTER_REPLICA_LOG_DIRS_REQUEST_V0` in `AlterReplicaLogDirsRequest.schemaVersions()` @ 1.1.1. The Java client
 * keeps the request as a flat `Map<TopicPartition, String>` and turns it inside out while it writes the frame
 * (`AlterReplicaLogDirsRequest.toStruct` groups by directory first and by topic second), which is the shape of
 * this DTO.
 *
 * @see docs/protocol/2.8.md, section "AlterReplicaLogDirs API (key 34, v0)"
 */
class AlterReplicaLogDirsRequestTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic whose replicas should be moved
     */
    public string $topic;

    /**
     * Ids of the partitions of this topic whose replica should move into the directory of this entry
     *
     * @var list<int>
     */
    public array $partitions;

    /**
     * @param string    $topic      Name of the topic
     * @param list<int> $partitions Ids of the partitions to move
     */
    public function __construct(string $topic, array $partitions)
    {
        $this->topic      = $topic;
        $this->partitions = array_values(array_map(intval(...), $partitions));
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => [BinarySchema::TYPE_INT32],
        ];
    }
}
