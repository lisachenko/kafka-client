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
 * One topic of a DescribeLogDirs request, i.e. one entry of the nullable `topics` array
 *
 * <pre>
 *   DescribeLogDirsRequestTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => INT32
 * </pre>
 *
 * `DESCRIBE_LOG_DIRS_REQUEST_V0` in `DescribeLogDirsRequest.schemaVersions()` @ 1.1.1, whose `partitions` is a bare
 * `ArrayOf(INT32)` - the api asks about replicas, not about offsets, so a partition needs nothing but its id. The
 * Java client keeps the request as a flat `Set<TopicPartition>` and groups it by topic only while it writes the
 * frame (`DescribeLogDirsRequest.toStruct`), which is exactly the shape of this DTO.
 *
 * A partition that the broker does not host is simply absent from the answer: the broker intersects the requested
 * set with the logs it has (`ReplicaManager.describeLogDirs` filters `logsByDir` by `partitions.contains`), so an
 * unknown topic or partition is neither an error nor an entry.
 *
 * @see docs/protocol/1.1.md, section "DescribeLogDirs API (key 35, v0)"
 */
class DescribeLogDirsRequestTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic whose replicas should be described
     */
    public string $topic;

    /**
     * Ids of the partitions of this topic that should be described
     *
     * @var list<int>
     */
    public array $partitions;

    /**
     * @param string    $topic      Name of the topic
     * @param list<int> $partitions Ids of the partitions to describe
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
