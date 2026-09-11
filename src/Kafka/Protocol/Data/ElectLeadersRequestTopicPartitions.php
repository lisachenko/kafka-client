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
 * The partitions of one topic whose leader should be elected, i.e. one entry of `topic_partitions`
 *
 * <pre>
 *   ElectLeadersRequestTopicPartitions => topic [partition_id]
 *     topic        => STRING
 *     partition_id => INT32
 * </pre>
 *
 * `TopicPartitions` of `ElectLeadersRequest.json` @ 2.8.2. The array of partition ids is a plain int32 array and
 * not a structure, exactly like the partitions of an {@see AlterReplicaLogDirsRequestTopic}; the whole
 * `topic_partitions` array of the request is **nullable**, and a null one asks the controller to look at every
 * partition of the cluster.
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0 to v2)"
 */
class ElectLeadersRequestTopicPartitions implements BinarySchemaInterface
{
    /**
     * Name of the topic whose partitions should be elected
     */
    public string $topic;

    /**
     * Ids of the partitions of that topic
     *
     * @var list<int>
     */
    public array $partitionId;

    /**
     * @param list<int> $partitionIds Ids of the partitions whose leader should be elected
     */
    public function __construct(string $topic, array $partitionIds)
    {
        $this->topic       = $topic;
        $this->partitionId = array_values(array_map(intval(...), $partitionIds));
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'       => BinarySchema::TYPE_STRING,
            'partitionId' => [BinarySchema::TYPE_INT32],
        ];
    }
}
