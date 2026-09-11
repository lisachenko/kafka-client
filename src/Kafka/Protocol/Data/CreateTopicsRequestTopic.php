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

use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One topic to create, i.e. one entry of the `create_topic_requests` array
 *
 * <pre>
 *   CreateTopicsRequestTopic => Topic NumPartitions ReplicationFactor [ReplicaAssignment] [Configs]
 *     Topic             => string
 *     NumPartitions     => int32
 *     ReplicationFactor => int16
 *     ReplicaAssignment => PartitionId [Replicas]
 *     Configs           => ConfigKey ConfigValue
 * </pre>
 *
 * `SINGLE_CREATE_TOPIC_REQUEST_V0` in `Protocol.java` @ 0.10.2.2, unchanged in version 1
 * (`SINGLE_CREATE_TOPIC_REQUEST_V1 = SINGLE_CREATE_TOPIC_REQUEST_V0`).
 *
 * The topic is described in exactly one of two ways, and the broker rejects a mixture of them with the error code
 * 42 (InvalidRequest), see `AdminManager.createTopics` @ 0.10.2.2:
 *
 *  - `numPartitions` and `replicationFactor` are set and the assignment array is EMPTY: the controller places the
 *    replicas itself with `AdminUtils.assignReplicasToBrokers`;
 *  - `numPartitions` and `replicationFactor` are both {@see NewTopic::NO_NUM_PARTITIONS} / -1 and the assignment
 *    array names the replicas of every partition.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v3)"
 */
class CreateTopicsRequestTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic to create
     */
    public string $topic;

    /**
     * Number of partitions to create, -1 when an explicit replica assignment is given
     */
    public int $numPartitions;

    /**
     * Number of replicas of each partition, -1 when an explicit replica assignment is given
     */
    public int $replicationFactor;

    /**
     * Explicit placement of the replicas, indexed by the partition id; empty for a broker-side assignment
     *
     * @var array<int, CreateTopicsRequestReplicaAssignment>
     */
    public array $replicaAssignment;

    /**
     * Topic-level configuration of the new topic, indexed by the option name
     *
     * @var array<string, CreateTopicsRequestConfig>
     */
    public array $configs;

    /**
     * @param array<int, list<int>> $replicasAssignments Broker ids of every partition, indexed by the partition id
     * @param array<string, string> $configs             Topic-level options of the new topic
     */
    public function __construct(
        string $topic,
        int $numPartitions,
        int $replicationFactor,
        array $replicasAssignments = [],
        array $configs = []
    ) {
        $assignments = [];
        foreach ($replicasAssignments as $partitionId => $replicas) {
            $assignments[$partitionId] = new CreateTopicsRequestReplicaAssignment(
                (int) $partitionId,
                array_values(array_map(intval(...), $replicas))
            );
        }

        $configEntries = [];
        foreach ($configs as $configKey => $configValue) {
            // The wire field is a NULLABLE_STRING in every version of the api, but a null value makes the
            // controller answer that topic with the error code -1, so a value given here is always a string
            $configEntries[$configKey] = new CreateTopicsRequestConfig((string) $configKey, (string) $configValue);
        }

        $this->topic             = $topic;
        $this->numPartitions     = $numPartitions;
        $this->replicationFactor = $replicationFactor;
        $this->replicaAssignment = $assignments;
        $this->configs           = $configEntries;
    }

    /**
     * Builds the wire entry of a topic that the caller described with an {@see NewTopic}
     */
    public static function fromNewTopic(NewTopic $newTopic): self
    {
        return new self(
            $newTopic->topic,
            $newTopic->numPartitions,
            $newTopic->replicationFactor,
            $newTopic->replicasAssignments,
            $newTopic->configs
        );
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'             => BinarySchema::TYPE_STRING,
            'numPartitions'     => BinarySchema::TYPE_INT32,
            'replicationFactor' => BinarySchema::TYPE_INT16,
            'replicaAssignment' => ['partitionId' => CreateTopicsRequestReplicaAssignment::class],
            'configs'           => ['configKey' => CreateTopicsRequestConfig::class],
        ];
    }
}
