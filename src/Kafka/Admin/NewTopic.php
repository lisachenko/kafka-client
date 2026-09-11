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

namespace Protocol\Kafka\Admin;

/**
 * Description of a topic that {@see AdminClient::createTopics()} should create
 *
 * The fields are the ones of `CreateTopicsRequest.TopicDetails` @ 0.10.2.2 and of the `NewTopic` of the later Java
 * admin client: a topic is either described by the number of partitions and the replication factor it should get,
 * and the controller places the replicas itself, or by an explicit assignment of broker ids to partitions. The two
 * forms exclude each other, and the broker answers a request that mixes them with the error code 42
 * (InvalidRequest), see `AdminManager.createTopics` @ 0.10.2.2.
 *
 * **Kafka 2.4 added a third form** (KIP-464): both numbers may be -1 with NO assignment, which asks the broker for
 * its own `num.partitions` and `default.replication.factor`. It needs the **version 4** of the api - a lower
 * version is refused by the client, see the section of the document - and it is what the Java
 * `NewTopic(String, Optional.empty(), Optional.empty())` sends.
 *
 * <code>
 *   // three partitions with one replica each, placed by the controller, kept for an hour
 *   new NewTopic('events', 3, 1, configs: ['retention.ms' => '3600000']);
 *
 *   // two partitions, both hosted by the broker 0, which is also their preferred leader
 *   NewTopic::withReplicaAssignment('events', [0 => [0], 1 => [0]]);
 *
 *   // whatever `num.partitions` and `default.replication.factor` say on the broker (KIP-464, CreateTopics v4)
 *   NewTopic::withBrokerDefaults('events');
 * </code>
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v4)"
 */
final class NewTopic
{
    /**
     * Number of partitions of a topic whose replicas are assigned explicitly, or whose count the broker chooses
     *
     * `CreateTopicsRequest.NO_NUM_PARTITIONS` @ 0.10.2.2. From the **version 4** of the api (Kafka 2.4, KIP-464)
     * it also means "use the `num.partitions` of the broker" when there is no assignment either.
     */
    public const int NO_NUM_PARTITIONS = -1;

    /**
     * Replication factor of a topic whose replicas are assigned explicitly, or which the broker chooses
     *
     * `CreateTopicsRequest.NO_REPLICATION_FACTOR` @ 0.10.2.2. From the **version 4** of the api (Kafka 2.4,
     * KIP-464) it also means "use the `default.replication.factor` of the broker" when there is no assignment.
     */
    public const int NO_REPLICATION_FACTOR = -1;

    /**
     * @param string                $topic               Name of the topic to create
     * @param int                   $numPartitions       Partitions to create, {@see self::NO_NUM_PARTITIONS} when
     *                                                   the replicas are assigned explicitly
     * @param int                   $replicationFactor   Replicas of each partition,
     *                                                   {@see self::NO_REPLICATION_FACTOR} with an explicit
     *                                                   assignment
     * @param array<int, list<int>> $replicasAssignments Broker ids that host each partition, indexed by the
     *                                                   partition id, the preferred leader first; empty to let the
     *                                                   controller place the replicas
     * @param array<string, string> $configs             Topic-level options of the new topic, e.g.
     *                                                   `['retention.ms' => '3600000']`
     */
    public function __construct(
        public readonly string $topic,
        public readonly int $numPartitions = self::NO_NUM_PARTITIONS,
        public readonly int $replicationFactor = self::NO_REPLICATION_FACTOR,
        public readonly array $replicasAssignments = [],
        public readonly array $configs = []
    ) {}

    /**
     * Describes a topic whose replicas the caller places itself
     *
     * @param array<int, list<int>> $replicasAssignments Broker ids that host each partition, indexed by the
     *                                                   partition id, the preferred leader first
     * @param array<string, string> $configs             Topic-level options of the new topic
     */
    public static function withReplicaAssignment(
        string $topic,
        array $replicasAssignments,
        array $configs = []
    ): self {
        return new self(
            $topic,
            self::NO_NUM_PARTITIONS,
            self::NO_REPLICATION_FACTOR,
            $replicasAssignments,
            $configs
        );
    }

    /**
     * Describes a topic whose partition count and replication factor the BROKER chooses (Kafka 2.4, KIP-464)
     *
     * Both numbers travel as -1 and no assignment is sent, so the controller falls back to the `num.partitions`
     * and the `default.replication.factor` of its own configuration. The shape needs the **version 4** of the api,
     * and {@see \Protocol\Kafka\Protocol\Request\CreateTopicsRequest} refuses it for a lower one - a broker of
     * Kafka 2.3 or below has no fallback for the -1 and answers the topic with 37 or 38.
     *
     * @param array<string, string> $configs Topic-level options of the new topic
     */
    public static function withBrokerDefaults(string $topic, array $configs = []): self
    {
        return new self($topic, self::NO_NUM_PARTITIONS, self::NO_REPLICATION_FACTOR, [], $configs);
    }

    /**
     * Returns a copy of this description with the given topic-level options
     *
     * @param array<string, string> $configs Topic-level options of the new topic
     */
    public function withConfigs(array $configs): self
    {
        return new self(
            $this->topic,
            $this->numPartitions,
            $this->replicationFactor,
            $this->replicasAssignments,
            $configs
        );
    }
}
