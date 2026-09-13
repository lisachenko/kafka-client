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
 * The election results of one topic, i.e. one entry of the `replica_election_results` array
 *
 * <pre>
 *   ElectLeadersResponseReplicaElectionResult => topic [partition_result]
 *     topic            => STRING
 *     partition_result => ElectLeadersResponsePartitionResult
 * </pre>
 *
 * `ReplicaElectionResult` of `ElectLeadersResponse.json` @ 2.8.2. The controller groups its per-partition results
 * by topic - `adjustedResults.groupBy { case (tp, _) => tp.topic }` in `KafkaApis.handleElectReplicaLeader` - so
 * the order of the topics is the order of that map and not the order of the request.
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0 to v2)"
 */
class ElectLeadersResponseReplicaElectionResult implements BinarySchemaInterface
{
    /**
     * Name of the topic these results belong to
     */
    public string $topic;

    /**
     * Result of every partition of that topic, indexed by the partition id
     *
     * @var array<int, ElectLeadersResponsePartitionResult>
     */
    public array $partitionResult = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'           => BinarySchema::TYPE_STRING,
            'partitionResult' => ['partitionId' => ElectLeadersResponsePartitionResult::class],
        ];
    }
}
