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
use Protocol\Kafka\Protocol\Data\ElectLeadersResponseReplicaElectionResult;

/**
 * ElectLeaders response object, version 0 (key 43, Kafka 2.2)
 *
 * <pre>
 *   ElectLeaders Response (Version: 0) => throttle_time_ms [replica_election_results]
 *     throttle_time_ms         => INT32
 *     replica_election_results => topic [partition_result]
 *       topic            => STRING
 *       partition_result => partition_id error_code error_message
 * </pre>
 *
 * Version 0 has **no top-level error code**: `ElectLeadersResponse(throttleTimeMs, errorCode, results, version)`
 * @ 2.8.2 writes the code into the answer only `if (version >= 1)`, so an error of the whole request - the **31**
 * `ClusterAuthorizationFailed` of a client that may not `Alter` the cluster - reaches a version 0 client as that
 * code on every partition it named instead. Kafka 2.4 added the top-level field with version 1 (KIP-460), which is
 * also the version that carries an `election_type`.
 *
 * Everything else is reported per partition
 * ({@see \Protocol\Kafka\Protocol\Data\ElectLeadersResponsePartitionResult}), and the entries are grouped by topic
 * in the order of the controller's map, not in the order of the request. A request with a **null** topic array
 * gets the partitions that were really elected or really failed alone: the handler drops every
 * `ELECTION_NOT_NEEDED` from such an answer.
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0)"
 */
class ElectLeadersResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Election result of every topic of the answer, indexed by the topic name
     *
     * @var array<string, ElectLeadersResponseReplicaElectionResult>
     */
    public array $replicaElectionResults = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs'         => BinarySchema::TYPE_INT32,
            'replicaElectionResults' => ['topic' => ElectLeadersResponseReplicaElectionResult::class],
        ];
    }
}
