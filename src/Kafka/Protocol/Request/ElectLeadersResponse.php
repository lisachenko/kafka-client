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
 * ElectLeaders response object, version 1 (key 43, Kafka 2.2)
 *
 * <pre>
 *   ElectLeaders Response (Version: 0 and 1) => throttle_time_ms error_code [replica_election_results]
 *     throttle_time_ms         => INT32
 *     error_code               => INT16      -- since version 1
 *     replica_election_results => topic [partition_result]
 *       topic            => STRING
 *       partition_result => partition_id error_code error_message
 * </pre>
 *
 * Version 0 has **no top-level error code**: `ElectLeadersResponse(throttleTimeMs, errorCode, results, version)`
 * @ 2.8.2 writes the code into the answer only `if (version >= 1)`, so an error of the whole request - the **31**
 * `ClusterAuthorizationFailed` of a client that may not `Alter` the cluster - reaches a version 0 client as that
 * code on every partition it named instead. **Kafka 2.4 added the field with version 1** (KIP-460), the version
 * that also carries the `election_type`; {@see ElectLeadersResponseV0} is the frame without it.
 *
 * The top-level code is `ApiError.NONE` for everything `handleElectReplicaLeader` @ 2.8.2 reaches the controller
 * with - the **0** of every answer of this document - and the 31 of a client the authorizer refused, which is the
 * one case in which the handler never asks the controller at all.
 *
 * Everything else is reported per partition
 * ({@see \Protocol\Kafka\Protocol\Data\ElectLeadersResponsePartitionResult}), and the entries are grouped by topic
 * in the order of the controller's map, not in the order of the request. A request with a **null** topic array
 * gets the partitions that were really elected or really failed alone: the handler drops every
 * `ELECTION_NOT_NEEDED` from such an answer.
 *
 * **Kafka 2.4 added the version 2** (KIP-482), the same frame in the flexible encoding.
 * {@see ElectLeadersResponseV1} is the version that carries the top-level error code without it.
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0 to v2)"
 */
class ElectLeadersResponse extends AbstractResponse
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
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Error code of the whole request, 0 for everything that reached the controller
     *
     * @since Version 1 of protocol
     */
    public int $errorCode = 0;

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
        $body   = ['throttleTimeMs' => BinarySchema::TYPE_INT32];
        if (static::VERSION >= 1) {
            $body['errorCode'] = BinarySchema::TYPE_INT16;
        }
        $body['replicaElectionResults'] = ['topic' => ElectLeadersResponseReplicaElectionResult::class];

        return $header + $body;
    }
}
