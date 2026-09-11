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
 * The result of the election of one partition, i.e. one entry of a `replica_election_results` topic
 *
 * <pre>
 *   ElectLeadersResponsePartitionResult => partition_id error_code error_message
 *     partition_id  => INT32
 *     error_code    => INT16
 *     error_message => NULLABLE_STRING
 * </pre>
 *
 * `PartitionResult` of `ElectLeadersResponse.json` @ 2.8.2. The error is per partition and it is the only place
 * the api reports anything at all in version 0, which has no top-level error code
 * ({@see \Protocol\Kafka\Protocol\Request\ElectLeadersResponse}). The codes
 * `KafkaController.processReplicaLeaderElection` @ 2.8.2 produces are:
 *
 * | Code | Name                         | Meaning                                                              |
 * |------|------------------------------|----------------------------------------------------------------------|
 * | 0    | None                         | The partition has a new leader now                                   |
 * | 3    | UnknownTopicOrPartition      | `The partition does not exist.`                                      |
 * | 17   | InvalidTopic                 | `The topic is being deleted`                                         |
 * | 80   | PreferredLeaderNotAvailable  | A preferred election whose preferred replica is not in the ISR       |
 * | 84   | ElectionNotNeeded            | The partition already has the leader the election would give it      |
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0 and v1)"
 */
class ElectLeadersResponsePartitionResult implements BinarySchemaInterface
{
    /**
     * Id of the partition this result belongs to
     */
    public int $partitionId;

    /**
     * Error code of the election of that partition, 0 when it got a new leader
     */
    public int $errorCode;

    /**
     * Message of the controller for that error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionId'  => BinarySchema::TYPE_INT32,
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
