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
 * The result of moving one replica, i.e. one entry of the `partitions` array of a topic
 *
 * <pre>
 *   AlterReplicaLogDirsResponsePartition => partition error_code
 *     partition  => INT32
 *     error_code => INT16
 * </pre>
 *
 * `ALTER_REPLICA_LOG_DIRS_RESPONSE_V0` in `AlterReplicaLogDirsResponse.schemaVersions()` @ 1.1.1. The error code 0
 * means that the move was **accepted**, not that it is finished: the broker creates the future log and starts a
 * `ReplicaAlterLogDirsThread` inside the request handler and answers immediately, and the copy runs in the
 * background. Whether it is done is a question for DescribeLogDirs.
 *
 * The codes that `AlterReplicaLogDirsResponse` @ 1.1.1 names, and what produces each of them:
 *
 * | Code | Name                | Request that produces it                                                    |
 * |------|---------------------|-------------------------------------------------------------------------------|
 * | 0    | None                | The move was accepted, or the replica already sits in the named directory     |
 * | 9    | ReplicaNotAvailable | The broker has no replica of that partition, including an unknown topic       |
 * | 56   | KafkaStorageError   | The named directory is configured but offline                                 |
 * | 57   | LogDirNotFound      | The path is not one of the directories of `log.dirs`, or it is relative       |
 * | 31   | ClusterAuthorizationFailed | The client may not `Alter` the cluster resource                        |
 *
 * @see docs/protocol/2.8.md, section "AlterReplicaLogDirs API (key 34, v0 to v2)"
 */
class AlterReplicaLogDirsResponsePartition implements BinarySchemaInterface
{
    /**
     * Id of the partition this entry belongs to
     */
    public int $partition;

    /**
     * Error code of this replica, 0 when the move was accepted
     */
    public int $errorCode;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
