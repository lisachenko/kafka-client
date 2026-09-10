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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\AlterReplicaLogDirsRequestLogDir;

/**
 * AlterReplicaLogDirs, version 0: moves a replica to another disk of the same broker (ApiKey 34, Kafka 1.0, KIP-113)
 *
 * <pre>
 *   AlterReplicaLogDirs Request (Version: 0) => [log_dirs]
 *     log_dirs => log_dir [topics]
 *       log_dir => STRING
 *       topics  => topic [partitions]
 *         topic      => STRING
 *         partitions => INT32
 * </pre>
 *
 * The counterpart of DescribeLogDirs: it says where a replica **should** live, so that an administrator can even out
 * the disks of a broker without moving the partition to another broker. Like DescribeLogDirs it is broker-local -
 * `KafkaApis.handleAlterReplicaLogDirsRequest` hands the map to the local `ReplicaManager.alterReplicaLogDirs` - so
 * it goes to the broker that hosts the replica, which is what
 * {@see \Protocol\Kafka\Admin\AdminClient::alterReplicaLogDirs()} does.
 *
 * The frame is grouped by destination directory, because that is the value of the map: the Java client keeps the
 * request as `Map<TopicPartition, String>` and inverts it while it writes the frame. Naming the same replica in two
 * directories of one request is therefore possible on the wire and answered per replica, in the order the broker
 * walks the map.
 *
 * **The answer only says that the move was accepted.** The broker creates the future log and starts the
 * `ReplicaAlterLogDirsThread` synchronously, then answers 0; the copy itself runs in the background, and the replica
 * is visible in *both* directories - as the current log and as the future log - until it is done. What "done" means
 * is a DescribeLogDirs question: the entry with `is_future = true` disappears and the replica is reported in the
 * destination alone.
 *
 * @see docs/protocol/1.1.md, section "AlterReplicaLogDirs API (key 34, v0)"
 */
class AlterReplicaLogDirsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::ALTER_REPLICA_LOG_DIRS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Destination directories of this request, indexed by their absolute path
     *
     * @var array<string, AlterReplicaLogDirsRequestLogDir>
     */
    protected readonly array $logDirs;

    /**
     * A value of the `$logDirTopicPartitions` map is either a topic => partitions map or an already built DTO.
     *
     * @param array<string, array<string, list<int>>|AlterReplicaLogDirsRequestLogDir> $logDirTopicPartitions
     *        Replicas to move, as absolute log directory => topic => list of partition ids
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(array $logDirTopicPartitions, string $clientId = '', int $correlationId = 0)
    {
        $packedLogDirs = [];
        foreach ($logDirTopicPartitions as $logDir => $topicPartitions) {
            $packedLogDirs[$logDir] = $topicPartitions instanceof AlterReplicaLogDirsRequestLogDir
                ? $topicPartitions
                : new AlterReplicaLogDirsRequestLogDir((string) $logDir, $topicPartitions);
        }
        $this->logDirs = $packedLogDirs;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'logDirs' => ['logDir' => AlterReplicaLogDirsRequestLogDir::class],
        ];
    }
}
