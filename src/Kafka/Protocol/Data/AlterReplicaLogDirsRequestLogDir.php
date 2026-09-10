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
 * One destination directory of an AlterReplicaLogDirs request, i.e. one entry of the `log_dirs` array
 *
 * <pre>
 *   AlterReplicaLogDirsRequestLogDir => log_dir [topics]
 *     log_dir => STRING
 *     topics  => AlterReplicaLogDirsRequestTopic
 * </pre>
 *
 * `ALTER_REPLICA_LOG_DIRS_REQUEST_V0` in `AlterReplicaLogDirsRequest.schemaVersions()` @ 1.1.1: "The absolute log
 * directory path." **Absolute** is not a hint - `LogManager.isLogDirOnline` compares the value against
 * `logDirs.map(_.getAbsolutePath)`, so a relative path is refused with the error code 57 even when it names one of
 * the configured directories.
 *
 * The directory the replica already sits in is a legal destination: the broker answers 0 and creates no future log
 * at all, because `Partition.maybeCreateFutureReplica` only creates one when the destination differs from the
 * current directory.
 *
 * @see docs/protocol/1.1.md, section "AlterReplicaLogDirs API (key 34, v0)"
 */
class AlterReplicaLogDirsRequestLogDir implements BinarySchemaInterface
{
    /**
     * Absolute path of the directory the replicas of this entry should be moved to
     */
    public string $logDir;

    /**
     * Topics whose replicas should move into this directory, indexed by the topic name
     *
     * @var array<string, AlterReplicaLogDirsRequestTopic>
     */
    public array $topics;

    /**
     * A value of the `$topicPartitions` map is either a list of partition ids or an already built topic DTO.
     *
     * @param string                                                    $logDir          Absolute path of the target
     * @param array<string, list<int>|AlterReplicaLogDirsRequestTopic>   $topicPartitions Replicas to move, as
     *        topic => list of partition ids
     */
    public function __construct(string $logDir, array $topicPartitions)
    {
        $topics = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $topics[$topic] = $partitions instanceof AlterReplicaLogDirsRequestTopic
                ? $partitions
                : new AlterReplicaLogDirsRequestTopic((string) $topic, $partitions);
        }

        $this->logDir = $logDir;
        $this->topics = $topics;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'logDir' => BinarySchema::TYPE_STRING,
            'topics' => ['topic' => AlterReplicaLogDirsRequestTopic::class],
        ];
    }
}
