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

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponseLogDir;

/**
 * What one log directory of one broker holds, as {@see AdminClient::describeLogDirs()} reports it
 *
 * `org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo` of the Java client, with its two fields:
 * the `error` of the directory and the `replicaInfos` that live in it. The Java field is an `Errors`; here it is
 * the exception of that error code, or `null` when the directory answered 0 - the same shape
 * {@see AdminClient::alterConfigs()} reports a resource with.
 *
 * PHP cannot use an object as an array key, so `replicaInfos` is indexed by the string form of a
 * {@see TopicPartition} - `events-0` - which {@see self::keyOf()} builds and {@see TopicPartition::__toString()}
 * produces.
 *
 * A directory that is configured but **offline** is reported with the error code 56 (KafkaStorageError) and an
 * empty `replicaInfos`, so an empty map alone does not mean that the disk is healthy - the error does.
 *
 * {@see self::$totalBytes} and {@see self::$usableBytes} are the two sizes of KIP-827 that Kafka 3.3 added to the
 * answer: the size and the free space of the **volume** the directory sits on, in bytes. They are
 * {@see self::UNKNOWN_BYTES} for every answer below the version 4 - the Java `LogDirDescription` reports them as
 * an empty `OptionalLong` there - and for a directory the broker could not measure. Two directories of the same
 * filesystem answer the same two numbers, which is what the two log directories of the node of this line do.
 *
 * {@see self::$isCordoned} is the flag of KIP-1066 that Kafka 4.3 added to the answer (version 5): `true` for a
 * directory listed in the dynamic per-broker option `cordoned.log.dirs`, which keeps and serves the replicas it
 * holds but takes no new one - the `isCordoned()` of the Java `LogDirDescription`. It is `false` for every answer
 * below the version 5, for a broker whose finalized `metadata.version` is below `4.3-IV0`, and for an offline
 * directory.
 *
 * @see docs/protocol/4.3.md, section "DescribeLogDirs API (key 35, v0 to v5)"
 */
final class LogDirInfo
{
    /**
     * The two sizes of a directory the broker did not measure, and of every answer below the version 4
     */
    public const int UNKNOWN_BYTES = DescribeLogDirsResponseLogDir::UNKNOWN_BYTES;

    /**
     * @param string                     $logDir       Absolute path of the directory, as the broker resolved it
     * @param KafkaException|null        $error        Error of the directory, null when it is online
     * @param array<string, ReplicaInfo> $replicaInfos Replicas in this directory, indexed by `topic-partition`
     * @param int                        $totalBytes   Size of the volume in bytes, -1 when it was not measured
     * @param int                        $usableBytes  Free bytes of the volume, -1 when it was not measured
     * @param bool                       $isCordoned   Whether the directory is cordoned, i.e. takes no new replica
     */
    public function __construct(
        public readonly string $logDir,
        public readonly ?KafkaException $error,
        public readonly array $replicaInfos,
        public readonly int $totalBytes = self::UNKNOWN_BYTES,
        public readonly int $usableBytes = self::UNKNOWN_BYTES,
        public readonly bool $isCordoned = false,
    ) {}

    /**
     * Builds the result from one `log_dirs` entry of a DescribeLogDirs answer
     */
    public static function fromResponseLogDir(DescribeLogDirsResponseLogDir $logDir): self
    {
        $replicaInfos = [];
        foreach ($logDir->topics as $topic) {
            foreach ($topic->partitions as $partition) {
                $key                = self::keyOf($topic->topic, $partition->partition);
                $replicaInfos[$key] = ReplicaInfo::fromResponsePartition($partition);
            }
        }

        return new self(
            $logDir->logDir,
            $logDir->errorCode === KafkaException::NO_ERROR
                ? null
                : KafkaException::fromCode($logDir->errorCode, ['logDir' => $logDir->logDir]),
            $replicaInfos,
            $logDir->totalBytes,
            $logDir->usableBytes,
            $logDir->isCordoned
        );
    }

    /**
     * Returns the key a replica of the given partition is indexed by, i.e. `events-0`
     */
    public static function keyOf(string $topic, int $partition): string
    {
        return (string) new TopicPartition($topic, $partition);
    }

    /**
     * Tells whether the broker measured the volume of this directory, i.e. whether the two sizes are real
     *
     * They are never measured below the version 4 of Kafka 3.3.
     */
    public function hasVolumeSizes(): bool
    {
        return $this->totalBytes !== self::UNKNOWN_BYTES && $this->usableBytes !== self::UNKNOWN_BYTES;
    }

    /**
     * Returns the replica of the given partition, or null when this directory does not hold it
     */
    public function replica(string $topic, int $partition): ?ReplicaInfo
    {
        return $this->replicaInfos[self::keyOf($topic, $partition)] ?? null;
    }
}
