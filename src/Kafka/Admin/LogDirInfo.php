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
 * @see docs/protocol/2.8.md, section "DescribeLogDirs API (key 35, v0 to v2)"
 */
final class LogDirInfo
{
    /**
     * @param string                     $logDir       Absolute path of the directory, as the broker resolved it
     * @param KafkaException|null        $error        Error of the directory, null when it is online
     * @param array<string, ReplicaInfo> $replicaInfos Replicas in this directory, indexed by `topic-partition`
     */
    public function __construct(
        public readonly string $logDir,
        public readonly ?KafkaException $error,
        public readonly array $replicaInfos,
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
            $replicaInfos
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
     * Returns the replica of the given partition, or null when this directory does not hold it
     */
    public function replica(string $topic, int $partition): ?ReplicaInfo
    {
        return $this->replicaInfos[self::keyOf($topic, $partition)] ?? null;
    }
}
