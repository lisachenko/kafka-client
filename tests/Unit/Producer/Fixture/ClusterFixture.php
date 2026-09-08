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

namespace Protocol\Kafka\Tests\Unit\Producer\Fixture;

use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\TopicMetadata;

/**
 * Builds a {@see Cluster} of a fixed metadata set, without talking to any broker.
 *
 * `Cluster` is final and only reachable through `Cluster::bootstrap()`, which either asks a broker for the metadata
 * or reads them out of the file that the `metadata.cache.file` option points at. This fixture writes such a file, so
 * that a unit test can hand a producer a cluster of the shape it wants to exercise.
 */
final class ClusterFixture
{
    /**
     * Files written by this fixture, removed by {@see ClusterFixture::cleanUp()}
     *
     * @var list<string>
     */
    private static array $cacheFiles = [];

    /**
     * This class is only a namespace for the factory below and is never instantiated
     */
    private function __construct() {}

    /**
     * Builds a cluster of one topic whose partitions have the given leaders
     *
     * @param array<string, array<int, int>> $topicPartitionLeaders Leader node id of every partition, `-1` for a
     *                                                             partition that currently has no leader
     * @param array<int, string>             $nodes                 Address of every broker node, indexed by node id
     * @param array<string, mixed>           $configuration         Extra client options of the cluster
     */
    public static function withPartitions(
        array $topicPartitionLeaders,
        array $nodes = [1 => '127.0.0.1:9092'],
        array $configuration = []
    ): Cluster {
        $brokers = [];
        foreach ($nodes as $nodeId => $address) {
            [$host, $port] = explode(':', $address);

            $node         = new Node();
            $node->nodeId = $nodeId;
            $node->host   = $host;
            $node->port   = (int) $port;

            $brokers[$nodeId] = $node;
        }

        $topics = [];
        foreach ($topicPartitionLeaders as $topicName => $partitionLeaders) {
            $partitions = [];
            foreach ($partitionLeaders as $partitionId => $leader) {
                $partition              = new PartitionMetadata();
                $partition->partitionId = $partitionId;
                $partition->leader      = $leader;
                $partition->replicas    = $leader === -1 ? [] : [$leader];
                $partition->isr         = $leader === -1 ? [] : [$leader];

                $partitions[$partitionId] = $partition;
            }

            $topic             = new TopicMetadata();
            $topic->topic      = $topicName;
            $topic->partitions = $partitions;

            $topics[$topicName] = $topic;
        }

        $metadata          = new \stdClass();
        $metadata->brokers = $brokers;
        $metadata->topics  = $topics;

        $cacheFile = tempnam(sys_get_temp_dir(), 'kafka-cluster-') . '.php';
        file_put_contents(
            $cacheFile,
            '<?php return ' . var_export([(int) (microtime(true) * 1e3), $metadata], true) . ';'
        );
        self::$cacheFiles[] = $cacheFile;

        return Cluster::bootstrap(self::configuration($cacheFile, $configuration));
    }

    /**
     * Returns the client configuration that reads the metadata of a fixture cluster back
     *
     * @param array<string, mixed> $configuration Extra client options
     *
     * @return array<string, mixed>
     */
    public static function configuration(string $cacheFile, array $configuration = []): array
    {
        return $configuration + [
            ClientConfig::METADATA_CACHE_FILE => $cacheFile,
            ClientConfig::METADATA_MAX_AGE_MS => 300000,
            ClientConfig::BOOTSTRAP_SERVERS   => [],
        ];
    }

    /**
     * Returns the cache file of the cluster that was built last
     */
    public static function lastCacheFile(): string
    {
        return self::$cacheFiles[count(self::$cacheFiles) - 1];
    }

    /**
     * Removes every metadata file that this fixture wrote
     */
    public static function cleanUp(): void
    {
        foreach (self::$cacheFiles as $cacheFile) {
            if (is_file($cacheFile)) {
                unlink($cacheFile);
            }
        }
        self::$cacheFiles = [];
    }
}
