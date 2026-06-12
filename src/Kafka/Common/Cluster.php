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
/**
 * @author Alexander.Lisachenko
 * @date   29.07.2016
 */

namespace Protocol\Kafka\Common;

use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Record\MetadataRequest;
use Protocol\Kafka\Common\Record\MetadataResponse;
use Protocol\Kafka\IO\SocketStream;

/**
 * A representation of a subset of the nodes, topics, and partitions in the Kafka cluster.
 */
final class Cluster
{
    /**
     * List of broker nodes
     *
     * @var Node[]|array
     */
    private $nodes = [];

    /**
     * Topic partitions
     *
     * @var TopicMetadata[]|array
     */
    private $topicPartitions = [];

    /**
     * Creates a new cluster with the given nodes and partitions
     *
     * @param array $configuration Client configuration
     */
    private function __construct(
        /**
         * Client configuration
         */
        private array $configuration
    ) {}

    /**
     * Creates a "bootstrap" cluster using the given list of host/ports
     *
     * @param array $configuration Broker client configuration
     *
     * @return Cluster
     */
    public static function bootstrap(array $configuration): Cluster
    {
        $cluster        = new Cluster($configuration);
        $isCacheEnabled = !empty($configuration[ClientConfig::METADATA_CACHE_FILE]);
        $isLoaded       = false;

        if ($isCacheEnabled) {
            $isLoaded = $cluster->loadFromCache();
        }
        if (!$isLoaded) {
            $cluster->reload();
        }

        return $cluster;
    }

    /**
     * Gets the current leader for the given topic-partition
     *
     * @param string  $topic     Name of the topic
     * @param integer $partition Number of the partition
     *
     * @return Node
     */
    public function leaderFor($topic, $partition)
    {
        $partitions = $this->partitionsForTopic($topic);
        if (!isset($partitions[$partition])) {
            throw new UnknownTopicOrPartitionException(['topic' => $topic, 'partition' => $partition]);
        }

        $meta = $partitions[$partition];
        if ($meta->partitionErrorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($meta->partitionErrorCode, ['topic' => $topic, 'partition' => $partition]);
        }

        $leaderId = $meta->leader;
        if (!isset($this->nodes[$leaderId])) {
            throw new UnknownErrorException(
                [
                    'message'         => 'Can not find node for leader',
                    'topic'           => $topic,
                    'partition'       => $partition,
                    'leader'          => $leaderId,
                    'topicPartitions' => $this->topicPartitions,
                    'nodes'           => $this->nodes,
                ]
            );
        }

        return $this->nodes[$leaderId];
    }

    /**
     * Gets the node by the node id (or null if no such node exists)
     *
     * @param integer $nodeId Node identifier
     *
     * @return null|Node
     */
    public function nodeById($nodeId)
    {
        if (!isset($this->nodes[$nodeId])) {
            $this->reload();
            if (!isset($this->nodes[$nodeId])) {
                throw new UnknownErrorException(['nodeId' => $nodeId] + ['error' => 'Node was not found']);
            }
        }

        return $this->nodes[$nodeId];
    }

    /**
     * Returns the list of nodes in the cluster
     *
     * @return Node[]
     */
    public function nodes()
    {
        return $this->nodes;
    }

    /**
     * Gets the metadata for the specified partition
     *
     * @param string  $topic     Name of the topic
     * @param integer $partition Number of the partition
     *
     * @return PartitionMetadata
     */
    public function partition($topic, $partition)
    {
        $partitions = $this->partitionsForTopic($topic);
        if (!isset($partitions[$partition])) {
            throw new UnknownTopicOrPartitionException(['topic' => $topic, 'partition' => $partition]);
        }

        return $partitions[$partition];
    }

    /**
     * Gets the list of partitions for this topic
     *
     * @param string $topic Name of the topic
     *
     * @return PartitionMetadata[]
     */
    public function partitionsForTopic($topic)
    {
        if (!isset($this->topicPartitions[$topic])) {
            $this->reload();
            if (!isset($this->topicPartitions[$topic])) {
                throw new InvalidTopicException(['topic' => $topic]);
            }
        }

        $meta = $this->topicPartitions[$topic];
        if ($meta->topicErrorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($meta->topicErrorCode, ['topic' => $topic]);
        }

        return $meta->partitions;
    }

    /**
     * Reloads the metadata from the broker and optionally save it in the cache
     *
     * @throws UnknownErrorException If information can not be reloaded
     */
    public function reload(): void
    {
        $brokerAddresses = $this->configuration[ClientConfig::BOOTSTRAP_SERVERS] ?? [];

        $cause = [];
        foreach ($brokerAddresses as $address) {
            try {
                $stream  = new SocketStream($address, $this->configuration);
                $request = new MetadataRequest();
                $request->writeTo($stream);

                $metadata = MetadataResponse::unpack($stream);
                break;
            } catch (NetworkException $e) {
                // we ignore all network errors and just try the next one address
                $cause[$address] = $e->getMessage();
                continue;
            }
        }
        if (empty($metadata)) {
            throw new UnknownErrorException(
                [
                    'error' => 'Can not fetch information about cluster metadata',
                    'cause' => $cause,
                ]
            );
        }

        $isCacheEnabled = !empty($this->configuration[ClientConfig::METADATA_CACHE_FILE]);
        if ($isCacheEnabled) {
            $milliSeconds = (int) (microtime(true) * 1e3);
            $content      = '<?php return ' . var_export([$milliSeconds, $metadata], true) . ';';
            $cacheFile    = $this->configuration[ClientConfig::METADATA_CACHE_FILE];
            file_put_contents($cacheFile, $content);
            if (function_exists('opcache_invalidate')) {
                opcache_invalidate($cacheFile, true);
            }
        }

        $this->nodes           = $metadata->brokers;
        $this->topicPartitions = $metadata->topics;
    }

    /**
     * Get all topics
     *
     * @return string[]
     */
    public function topics(): array
    {
        return array_keys($this->topicPartitions);
    }

    /**
     * Loads cluster configuration from the cache
     *
     * @return boolean True if metadata was successfully loaded from the cache
     */
    private function loadFromCache(): bool
    {
        $milliSeconds = (int) (microtime(true) * 1e3);
        $cacheFile    = $this->configuration[ClientConfig::METADATA_CACHE_FILE];
        if (is_readable($cacheFile)) {
            /** @var Record\MetadataResponse $metadata */
            [$cachePutTimeMs, $metadata] = include $cacheFile;
            if (($milliSeconds - $cachePutTimeMs) < $this->configuration[ClientConfig::METADATA_MAX_AGE_MS]) {
                $this->nodes           = $metadata->brokers;
                $this->topicPartitions = $metadata->topics;

                return true;
            }
        }

        return false;
    }
}
