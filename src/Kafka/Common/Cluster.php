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
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Common\Errors\UnknownErrorException;

/**
 * A representation of a subset of the nodes, topics, and partitions in the ApiKeys cluster.
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
     * Gets the list of available partitions for this topic
     *
     * @param string $topic Name of the topic
     *
     * @return array|PartitionMetadata[]
     */
    public function availablePartitionsForTopic($topic)
    {
        if (!isset($this->topicPartitions[$topic])) {
            throw new InvalidTopicException(['topic' => $topic]);
        }

        return $this->topicPartitions[$topic]->partitions;
    }

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

        $leaderId = $partitions[$partition]->leader;

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
            throw new UnknownErrorException(['nodeId' => $nodeId] + ['error' => 'Node was not found']);
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
            throw new InvalidTopicException(['topic' => $topic]);
        }

        return $this->topicPartitions[$topic]->partitions;
    }

    /**
     * Reloads the metadata from the broker and optionally save it in the cache
     *
     * @throws ApiKeys\Error\UnknownError If information can not be reloaded
     */
    public function reload(): void
    {
        $brokerAddresses = [];
        if (isset($this->configuration[ClientConfig::BOOTSTRAP_SERVERS])) {
            $brokerAddresses = $this->configuration[ClientConfig::BOOTSTRAP_SERVERS];
        }

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
            /** @var AbstractProtocolMessage\MetadataResponse $metadata */
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
