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
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;

/**
 * A representation of a subset of the nodes, topics, and partitions in the ApiKeys cluster.
 */
final class Cluster
{
    /**
     * Creates a new cluster with the given nodes and partitions
     */
    private function __construct(
        /**
         * List of broker nodes
         *
         * @var Node[]|array
         */
        private array $nodes,
        /**
         * Topic partitions
         *
         * @var TopicMetadata[]|array
         */
        private array $topicPartitions
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
        $brokerAddresses = [];
        if (isset($configuration[ClientConfig::BOOTSTRAP_SERVERS])) {
            $brokerAddresses = $configuration[ClientConfig::BOOTSTRAP_SERVERS];
        };

        foreach ($brokerAddresses as $address) {
            try {
                $stream = new SocketStream($address, $configuration);
                break;
            } catch (NetworkException) {
                // we ignore all network errors and just try the next one address
                continue;
            }
        }
        if (!isset($stream)) {
            throw new NetworkException(['brokerAddresses' => $brokerAddresses]);
        }
        $metadata = self::fetchMetadata($stream);
        $cluster  = new Cluster($metadata->brokers, $metadata->topics);

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
    public function nodeById($nodeId): ?Node
    {
        if (!isset($this->nodes[$nodeId])) {
            return null;
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
     * Get all topics
     *
     * @return string[]
     */
    public function topics(): array
    {
        return array_keys($this->topicPartitions);
    }

    /**
     * Queries metadata from the broker
     *
     * @param Stream $stream
     *
     * @return AbstractProtocolMessage\MetadataResponse
     */
    private static function fetchMetadata(Stream $stream)
    {
        $request = new MetadataRequest();
        $request->writeTo($stream);

        return MetadataResponse::unpack($stream);
    }
}
