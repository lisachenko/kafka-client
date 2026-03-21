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
 * @date   17.08.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\IO\Stream;

class StreamGroupRequest
{
    public $configuration;
    /**
     * List of streams for the group
     *
     * @var Stream[]
     */
    private readonly array $streams;

    /**
     * Storage of resources for each connection
     *
     * @var resource[]
     */
    private $resources;

    /**
     * @param string $requestClass
     * @param string $responseClass
     */
    public function __construct(/**
     * Instance of current cluster
     */
        private readonly Cluster $cluster, /**
     * Name of the request class
     */
        private $requestClass, /**
     * Name of the response class
     */
        private $responseClass,
        Stream ...$streams
    ) {
        $this->streams = $streams;
        foreach ($streams as $stream) {
            $stream->joinGroup($this);
        }
    }

    public function registerHandle(Stream $stream, $resource): void
    {
        $index = array_search($stream, $this->streams);
        if ($index === false) {
            throw new \InvalidArgumentException('Unknown stream registration');
        }
        $this->resources[$index] = $resource;
    }

    public function request(array $topicPartitionData, ...$requestArguments): void
    {
        $requestByNode = [];

        // We group all requests for each node by looking at partition leader
        foreach ($topicPartitionData as $topic => $partitions) {
            foreach ($partitions as $partition => $partitionData) {
                $leaderNode = $this->cluster->leaderFor($topic, $partition);
                $requestByNode[$leaderNode->nodeId][$topic][$partition] = $partitionData;
            }
        }

        $readNodeSockets = [];
        foreach ($requestByNode as $nodeId => $nodeTopicPartitions) {
            /** @var AbstractProtocolMessage $request */
            $request = new $this->requestClass($nodeTopicPartitions, ...$requestArguments);

            //TODO: need to receive configuration from somewhere, or encapsulate all logic in the cluster
            $stream  = $this->cluster->nodeById($nodeId)->getConnection($this->configuration);

            $streamIndex = array_search($stream, $this->resources);

            $readNodeSockets[$nodeId] = $this->resources[$streamIndex];
            $request->writeTo($stream);
        }

        $incompleteReads = $readNodeSockets;
        $timeout ??= $this->configuration[ConsumerConfig::REQUEST_TIMEOUT_MS];
        $responses = [];
        while ($incompleteReads !== []) {
            $readSelect  = $incompleteReads;
            $writeSelect = $exceptSelect = null;
            if (stream_select($readSelect, $writeSelect, $exceptSelect, intdiv($timeout, 1000), $timeout % 1000) > 0) {
                foreach ($readSelect as $resourceToRead) {
                    $nodeId             = array_search($resourceToRead, $readNodeSockets);
                    $connection         = $this->cluster->nodeById($nodeId)->getConnection($this->configuration);
                    $responses[$nodeId] = $responseClass::unpack($connection);
                }
                $incompleteReads = array_diff($incompleteReads, $readSelect);
            }
        }
    }
}
