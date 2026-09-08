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
 * @date 02.08.2016
 */

namespace Protocol\Kafka;

use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\ConsumerConfig as ConsumerConfig;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\ProducerConfig as ProducerConfig;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;

/**
 * Low-level client for the Kafka 0.8.2.2 protocol
 *
 * Kafka 0.8 has no broker-side group membership (the API keys 11-14 were only added in 0.9), therefore this client
 * only speaks Produce, Fetch, Offsets, OffsetCommit, OffsetFetch and GroupCoordinator (ConsumerMetadata in 0.8.2).
 */
class Client
{
    public function __construct(
        /**
         * Cluster configuration
         */
        private readonly Cluster $cluster,
        /**
         * Client configuration
         */
        private array $configuration = []
    ) {}

    /**
     * Produce messages to the specific topic partition
     *
     * @param array $topicPartitionMessages List of messages for each topic and partition
     *
     * @return ProduceResponse
     */
    public function produce(array $topicPartitionMessages)
    {
        $result = $this->clusterRequest($topicPartitionMessages, function (array $nodeTopicPartitionMessages): ProduceRequest {
            $request = new ProduceRequest(
                $nodeTopicPartitionMessages,
                $this->configuration[ProducerConfig::ACKS],
                $this->configuration[ProducerConfig::TIMEOUT_MS],
                $this->configuration[ProducerConfig::CLIENT_ID]
            );

            return $request;
        }, ProduceResponse::class, function (array $result, ProduceResponse $response): array {
            /** @var ProduceResponsePartition[] $partitions */
            foreach ($response->topics as $topic => $partitions) {
                foreach ($partitions as $partitionId => $partitionInfo) {
                    if ($partitionInfo->errorCode !== 0) {
                        throw KafkaException::fromCode($partitionInfo->errorCode, ['topic' => $topic, 'partitionId' => $partitionId]);
                    }
                    $result[$topic][$partitionId] = $partitionInfo;
                }
            }
            return $result;
        });

        return $result;
    }

    /**
     * Commits the offsets for topic partitions for the concrete consumer group
     *
     * @param Node   $coordinatorNode       Current offset coordinator for $groupId
     * @param string $groupId               Name of the group
     * @param array  $topicPartitionOffsets List of topic => partitions for fetching information
     *
     * @throws Common\Errors\OffsetMetadataTooLargeException
     * @throws Common\Errors\GroupLoadInProgressException
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     * @throws Common\Errors\NotCoordinatorForGroupException
     */
    public function commitGroupOffsets(Node $coordinatorNode, $groupId, array $topicPartitionOffsets): void
    {
        $stream  = $coordinatorNode->getConnection($this->configuration);
        $request = new OffsetCommitRequest(
            $groupId,
            $topicPartitionOffsets,
            $this->configuration[ConsumerConfig::CLIENT_ID]
        );
        $request->writeTo($stream);
        $response = OffsetCommitResponse::unpack($stream);
        foreach ($response->topics as $topic => $partitions) {
            foreach ($partitions as $partitionId => $errorCode) {
                if ($errorCode !== 0) {
                    throw KafkaException::fromCode($errorCode, ['topic' => $topic, 'partitionId' => $partitionId]);
                }
            }
        }
    }

    /**
     * Fetches the offsets for topic partition for the concrete consumer group
     *
     * @param Node   $coordinatorNode Current offset coordinator for $groupId
     * @param string $groupId         Name of the group
     * @param array $topicPartitions  List of topic => partitions for fetching information
     *
     * @return array
     *
     * Exception UnknownTopicOrPartition is ignored and silenced, offset -1 will be returned
     *
     * @throws Common\Errors\GroupLoadInProgressException
     * @throws Common\Errors\NotCoordinatorForGroupException
     */
    public function fetchGroupOffsets(Node $coordinatorNode, $groupId, array $topicPartitions): array
    {
        $stream = $coordinatorNode->getConnection($this->configuration);

        $request = new OffsetFetchRequest(
            $groupId,
            $topicPartitions,
            $this->configuration[ConsumerConfig::CLIENT_ID]
        );
        $request->writeTo($stream);
        $response = OffsetFetchResponse::unpack($stream);

        $result = [];
        foreach ($response->topics as $topic => $partitions) {
            /** @var OffsetFetchResponsePartition[] $partitions */
            foreach ($partitions as $partitionId => $partition) {
                $isUnknownTopicPartition = $partition->errorCode === KafkaException::UNKNOWN_TOPIC_OR_PARTITION;
                if ($partition->errorCode !== 0 && !$isUnknownTopicPartition) {
                    throw KafkaException::fromCode($partition->errorCode, ['topic' => $topic, 'partitionId' => $partitionId]);
                }
                $result[$topic][$partitionId] = $partition->offset;
            }
        }

        return $result;
    }

    /**
     * Discovers the coordinator node for the consumer group (ApiKey 10, called ConsumerMetadata in Kafka 0.8.2)
     *
     * The broker answers with error code 15 (ConsumerCoordinatorNotAvailable) while the internal __consumer_offsets
     * topic is still being created, so this call is worth retrying.
     *
     * @param string $groupId Name of the group
     *
     * @return Node
     *
     * @throws Common\Errors\GroupCoordinatorNotAvailableException
     */
    public function getGroupCoordinator($groupId)
    {
        // TODO: iterate over connections and wrap logic into the try..catch block
        /** @var Node $firstNode */
        $clusterNodes = $this->cluster->nodes();
        $firstNode    = reset($clusterNodes);
        $stream       = $firstNode->getConnection($this->configuration);

        $request = new GroupCoordinatorRequest(
            $groupId,
            $this->configuration[ConsumerConfig::CLIENT_ID]
        );
        $request->writeTo($stream);
        $response = GroupCoordinatorResponse::unpack($stream);
        if ($response->errorCode !== 0) {
            throw KafkaException::fromCode($response->errorCode, ['groupId' => $groupId]);
        }

        $coordinator = $this->cluster->nodeById($response->coordinator->nodeId);

        return $coordinator;
    }

    /**
     * Fetches messages from the specified topic and partitions
     *
     * @param array   $topicPartitionOffsets List of topic partition offsets as start point for fetching
     * @param integer $timeout               Timeout in ms to wait for fetching
     *
     * @return array
     *
     * @throws Common\Errors\OffsetOutOfRangeException
     * @throws Common\Errors\UnknownTopicOrPartitionException
     * @throws Common\Errors\NotLeaderForPartitionException
     * @throws Common\Errors\ReplicaNotAvailableException
     * @throws Common\Errors\UnknownErrorException
     */
    public function fetch(array $topicPartitionOffsets, $timeout)
    {
        $timeout = min($this->configuration[ConsumerConfig::FETCH_MAX_WAIT_MS], $timeout);

        $result = $this->clusterRequest($topicPartitionOffsets, function (array $nodeTopicRequest) use ($timeout): FetchRequest {
            $request = new FetchRequest(
                $nodeTopicRequest,
                $timeout,
                $this->configuration[ConsumerConfig::FETCH_MIN_BYTES],
                $this->configuration[ConsumerConfig::MAX_PARTITION_FETCH_BYTES],
                -1,
                $this->configuration[ConsumerConfig::CLIENT_ID]
            );

            return $request;
        }, FetchResponse::class, function (array $result, FetchResponse $response): array {
            foreach ($response->topics as $topic => $partitions) {
                foreach ($partitions as $partitionId => $responsePartition) {
                    /** @var FetchResponsePartition $responsePartition */
                    if ($responsePartition->errorCode !== 0) {
                        throw KafkaException::fromCode($responsePartition->errorCode, ['topic' => $topic, 'partitionId' => $partitionId]);
                    }
                    $result[$topic][$partitionId] = $responsePartition->messageSet;
                }
            }

            return $result;
        }, $timeout);

        return $result;
    }

    /**
     * Requests all offsets for the list of topic partitions
     *
     * This query will be made over the current cluster by checking the metadata for each topic partition
     * @param array $topicPartitions List of topic partitions
     *
     * @return array Array in the form: [topic => [partition => offset]]
     *
     * @throws Common\Errors\UnknownTopicOrPartitionException
     * @throws Common\Errors\NotLeaderForPartitionException
     * @throws Common\Errors\UnknownErrorException
     */
    public function fetchTopicPartitionOffsets(array $topicPartitions)
    {
        $result = $this->clusterRequest($topicPartitions, function (array $nodeTopicRequest): OffsetsRequest {
            $request = new OffsetsRequest(
                $nodeTopicRequest,
                1,
                -1,
                $this->configuration[ConsumerConfig::CLIENT_ID]
            );

            return $request;
        }, OffsetsResponse::class, function (array $result, OffsetsResponse $response): array {
            foreach ($response->topics as $topic => $partitions) {
                /** @var OffsetsResponsePartition[] $partitions */
                foreach ($partitions as $partitionId => $partitionMetadata) {
                    if ($partitionMetadata->errorCode !== 0) {
                        throw KafkaException::fromCode($partitionMetadata->errorCode, ['topic' => $topic, 'partitionId' => $partitionId]);
                    }
                    $result[$topic][$partitionId] = reset($partitionMetadata->offsets);
                }
            }

            return $result;
        });

        return $result;
    }

    private function clusterRequest(
        array $topicPartitionsRequest,
        \Closure $nodeRequest,
        string $responseClass,
        \Closure $responseAggregator,
        $timeout = null
    ) {
        $requestByNode = [];

        foreach ($topicPartitionsRequest as $topic => $partitions) {
            foreach ($partitions as $partition => $partitionData) {
                $leaderNode = $this->cluster->leaderFor($topic, $partition);
                $requestByNode[$leaderNode->nodeId][$topic][$partition] = $partitionData;
            }
        }

        // TODO: Implement StreamGroup(Stream[] $connections) and Stream->joinStreamGroup(StreamGroup $group)
        $socketAccessor = function (SocketStream $socket) {
            if (!$socket->isConnected) {
                $socket->connect();
            }

            return $socket->streamSocket;
        };
        $socketAccessor  = $socketAccessor->bindTo(null, SocketStream::class);
        $readNodeSockets = [];

        foreach ($requestByNode as $nodeId => $nodeTopicPartitions) {
            /** @var AbstractProtocolMessage $request */
            $request = $nodeRequest($nodeTopicPartitions);
            $stream  = $this->cluster->nodeById($nodeId)->getConnection($this->configuration);

            $readNodeSockets[$nodeId] = $socketAccessor($stream);
            $request->writeTo($stream);
        }

        $incompleteReads = $readNodeSockets;
        $timeout ??= $this->configuration[ConsumerConfig::REQUEST_TIMEOUT_MS];
        $responses  = [];
        $finishTime = microtime(true) + 2 * ($timeout / 1000);
        do {
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
            $canWaitMoreTime = microtime(true) < $finishTime;
        } while ($incompleteReads !== [] && $canWaitMoreTime);

        $result = array_reduce($responses, $responseAggregator, []);

        return $result;
    }
}
