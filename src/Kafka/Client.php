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
use Protocol\Kafka\IO\PersistentSocketStream;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\ProducerConfig as ProducerConfig;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;
use Protocol\Kafka\Protocol\Request\HeartbeatRequest;
use Protocol\Kafka\Protocol\Request\HeartbeatResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;

/**
 * ApiKeys low-level client
 */
class Client
{
    /**
     * List of streams for each node
     *
     * @var Stream[]
     */
    private $connections;

    public function __construct(/**
     * Cluster configuration
     */
        private readonly Cluster $cluster, /**
     * Client configuration
     */
        private array $configuration = []
    ) {
        foreach ($this->cluster->nodes() as $node) {
            // TODO: connection settings should be used as well, make async connect
            $this->connections[$node->nodeId] = new PersistentSocketStream("tcp://{$node->host}:{$node->port}");
        }
    }

    /**
     * Produce messages to the specific topic partition
     *
     * @param string  $topic         Name of the topic
     * @param integer $partition     Identifier of partition to send messages to
     * @param array   $topicMessages List of messages for this topic
     *
     * @return ProduceResponse
     */
    public function produce($topic, $partition, array $topicMessages)
    {
        $leader = $this->cluster->leaderFor($topic, $partition);
        $stream = $this->connections[$leader->nodeId];

        $request = new ProduceRequest(
            $topicMessages,
            $this->configuration[ProducerConfig::ACKS],
            $this->configuration[ProducerConfig::TIMEOUT_MS],
            $this->configuration[ProducerConfig::CLIENT_ID]
        );
        $request->writeTo($stream);
        $response = ProduceResponse::unpack($stream);

        return $response;
    }

    public function commitOffsets(Node $coordinatorNode, $groupId, array $topicPartitionOffsets)
    {
        $stream  = $this->connections[$coordinatorNode->nodeId];
        $request = new OffsetCommitRequest(
            $groupId,
            $topicPartitionOffsets,
            $this->configuration[ConsumerConfig::CLIENT_ID]
        );
        $request->writeTo($stream);
        $response = OffsetCommitResponse::unpack($stream);

        return $response;
    }

    /**
     * @param Node  $coordinatorNode
     * @param       $groupId
     * @param array $topicPartitions
     *
     * @return array
     */
    public function fetchOffsets(Node $coordinatorNode, $groupId, array $topicPartitions): array
    {
        $stream = $this->connections[$coordinatorNode->nodeId];

        $request = new OffsetFetchRequest(
            $groupId,
            $topicPartitions,
            $this->configuration[ConsumerConfig::CLIENT_ID]
        );
        $request->writeTo($stream);
        $response = OffsetFetchResponse::unpack($stream);

        $result = [];
        foreach ($response->topics as $topic => $partitions) {
            /** @var ApiKeys\DTO\OffsetFetchPartition $partition */
            foreach ($partitions as $partitionId => $partition) {
                $result[$topic][$partitionId] = $partition->offset;
            }
        }

        return $result;
    }

    /**
     * @param Node  $coordinatorNode
     * @param       $groupId
     * @param       $memberId
     * @param       $protocolType
     * @param array $groupProtocols
     *
     * @return JoinGroupResponse
     */
    public function joinGroup(Node $coordinatorNode, $groupId, $memberId, $protocolType, array $groupProtocols)
    {
        $stream = $this->connections[$coordinatorNode->nodeId];

        $request = new JoinGroupRequest(
            $groupId,
            $this->configuration[ConsumerConfig::SESSION_TIMEOUT_MS],
            $memberId,
            $protocolType,
            $groupProtocols,
            $this->configuration[ConsumerConfig::CLIENT_ID]
        );
        $request->writeTo($stream);
        $response = JoinGroupResponse::unpack($stream);

        return $response;
    }

    /**
     * @param Node $coordinatorNode
     * @param      $groupId
     * @param      $memberId
     *
     * @return LeaveGroupResponse
     */
    public function leaveGroup(Node $coordinatorNode, $groupId, $memberId)
    {
        $stream = $this->connections[$coordinatorNode->nodeId];

        $request = new LeaveGroupRequest(
            $groupId,
            $memberId,
            $this->configuration[ConsumerConfig::CLIENT_ID]
        );
        $request->writeTo($stream);
        $response = LeaveGroupResponse::unpack($stream);

        return $response;
    }

    /**
     * @param Node  $coordinatorNode
     * @param       $memberId
     * @param       $generationId
     * @param array $groupAssignments
     *
     * @return SyncGroupResponse
     */
    public function syncGroup(Node $coordinatorNode, $groupId, $memberId, $generationId, array $groupAssignments = [])
    {
        $stream = $this->connections[$coordinatorNode->nodeId];

        $request = new SyncGroupRequest(
            $groupId,
            $generationId,
            $memberId,
            $groupAssignments,
            $this->configuration[ConsumerConfig::CLIENT_ID]
        );
        $request->writeTo($stream);
        $response = SyncGroupResponse::unpack($stream);

        return $response;
    }

    /**
     * Performs a heartbeat request for the current group
     *
     * @param Node    $coordinatorNode Current group coordinator for $groupId
     * @param string  $groupId Name of the group
     * @param string  $memberId Name of the group member
     * @param integer $generationId Current group generation
     *
     * @throws ApiKeys\Error\GroupCoordinatorNotAvailable
     * @throws ApiKeys\Error\NotCoordinatorForGroup
     * @throws ApiKeys\Error\IllegalGeneration
     * @throws ApiKeys\Error\UnknownMemberId
     * @throws ApiKeys\Error\RebalanceInProgress
     * @throws ApiKeys\Error\GroupAuthorizationFailed
     */
    public function heartbeat(Node $coordinatorNode, $groupId, $memberId, $generationId): void
    {
        $stream = $this->connections[$coordinatorNode->nodeId];

        $request = new HeartbeatRequest(
            $groupId,
            $generationId,
            $memberId,
            $this->configuration[ConsumerConfig::CLIENT_ID]
        );
        $request->writeTo($stream);
        $response = HeartbeatResponse::unpack($stream);
        if ($response->errorCode !== 0) {
            throw KafkaException::fromCode($response->errorCode);
        }
    }

    public function getGroupCoordinator($groupId)
    {
        // TODO: iterate over connections and wrap logic into the try..catch block
        $stream = reset($this->connections);

        $request = new GroupCoordinatorRequest(
            $groupId,
            $this->configuration[ConsumerConfig::CLIENT_ID]
        );
        $request->writeTo($stream);
        $reponse = GroupCoordinatorResponse::unpack($stream);
        // TODO: error handling

        return $this->cluster->nodeById($reponse->coordinator->nodeId);
    }

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
                foreach ($partitions as $partitionId => $fetchResponsePartition) {
                    /** @var ApiKeys\DTO\FetchResponsePartition $fetchResponsePartition */
                    $result[$topic][$partitionId] = $fetchResponsePartition->messageSet;
                }
            }

            return $result;
        }, $timeout);

        return $result;
    }

    public function offsets(array $topicPartitions)
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
                foreach ($partitions as $partitionId => $partitionMetadata) {
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

        // TODO: Implement StreamGroup(Stream[] $connections) and Stream->joinGroup(StreamGroup $group)
        $socketAccessor = (fn(SocketStream $socket) => $socket->streamSocket);
        $socketAccessor  = $socketAccessor->bindTo(null, SocketStream::class);
        $readNodeSockets = [];

        foreach ($requestByNode as $nodeId => $nodeTopicPartitions) {
            /** @var AbstractProtocolMessage $request */
            $request = $nodeRequest($nodeTopicPartitions);
            $stream  = $this->connections[$nodeId];

            $readNodeSockets[$nodeId] = $socketAccessor($stream);
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
                    $nodeId = array_search($resourceToRead, $readNodeSockets);
                    $responses[$nodeId] = $responseClass::unpack($this->connections[$nodeId]);
                }
                $incompleteReads = array_diff($incompleteReads, $readSelect);
            }
        }
        $result = array_reduce($responses, $responseAggregator, []);

        return $result;
    }
}
