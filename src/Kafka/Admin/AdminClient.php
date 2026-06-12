<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Admin;

use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidGroupIdException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\RequestTimedOutException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopic;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequest;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponse;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;

/**
 * Kafka low-level administrative client
 */
class AdminClient
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
     * Describes group of consumers by name
     */
    public function describeGroup(string $groupId): DescribeGroupResponseMetadata
    {
        $coordinator = $this->findCoordinator($groupId);
        $request     = new DescribeGroupsRequest([$groupId], $this->configuration[ClientConfig::CLIENT_ID]);
        $stream      = $coordinator->getConnection($this->configuration);
        $request->writeTo($stream);
        $response = DescribeGroupsResponse::unpack($stream);
        $metadata = $response->groups[$groupId] ?? null;

        if ($metadata === null) {
            throw new InvalidGroupIdException([
                'error' => "Response from broker contained no metadata for group {$groupId}",
            ]);
        }

        return $metadata;
    }

    /**
     * Performs an API Versions request on given cluster node
     */
    public function getApiVersions(Node $node): array
    {
        $stream  = $node->getConnection($this->configuration);
        $request = new ApiVersionsRequest($this->configuration[ClientConfig::CLIENT_ID]);
        $request->writeTo($stream);
        $response = ApiVersionsResponse::unpack($stream);
        if ($response->errorCode !== 0) {
            $context = ['node' => $node];
            throw KafkaException::fromCode($response->errorCode, $context);
        }

        return $response->apiVersions;
    }

    /**
     * Returns all broker nodes
     *
     * @return Node[]
     */
    public function findAllBrokers(): array
    {
        $request  = new MetadataRequest();
        /** @var MetadataResponse $response */
        $response = $this->sendAnyNode($request, MetadataResponse::class);

        return $response->brokers;
    }


    /**
     * Lists all available groups
     *
     * @return array|ListGroupResponseProtocol[]
     */
    public function listAllGroups(): array
    {
        $result = [];
        foreach ($this->findAllBrokers() as $brokerNode) {
            try {
                $groups = $this->listGroups($brokerNode);
            } catch (\Exception) {
                trigger_error("Failed to find groups from broker {$brokerNode->nodeId}", E_USER_NOTICE);
                $groups = [];
            } finally {
                $result[$brokerNode->nodeId] = $groups;
            }
        }

        return $result;
    }

    /**
     * Finds a coordinator for the group
     *
     * @param string $groupId   Name of the group
     * @param int    $timeoutMs Timeout for looking coordinator
     *
     * @throws RequestTimedOutException If command was timed out
     *
     * @return Node
     */
    public function findCoordinator(string $groupId, int $timeoutMs = 0): Node
    {
        $request = new GroupCoordinatorRequest($groupId, $this->configuration[ClientConfig::CLIENT_ID]);

        $startTime = microtime(true);
        do {
            try {
                /** @var GroupCoordinatorResponse $response */
                $response = $this->sendAnyNode($request, GroupCoordinatorResponse::class);
            } catch (\Exception) {
                $response = null;
            }
            $isNegativeResponse = $response === null
                || $response->errorCode === KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE;

            if ($isNegativeResponse) {
                usleep($this->configuration[ClientConfig::RETRY_BACKOFF_MS]);
            }
            $canWaitMoreTime = microtime(true) - $startTime < $timeoutMs;
        } while ($isNegativeResponse && $canWaitMoreTime);

        if ($isNegativeResponse) {
            throw new RequestTimedOutException([
                'error' => 'The consumer group command timed out while waiting for group to initialize',
            ], $internalException ?? null);
        }

        $node = $this->cluster->nodeById($response->coordinator->nodeId);
        if ($node === null) {
            throw new NotCoordinatorForGroupException([
                'error' => "No coordinator for the group {$groupId}",
            ]);
        }

        return $node;
    }

    /**
     * Lists group available on node
     *
     * @param Node $node
     *
     * @throws KafkaException
     *
     * @return ListGroupResponseProtocol[]
     */
    public function listGroups(Node $node): array
    {
        $stream  = $node->getConnection($this->configuration);
        $request = new ListGroupsRequest($this->configuration[ClientConfig::CLIENT_ID]);
        $request->writeTo($stream);
        $response = ListGroupsResponse::unpack($stream);
        if ($response->errorCode !== 0) {
            $context = ['node' => $node];
            throw KafkaException::fromCode($response->errorCode, $context);
        }

        return $response->groups;
    }

    /**
     * List all group topic partition offsets for specified groupID
     *
     * @param string $groupId Identifier of group
     *
     * @return OffsetFetchResponseTopic[]
     */
    public function listGroupOffsets(string $groupId): array
    {
        $coordinator = $this->findCoordinator($groupId);
        $request     = new OffsetFetchRequest($groupId, null, $this->configuration[ClientConfig::CLIENT_ID]);
        $stream      = $coordinator->getConnection($this->configuration);
        $request->writeTo($stream);
        $response = OffsetFetchResponse::unpack($stream);
        if ($response->errorCode !== 0) {
            $context = ['groupId' => $groupId];
            throw KafkaException::fromCode($response->errorCode, $context);
        }

        return $response->topics;
    }

    /**
     * Sends request to any existing node
     *
     * @param AbstractRequest $request       Instance of request to send
     * @param string          $responseClass Response class name to unpack
     *
     * @return AbstractProtocolMessage
     * @throws \RuntimeException
     */
    private function sendAnyNode(AbstractRequest $request, string $responseClass): AbstractProtocolMessage
    {
        foreach ($this->cluster->nodes() as $node) {
            try {
                $stream = $node->getConnection($this->configuration);
                $request->writeTo($stream);

                return $responseClass::unpack($stream);
            } catch (\Exception $e) {
                trigger_error($e->getMessage(), E_USER_NOTICE);
            }
        }
        $requestClass = $request::class;
        throw new \RuntimeException("Request {$requestClass} failed on brokers");
    }
}
