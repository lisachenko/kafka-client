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
 * @date 13.09.2017
 */

namespace Protocol\Kafka\Admin;

use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidGroupIdException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\RequestTimedOutException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
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
     *
     * @param string $groupId Identifier of group
     *
     * @return DescribeGroupResponseMetadata
     */
    public function describeGroup($groupId)
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
     * Performs an API Versions request
     *
     * @param Node $node Node for querying versions
     *
     * @return ApiVersionsResponseMetadata[]
     */
    public function getApiVersions(Node $node)
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
     * @return array|Node[]
     */
    public function findAllBrokers()
    {
        $request  = new MetadataRequest();
        /** @var MetadataResponse $response */
        $response = $this->sendAnyNode($request, MetadataResponse::class);

        return $response->brokers;
    }


    /**
     * @return array|
     * @return mixed[]
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
     * @return null|Node
     * @throws RequestTimedOutException
     */
    public function findCoordinator($groupId, $timeoutMs = 0)
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
            ], $e ?? null);
        }

        return $this->cluster->nodeById($response->coordinator->nodeId);
    }

    /**
     * Lists group available on node
     *
     * @param Node $node
     *
     * @return array
     * @throws KafkaException
     */
    public function listGroups(Node $node)
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
     * Sends request to any existing node
     *
     * @param AbstractRequest $request       Instance of request to send
     * @param string          $responseClass Response class name to unpack
     *
     * @return AbstractProtocolMessage
     * @throws \RuntimeException
     */
    private function sendAnyNode(AbstractRequest $request, string $responseClass)
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
