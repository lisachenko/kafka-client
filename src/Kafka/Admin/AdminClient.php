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

use Closure;
use Exception;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\AllBrokersNotAvailableException;
use Protocol\Kafka\Common\Errors\CorrelationIdMismatchException;
use Protocol\Kafka\Common\Errors\InvalidGroupIdException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;
use Protocol\Kafka\Protocol\Data\ControlledShutdownResponsePartition;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopic;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequest;
use Protocol\Kafka\Protocol\Request\ControlledShutdownResponse;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequest;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponse;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;

/**
 * Kafka low-level administrative client
 *
 * This is the 0.9.0.1 port of the `AdminClient` of the `main` branch: the methods that a 0.9 broker can serve keep
 * their names and signatures, the ones it cannot serve are absent, and the APIs that only the older lines have a use
 * for were added next to them.
 *
 * Kafka 0.9 moved the consumer groups from ZooKeeper into the broker, so the coordinator of a group can now be asked
 * about its membership: {@see self::listGroups()} and {@see self::listAllGroups()} name the groups, and
 * {@see self::describeGroup()} reports the state, the protocol and the members of one of them. The committed offsets
 * of a group are still read with {@see self::listGroupOffsets()}.
 *
 * Kafka 0.10.0 added the api that says what a broker speaks: {@see self::getApiVersions()} (key 18) reports the
 * version range of every api of one broker, which is the only way to tell one release of the protocol from another
 * without guessing. A broker of a line below answers nothing at all for that key and closes or drops the frame.
 *
 * A topic can also be created through the protocol from Kafka 0.10.1 on (CreateTopics, key 19); until then a topic
 * was created by writing to ZooKeeper, e.g. with `kafka-topics.sh`, or implicitly by asking for its metadata while
 * `auto.create.topics.enable` is on - see {@see self::describeTopics()}.
 */
class AdminClient
{
    /**
     * Client configuration, with the defaults of {@see ClientConfig} filled in
     *
     * @var array<string, mixed>
     */
    private readonly array $configuration;

    /**
     * @param Cluster              $cluster       Cluster to administer
     * @param array<string, mixed> $configuration Client configuration
     */
    public function __construct(
        private readonly Cluster $cluster,
        array $configuration = []
    ) {
        $this->configuration = $configuration + ClientConfig::getDefaultConfiguration();
    }

    /**
     * Returns the version range of every api one broker serves, indexed by the api key (ApiKey 18)
     *
     * The method carries the name it has on the `main` branch. Every broker answers for itself, so a rolling upgrade
     * is visible here as brokers that report different ranges; ask each of them with {@see self::findAllBrokers()}.
     *
     * A 0.10.2.2 broker reports the keys 0 to 20 - the table of the "API keys" section of the protocol document -
     * and its answer is authoritative for two things the wire format does not show: ControlledShutdown (key 7) is
     * reported with `minVersion = 1`, because version 0 uses a header without a client id, and every key above 20
     * is simply absent instead of being reported with an empty range.
     *
     * @param Node $node Broker to ask
     *
     * @throws KafkaException If the broker answered the error code 35 (UnsupportedVersion), i.e. it is older than
     *                        Kafka 0.10.0 and does not serve version 0 of this api either
     *
     * @return array<int, ApiVersionsResponseMetadata> Version range of each api, indexed by the api key
     */
    public function getApiVersions(Node $node): array
    {
        /** @var ApiVersionsResponse $response */
        $response = $this->sendTo(
            $node->getConnection($this->configuration),
            fn(int $correlationId): ApiVersionsRequest => new ApiVersionsRequest($this->clientId(), $correlationId),
            ApiVersionsResponse::class,
            ['node' => $node->nodeId]
        );

        if ($response->errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($response->errorCode, ['node' => $node->nodeId]);
        }

        return $response->apiVersions;
    }

    /**
     * Returns all broker nodes of the cluster, indexed by their node id
     *
     * A broker whose metadata cache has not been filled by the controller yet answers with an EMPTY broker array;
     * that is "not ready, retry", never "the cluster has no brokers", see the "Cluster readiness" section of the
     * protocol document.
     *
     * @return array<int, Node>
     */
    public function findAllBrokers(): array
    {
        /** @var MetadataResponse $response */
        $response = $this->sendAnyNode(
            fn(int $correlationId): MetadataRequest => new MetadataRequest([], $this->clientId(), $correlationId),
            MetadataResponse::class
        );

        return $response->brokers;
    }

    /**
     * Finds the coordinator of a consumer group, i.e. the broker that holds its committed offsets
     *
     * The lookup is retried while the broker answers with the error code 15 (the `__consumer_offsets` topic is still
     * being created) or 14 (the coordinator is still loading the offsets of the group), see {@see CoordinatorLookup}.
     *
     * @param string $groupId   Name of the group
     * @param int    $timeoutMs How long to keep retrying; 0, the default, uses `metadata.fetch.timeout.ms`
     *
     * @throws \Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException If the coordinator did not become available in time
     * @throws AllBrokersNotAvailableException If not a single broker of the cluster answered
     */
    public function findCoordinator(string $groupId, int $timeoutMs = 0): Node
    {
        $lookup = new CoordinatorLookup($this->cluster, $this->configuration);

        return $lookup->findCoordinator($groupId, $timeoutMs > 0 ? $timeoutMs : null);
    }

    /**
     * Returns the names of every topic of the cluster
     *
     * @return list<string>
     */
    public function listTopics(): array
    {
        return array_keys($this->describeTopics());
    }

    /**
     * Returns the metadata of the given topics, indexed by the topic name
     *
     * CAVEAT: asking for a topic that does not exist CREATES it when the broker runs with the default
     * `auto.create.topics.enable=true`. That first answer carries the topic error code 5 (LeaderNotAvailable) and an
     * empty partition list, because the controller has not elected the leaders yet; the metadata of the fresh topic
     * arrives with one of the next requests. This is the only way a 0.8 broker creates a topic - the CreateTopics
     * api key does not exist before Kafka 0.10.1.
     *
     * @param list<string> $topics Topics to describe, an empty list asks for every topic of the cluster
     *
     * @return array<string, TopicMetadata>
     */
    public function describeTopics(array $topics = []): array
    {
        $requestedTopics = array_values($topics);
        /** @var MetadataResponse $response */
        $response = $this->sendAnyNode(
            fn(int $correlationId): MetadataRequest => new MetadataRequest(
                $requestedTopics,
                $this->clientId(),
                $correlationId
            ),
            MetadataResponse::class
        );

        return $response->topics;
    }

    /**
     * Lists the offsets of the given topic partitions
     *
     * Every request goes to the leader of its partitions, as the Offsets api is served by the leader only. With the
     * default `$time` the answer is the log end offset, i.e. the offset the next produced message will get; with
     * `OffsetsRequest::EARLIEST` it is the first offset that is still on disk. An ordinary timestamp in milliseconds
     * asks for the offsets of the log segments that were created before it, of which at most
     * `$maxNumberOfOffsets` are returned - the timestamp of a message itself is unknown to a 0.8 broker, message
     * format v1 and the timestamp-based v1 of this api only arrived with Kafka 0.10.
     *
     * @param array<string, list<int>>|iterable<TopicPartition> $topicPartitions    Partitions to list the offsets of
     * @param int                                               $time               Timestamp in ms, or one of
     *                                                                              OffsetsRequest::LATEST/EARLIEST
     * @param int                                               $maxNumberOfOffsets Offsets to return per partition
     *
     * @throws \Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException If the cluster does not host one of the partitions
     * @throws \Protocol\Kafka\Common\Errors\NotLeaderForPartitionException If the leader of a partition changed in the meantime
     *
     * @return array<string, array<int, list<int>>> Offsets as topic => partition => list of offsets, newest first
     */
    public function listOffsets(
        iterable $topicPartitions,
        int $time = OffsetsRequest::LATEST,
        int $maxNumberOfOffsets = 1
    ): array {
        $partitionTimes = [];
        foreach (self::normalizeTopicPartitions($topicPartitions) as $topic => $partitions) {
            foreach ($partitions as $partition) {
                $partitionTimes[$topic][$partition] = $time;
            }
        }

        $result = [];
        foreach ($this->groupByLeader($partitionTimes) as [$leader, $nodePartitionTimes]) {
            /** @var OffsetsResponse $response */
            $response = $this->sendTo(
                $leader->getConnection($this->configuration),
                fn(int $correlationId): OffsetsRequest => new OffsetsRequest(
                    $nodePartitionTimes,
                    $maxNumberOfOffsets,
                    -1,
                    $this->clientId(),
                    $correlationId
                ),
                OffsetsResponse::class,
                ['node' => $leader->nodeId]
            );

            foreach ($response->topics as $topic => $topicResponse) {
                /** @var OffsetsResponsePartition $partitionOffsets */
                foreach ($topicResponse->partitions as $partitionId => $partitionOffsets) {
                    if ($partitionOffsets->errorCode !== KafkaException::NO_ERROR) {
                        throw KafkaException::fromCode(
                            $partitionOffsets->errorCode,
                            ['topic' => $topic, 'partition' => $partitionId]
                        );
                    }
                    $result[$topic][$partitionId] = $partitionOffsets->offsets;
                }
            }
        }

        return $result;
    }

    /**
     * Lists the committed offsets of a consumer group for the given topic partitions
     *
     * The 0.8 OffsetFetch api has no nullable topic array - the "give me every topic of this group" request only
     * arrived with version 2 in Kafka 0.9 - so the partitions this method reads have to be named explicitly.
     *
     * The version of the request follows the `offsets.storage` option: `kafka` (the default) reads the offsets that
     * version 1 stored in the `__consumer_offsets` topic and has to be sent to the coordinator of the group, while
     * `zookeeper` reads with version 0 from ZooKeeper, which every broker of the cluster can answer.
     *
     * A topic-partition without a committed offset is not an error: version 1 answers it with the offset -1 and the
     * error code 0, version 0 with the offset -1 and the error code 3 (UnknownTopicOrPartition). Both are returned
     * as they are, any other error code is thrown.
     *
     * @param string                                            $groupId         Name of the consumer group
     * @param array<string, list<int>>|iterable<TopicPartition> $topicPartitions Partitions to read the offsets of
     *
     * @throws \Protocol\Kafka\Common\Errors\GroupLoadInProgressException If the coordinator is still loading the offsets
     * @throws \Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException If the group moved to another coordinator
     *
     * @return array<string, OffsetFetchResponseTopic> Committed offsets, indexed by the topic name
     */
    public function listGroupOffsets(string $groupId, iterable $topicPartitions): array
    {
        $partitions    = self::normalizeTopicPartitions($topicPartitions);
        $isInKafka     = $this->isOffsetStorageKafka();
        $createRequest = fn(int $correlationId): OffsetFetchRequest => $isInKafka
            ? new OffsetFetchRequest($groupId, $partitions, $this->clientId(), $correlationId)
            : new OffsetFetchRequestV0($groupId, $partitions, $this->clientId(), $correlationId);

        /** @var OffsetFetchResponse $response */
        $response = $isInKafka
            // Version 1 reads the offsets out of __consumer_offsets, which only the coordinator of the group serves
            ? $this->sendTo(
                $this->findCoordinator($groupId)->getConnection($this->configuration),
                $createRequest,
                OffsetFetchResponse::class,
                ['groupId' => $groupId]
            )
            // Version 0 reads them from ZooKeeper, which every broker of the cluster can answer
            : $this->sendAnyNode($createRequest, OffsetFetchResponse::class);

        foreach ($response->topics as $topic => $topicResponse) {
            /** @var OffsetFetchResponsePartition $partition */
            foreach ($topicResponse->partitions as $partitionId => $partition) {
                $isMissingOffset = $partition->errorCode === KafkaException::UNKNOWN_TOPIC_OR_PARTITION;
                if ($partition->errorCode !== KafkaException::NO_ERROR && !$isMissingOffset) {
                    throw KafkaException::fromCode(
                        $partition->errorCode,
                        ['groupId' => $groupId, 'topic' => $topic, 'partition' => $partitionId]
                    );
                }
            }
        }

        return $response->topics;
    }

    /**
     * Lists the consumer groups that the given broker is the coordinator of
     *
     * A broker only knows the groups it coordinates itself, so this is never the list of the whole cluster - use
     * {@see self::listAllGroups()} for that. The answer holds one entry per group with its protocol type, `consumer`
     * for the groups of a `KafkaConsumer` and of the Java consumer; an entry says nothing about the state of the
     * group, {@see self::describeGroup()} does.
     *
     * A group appears here as soon as it has a member and stays until the coordinator forgets it, which happens once
     * the last member is gone and the retention of its committed offsets has expired.
     *
     * @param Node $node Broker to ask
     *
     * @throws \Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException If the coordinator is shutting down
     * @throws \Protocol\Kafka\Common\Errors\GroupLoadInProgressException If it is still reading `__consumer_offsets`
     *
     * @return array<string, ListGroupResponseProtocol> Groups of that broker, indexed by the group id
     */
    public function listGroups(Node $node): array
    {
        /** @var ListGroupsResponse $response */
        $response = $this->sendTo(
            $node->getConnection($this->configuration),
            fn(int $correlationId): ListGroupsRequest => new ListGroupsRequest($this->clientId(), $correlationId),
            ListGroupsResponse::class,
            ['node' => $node->nodeId]
        );

        if ($response->errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($response->errorCode, ['node' => $node->nodeId]);
        }

        return $response->groups;
    }

    /**
     * Lists the consumer groups of the whole cluster
     *
     * Every broker of the cluster is asked for the groups it coordinates and the answers are merged. Unlike the
     * method of the `main` branch, which returns one entry per broker and swallows the error of a broker that did
     * not answer, this returns the single merged map that the callers of an admin client want and lets the error of
     * an unreachable broker through - a silently incomplete group list is worse than a failed call. Ask the brokers
     * one by one with {@see self::listGroups()} when a partial answer is good enough.
     *
     * @throws AllBrokersNotAvailableException If not a single broker answered the metadata request
     *
     * @return array<string, ListGroupResponseProtocol> Groups of the cluster, indexed by the group id
     */
    public function listAllGroups(): array
    {
        $groups = [];
        foreach ($this->findAllBrokers() as $node) {
            $groups += $this->listGroups($node);
        }

        return $groups;
    }

    /**
     * Describes one consumer group: its state, the protocol its members agreed on and the members themselves
     *
     * The request goes to the coordinator of the group ({@see self::findCoordinator()}), the only broker that knows
     * anything about it. The state is one of `PreparingRebalance`, `AwaitingSync`, `Stable` and `Dead`
     * (`kafka/coordinator/GroupMetadata.scala` @ 0.9.0.1); a group the coordinator has never heard of, or that has
     * lost its last member, is NOT an error - it is answered with the error code 0, the state `Dead`, an empty
     * protocol type and no members.
     *
     * @param string $groupId Name of the group
     *
     * @throws \Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException If the group moved to another coordinator
     *         between the lookup and this request
     * @throws \Protocol\Kafka\Common\Errors\GroupAuthorizationFailedException If the client may not describe the group
     * @throws InvalidGroupIdException If the coordinator answered without an entry for the group
     */
    public function describeGroup(string $groupId): DescribeGroupResponseMetadata
    {
        return $this->describeGroups([$groupId])[$groupId] ?? throw new InvalidGroupIdException(
            ['groupId' => $groupId, 'error' => "The coordinator answered with no description of the group {$groupId}"]
        );
    }

    /**
     * Describes several consumer groups at once
     *
     * Groups that share a coordinator are described with a single request; the groups of the cluster are spread over
     * the partitions of `__consumer_offsets` and therefore over its brokers, and a broker answers a group it does not
     * coordinate with the error code 16 (NotCoordinatorForGroup), so a coordinator is looked up for every group.
     *
     * @param list<string> $groupIds Names of the groups, duplicates are collapsed
     *
     * @throws \Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException If a group moved to another coordinator
     * @throws \Protocol\Kafka\Common\Errors\GroupAuthorizationFailedException If the client may not describe a group
     *
     * @return array<string, DescribeGroupResponseMetadata> Descriptions, indexed by the group id
     */
    public function describeGroups(array $groupIds): array
    {
        $coordinators  = [];
        $groupsPerNode = [];
        foreach (array_unique($groupIds) as $groupId) {
            $coordinator                           = $this->findCoordinator($groupId);
            $coordinators[$coordinator->nodeId]    = $coordinator;
            $groupsPerNode[$coordinator->nodeId][] = $groupId;
        }

        $descriptions = [];
        foreach ($groupsPerNode as $nodeId => $groups) {
            /** @var DescribeGroupsResponse $response */
            $response = $this->sendTo(
                $coordinators[$nodeId]->getConnection($this->configuration),
                fn(int $correlationId): DescribeGroupsRequest => new DescribeGroupsRequest(
                    $groups,
                    $this->clientId(),
                    $correlationId
                ),
                DescribeGroupsResponse::class,
                ['node' => $nodeId, 'groups' => $groups]
            );

            foreach ($response->groups as $groupId => $description) {
                if ($description->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode($description->errorCode, ['groupId' => $groupId]);
                }
                $descriptions[$groupId] = $description;
            }
        }

        return $descriptions;
    }

    /**
     * Asks the controller to move every leader and every replica off the given broker
     *
     * This is what `kafka-server-stop.sh` triggers through `controlled.shutdown.enable`; a client normally has no
     * reason to send it. Only the active controller serves the request - and 0.9 metadata does not tell which broker
     * that is - so it is sent to the brokers of the cluster until one of them answers. The request goes out as
     * version 1, the version Kafka 0.9 added, which is the first one whose header carries the client id;
     * {@see \Protocol\Kafka\Protocol\Request\ControlledShutdownRequestV0} sends the header-less version 0 of a
     * 0.8 broker.
     *
     * @param int $brokerId Identifier of the broker to shut down
     *
     * @throws \Protocol\Kafka\Common\Errors\BrokerNotAvailableException If the controller does not know that
     *         broker id - a 0.9.0.1 broker answers the error code 8 for it, where 0.8.2.2 answered -1, see
     *         {@see ControlledShutdownRequest}
     *
     * @return list<ControlledShutdownResponsePartition> Partitions that still live on the broker, empty when it is
     *                                                   safe to stop it
     */
    public function controlledShutdown(int $brokerId): array
    {
        /** @var ControlledShutdownResponse $response */
        $response = $this->sendAnyNode(
            fn(int $correlationId): ControlledShutdownRequest
                => new ControlledShutdownRequest($brokerId, $this->clientId(), $correlationId),
            ControlledShutdownResponse::class
        );

        if ($response->errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($response->errorCode, ['brokerId' => $brokerId]);
        }

        return $response->remainingTopicPartitions;
    }

    /**
     * Sends a request to the nodes of the cluster until one of them answers
     *
     * Every attempt builds its own request, because each of them carries its own correlation id.
     *
     * @param Closure(int): AbstractRequest  $createRequest Builds the request for a given correlation id
     * @param class-string<AbstractResponse> $responseClass Response class to unpack the answer with
     *
     * @throws AllBrokersNotAvailableException If not a single broker of the cluster answered
     */
    private function sendAnyNode(Closure $createRequest, string $responseClass): AbstractResponse
    {
        $lastException = null;
        foreach ($this->cluster->nodes() as $node) {
            try {
                return $this->sendTo(
                    $node->getConnection($this->configuration),
                    $createRequest,
                    $responseClass,
                    ['node' => $node->nodeId]
                );
            } catch (Exception $exception) {
                $lastException = $exception;
            }
        }

        throw new AllBrokersNotAvailableException(
            ['response' => $responseClass, 'error' => 'No broker of the cluster answered the request'],
            KafkaException::UNKNOWN,
            $lastException
        );
    }

    /**
     * Sends one request over the given connection and reads the answer that belongs to it
     *
     * Connections are kept open and shared between requests, so an answer is only accepted when it carries the
     * correlation id of the request; a connection that answered something else has an unknown stream position and is
     * dropped instead of being handed out again.
     *
     * @param Stream                         $stream        Connection to the broker
     * @param Closure(int): AbstractRequest  $createRequest Builds the request for a given correlation id
     * @param class-string<AbstractResponse> $responseClass Response class to unpack the answer with
     * @param array<string, mixed>           $context       Additional context for an exception
     *
     * @throws CorrelationIdMismatchException If the broker answered a different request
     */
    private function sendTo(
        Stream $stream,
        Closure $createRequest,
        string $responseClass,
        array $context = []
    ): AbstractResponse {
        $correlationId = AbstractRequest::nextCorrelationId();
        $createRequest($correlationId)->writeTo($stream);

        try {
            return ResponseValidator::read($responseClass, $stream, $correlationId, $context);
        } catch (CorrelationIdMismatchException $exception) {
            ConnectionFactory::closeStream($stream);

            throw $exception;
        }
    }

    /**
     * Groups a topic => partition => value map by the node that leads each partition
     *
     * The Offsets api is served by the leader of a partition only, so one request goes to each of them.
     *
     * @param array<string, array<int, mixed>> $topicPartitionValues
     *
     * @return list<array{0: Node, 1: array<string, array<int, mixed>>}> The leader and the partitions it leads
     */
    private function groupByLeader(array $topicPartitionValues): array
    {
        $leaders       = [];
        $requestByNode = [];
        foreach ($topicPartitionValues as $topic => $partitionValues) {
            foreach ($partitionValues as $partition => $value) {
                $leader = $this->cluster->leaderFor($topic, $partition);

                $leaders[$leader->nodeId]                           = $leader;
                $requestByNode[$leader->nodeId][$topic][$partition] = $value;
            }
        }

        return array_map(
            static fn(int $nodeId): array => [$leaders[$nodeId], $requestByNode[$nodeId]],
            array_keys($requestByNode)
        );
    }

    /**
     * Normalizes the accepted partition notations into a topic => list of partitions map
     *
     * @param array<string, list<int>>|iterable<TopicPartition> $topicPartitions
     *
     * @return array<string, list<int>>
     */
    private static function normalizeTopicPartitions(iterable $topicPartitions): array
    {
        $result = [];
        foreach ($topicPartitions as $topic => $partitions) {
            if ($partitions instanceof TopicPartition) {
                $result[$partitions->topic][] = $partitions->partition;

                continue;
            }
            foreach ($partitions as $partition) {
                $result[(string) $topic][] = (int) $partition;
            }
        }

        return $result;
    }

    /**
     * Returns the configured client id
     */
    private function clientId(): string
    {
        return (string) ($this->configuration[ClientConfig::CLIENT_ID] ?? '');
    }

    /**
     * Checks whether the offsets of a group are stored in Kafka (OffsetCommit/OffsetFetch v1) or in ZooKeeper (v0)
     */
    private function isOffsetStorageKafka(): bool
    {
        $storage = $this->configuration[ClientConfig::OFFSETS_STORAGE] ?? ClientConfig::OFFSETS_STORAGE_KAFKA;

        return $storage === ClientConfig::OFFSETS_STORAGE_KAFKA;
    }
}
