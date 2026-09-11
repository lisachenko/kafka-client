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
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\AllBrokersNotAvailableException;
use Protocol\Kafka\Common\Errors\BrokerNotAvailableException;
use Protocol\Kafka\Common\Errors\CorrelationIdMismatchException;
use Protocol\Kafka\Common\Errors\InvalidGroupIdException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NotControllerException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Protocol\Data\AlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;
use Protocol\Kafka\Protocol\Data\ControlledShutdownResponsePartition;
use Protocol\Kafka\Protocol\Data\DescribeConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Data\LeaveGroupRequestMember;
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopic;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\AlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\AlterConfigsResponse;
use Protocol\Kafka\Protocol\Request\AlterReplicaLogDirsRequest;
use Protocol\Kafka\Protocol\Request\AlterReplicaLogDirsResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequest;
use Protocol\Kafka\Protocol\Request\ControlledShutdownResponse;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenResponse;
use Protocol\Kafka\Protocol\Request\DeleteGroupsRequest;
use Protocol\Kafka\Protocol\Request\DeleteGroupsResponse;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequest;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponse;
use Protocol\Kafka\Protocol\Request\DescribeDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\DescribeDelegationTokenResponse;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequest;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponse;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsRequest;
use Protocol\Kafka\Protocol\Request\DescribeLogDirsResponse;
use Protocol\Kafka\Protocol\Request\ExpireDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\ExpireDelegationTokenResponse;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV0;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\RenewDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\RenewDelegationTokenResponse;

/**
 * Kafka low-level administrative client
 *
 * This is the 0.10.2.2 port of the `AdminClient` of the `main` branch: the methods a 0.10 broker can serve keep
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
 * `auto.create.topics.enable` is on. That accident is over on this line: every metadata request of this class is a
 * version 4 one with `allow_auto_topic_creation = false`, so an admin never creates a topic by describing it - see
 * {@see self::describeTopics()}.
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
     * A 2.8.2 broker reports the **56** keys 0 to 51, 56, 57, 60 and 61 - the table of the "API keys" section of
     * the protocol document, which is the `zkBroker` listener set of the JSON message specifications. Its answer is
     * authoritative for what the wire format does not show: a key the broker does not serve is simply absent
     * instead of being reported with an empty range, and the KRaft apis of the controller listener (52-55, 58, 59,
     * 62-64) never appear on a ZooKeeper-backed broker at all.
     *
     * The request goes out as version 2 ({@see ApiVersionsRequest}), the KIP-219 bump of Kafka 2.0, so the answer
     * carries the trailing `throttleTimeMs` of KIP-124; only the whole {@see Client::apiVersions()} response
     * exposes it, this method returns the api table alone.
     *
     * @param Node $node Broker to ask
     *
     * @throws KafkaException If the broker answered the error code 35 (UnsupportedVersion), i.e. it is older than
     *                        Kafka 2.0 and does not serve version 2 of this api
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
     * The request names an EMPTY topic list, which version 1 of the Metadata API (Kafka 0.10.0) made a request for
     * NO topic at all instead of one for every topic: the answer carries the brokers of the cluster and nothing
     * else, which is exactly what this method needs. Every broker also reports its `broker.rack` from that version
     * on, so {@see Node::$rack} is filled here whenever the cluster is rack aware.
     *
     * It is sent as version 4 with `allow_auto_topic_creation = false`, like every metadata request of this class;
     * an empty topic list names no topic that could be created anyway.
     *
     * @return array<int, Node>
     */
    public function findAllBrokers(): array
    {
        /** @var MetadataResponse $response */
        $response = $this->sendAnyNode(
            fn(int $correlationId): MetadataRequest => new MetadataRequest(
                [],
                false,
                $this->clientId(),
                $correlationId
            ),
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

        return $lookup->findCoordinator(
            $groupId,
            GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP,
            $timeoutMs > 0 ? $timeoutMs : null
        );
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
     * **Version 4 of the Metadata api (Kafka 0.11) ended the caveat this method used to carry.** Until then, asking
     * for a topic that does not exist CREATED it whenever the broker ran with the default
     * `auto.create.topics.enable=true`, and that first answer carried the topic error code 5 (LeaderNotAvailable)
     * with an empty partition list. This client now sends `allow_auto_topic_creation = false` from the whole
     * administrative side - an admin must not bring a topic into being by looking at it - so a topic the cluster
     * does not have is answered with the error code **3** (UnknownTopicOrPartition) and stays non-existent. Use
     * {@see self::createTopics()} to create one.
     *
     * An empty list asks for every topic of the cluster, the internal ones included: it is sent as the NULL topic
     * array of Metadata v1, because an empty array means "no topic at all" from that version on. Which of the
     * answered topics Kafka keeps for itself is in {@see TopicMetadata::$isInternal}.
     *
     * The request goes out as **Metadata v5** (Kafka 1.0, KIP-112/113), so every partition of the answer reports
     * its {@see \Protocol\Kafka\Common\PartitionMetadata::$offlineReplicas} next to its replicas and its
     * in-sync replicas: the replicas whose broker is down or whose log directory has failed. On a one-broker
     * cluster that array is always empty.
     *
     * @param list<string> $topics Topics to describe, an empty list asks for every topic of the cluster
     *
     * @return array<string, TopicMetadata>
     */
    public function describeTopics(array $topics = []): array
    {
        $requestedTopics = $topics !== [] ? array_values($topics) : null;
        /** @var MetadataResponse $response */
        $response = $this->sendAnyNode(
            fn(int $correlationId): MetadataRequest => new MetadataRequest(
                $requestedTopics,
                false,
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
     * asks version 1 of the api (Kafka 0.10.1) for the offset of the first message whose own timestamp is at or
     * after it, which the time index of the log resolves.
     *
     * Version 1 answers exactly one offset per partition, so the `$maxNumberOfOffsets` of the version 0 api is gone;
     * a caller that wants the segment offsets of that older version builds an
     * {@see \Protocol\Kafka\Protocol\Request\OffsetsRequestV0} itself. A timestamp that no message of a partition
     * matches - and every timestamp on an empty partition - is not an error either: such a partition is answered
     * with the offset {@see OffsetsResponsePartition::UNKNOWN_OFFSET}, i.e. -1.
     *
     * @param array<string, list<int>>|iterable<TopicPartition> $topicPartitions Partitions to list the offsets of
     * @param int                                               $time            Timestamp in ms, or one of
     *                                                                           OffsetsRequest::LATEST/EARLIEST
     *
     * @throws \Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException If the cluster does not host one of the partitions
     * @throws \Protocol\Kafka\Common\Errors\NotLeaderForPartitionException If the leader of a partition changed in the meantime
     * @throws \Protocol\Kafka\Common\Errors\UnsupportedForMessageFormatException If a timestamp was searched for in a
     *         topic whose `message.format.version` is older than 0.10.0
     *
     * @return array<string, array<int, int>> Offsets as topic => partition => offset
     */
    public function listOffsets(iterable $topicPartitions, int $time = OffsetsRequest::LATEST): array
    {
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
                    OffsetsRequest::CONSUMER_REPLICA_ID,
                    // Deliberately not the `isolation.level` of a consumer: an administrator asks what is in the
                    // log, not what a `read_committed` reader may see, so this stays at the high watermark even
                    // while a transaction is open. A consumer gets the last stable offset through
                    // KafkaConsumer::endOffsets(), which reads ConsumerConfig::ISOLATION_LEVEL.
                    FetchRequest::READ_UNCOMMITTED,
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
                    $result[$topic][$partitionId] = $partitionOffsets->offset;
                }
            }
        }

        return $result;
    }

    /**
     * Lists the committed offsets of a consumer group, of the given topic partitions or of every topic it committed
     *
     * `$topicPartitions` of **null** - the default, and the shape this method has on the `main` branch - asks the
     * coordinator for every topic-partition the group has a committed offset for. That is what the nullable topic
     * array of the version 2 of the api (Kafka 0.10.2, KIP-88) made possible; an **empty** iterable is a different
     * request that names no topic at all and comes back empty.
     *
     * The version of the request follows the `offsets.storage` option: `kafka` (the default) reads the offsets that
     * version 4 stored in the `__consumer_offsets` topic and has to be sent to the coordinator of the group, while
     * `zookeeper` reads with version 0 from ZooKeeper, which every broker of the cluster can answer - and which has
     * no nullable topic array, so it refuses a null with an
     * {@see \Protocol\Kafka\Common\Errors\UnsupportedVersionException}.
     *
     * A topic-partition without a committed offset is not an error: version 4 answers it with the offset -1 and the
     * error code 0, version 0 with the offset -1 and the error code 3 (UnknownTopicOrPartition). Both are returned
     * as they are, any other error code is thrown - including the group-level error code that version 2 appends
     * after the topics, which reports that this broker is not the coordinator of the group (16), that it is still
     * loading its offsets (14) or that the group may not be read (30).
     *
     * @param string                                                 $groupId         Name of the consumer group
     * @param array<string, list<int>>|iterable<TopicPartition>|null $topicPartitions Partitions to read the offsets
     *        of, null for every topic-partition of the group
     *
     * @throws \Protocol\Kafka\Common\Errors\GroupLoadInProgressException If the coordinator is still loading the offsets
     * @throws \Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException If the group moved to another coordinator
     *
     * @return array<string, OffsetFetchResponseTopic> Committed offsets, indexed by the topic name
     */
    public function listGroupOffsets(string $groupId, ?iterable $topicPartitions = null): array
    {
        $partitions    = $topicPartitions === null ? null : self::normalizeTopicPartitions($topicPartitions);
        $isInKafka     = $this->isOffsetStorageKafka();
        $createRequest = fn(int $correlationId): OffsetFetchRequest => $isInKafka
            ? new OffsetFetchRequest($groupId, $partitions, $this->clientId(), $correlationId)
            : new OffsetFetchRequestV0($groupId, $partitions, $this->clientId(), $correlationId);

        /** @var OffsetFetchResponse $response */
        $response = $isInKafka
            // Version 2 reads the offsets out of __consumer_offsets, which only the coordinator of the group serves
            ? $this->sendTo(
                $this->findCoordinator($groupId)->getConnection($this->configuration),
                $createRequest,
                OffsetFetchResponse::class,
                ['groupId' => $groupId]
            )
            // Version 0 reads them from ZooKeeper, which every broker of the cluster can answer
            : $this->sendAnyNode($createRequest, OffsetFetchResponseV0::class);

        if ($response->errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($response->errorCode, ['groupId' => $groupId]);
        }

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
     * anything about it. The state is one of `PreparingRebalance`, `CompletingRebalance`, `Stable`, `Empty` and
     * `Dead` (`kafka/coordinator/group/GroupMetadata.scala` @ 1.1.1), i.e. one of the `STATE_*` constants of
     * {@see DescribeGroupResponseMetadata}; a group the coordinator has never heard of, or that has lost its last
     * member and outlived its committed offsets, is NOT an error - it is answered with the error code 0, the state
     * `Dead`, an empty protocol type and no members.
     *
     * Kafka 1.0 renamed the state between the last JoinGroup and the leader's SyncGroup from `AwaitingSync` to
     * **`CompletingRebalance`**; a broker of this line answers the new name, and
     * {@see DescribeGroupResponseMetadata::STATE_AWAITING_SYNC} is kept only for the lines below.
     *
     * @param string $groupId Name of the group
     * @param bool   $includeAuthorizedOperations Whether the answer also reports the operations this client may
     *        perform on the group (KIP-430, version 3), see
     *        {@see DescribeGroupResponseMetadata::$authorizedOperations}
     *
     * @throws \Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException If the group moved to another coordinator
     *         between the lookup and this request
     * @throws \Protocol\Kafka\Common\Errors\GroupAuthorizationFailedException If the client may not describe the group
     * @throws InvalidGroupIdException If the coordinator answered without an entry for the group
     */
    public function describeGroup(
        string $groupId,
        bool $includeAuthorizedOperations = false
    ): DescribeGroupResponseMetadata {
        return $this->describeGroups([$groupId], $includeAuthorizedOperations)[$groupId] ?? throw new InvalidGroupIdException(
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
     * With `$includeAuthorizedOperations` the version 3 request of KIP-430 (Kafka 2.3) also asks which operations
     * this very client may perform on each group, which the answer reports as the bit set
     * {@see DescribeGroupResponseMetadata::$authorizedOperations}; without it that field stays at
     * {@see DescribeGroupResponseMetadata::OPERATIONS_NOT_REQUESTED}.
     *
     * @param list<string> $groupIds Names of the groups, duplicates are collapsed
     * @param bool         $includeAuthorizedOperations Whether the answer reports the authorized operations of every
     *        group (KIP-430, version 3)
     *
     * @throws \Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException If a group moved to another coordinator
     * @throws \Protocol\Kafka\Common\Errors\GroupAuthorizationFailedException If the client may not describe a group
     *
     * @return array<string, DescribeGroupResponseMetadata> Descriptions, indexed by the group id
     */
    public function describeGroups(array $groupIds, bool $includeAuthorizedOperations = false): array
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
                    $correlationId,
                    $includeAuthorizedOperations
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
     *         broker id - a 0.10.2.2 broker answers the error code 8 for it, where 0.8.2.2 answered -1, see
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

    /**
     * Low-level client of this cluster, built by {@see self::client()} when a topic api is used for the first time
     */
    private ?Client $kafkaClient = null;

    /**
     * Creates the given topics on the cluster (ApiKey 19, Kafka 0.10.1)
     *
     * Kafka 0.10.1 is the first release in which a client can create a topic without writing to ZooKeeper itself;
     * before it, the only way through the protocol was to ask a broker with `auto.create.topics.enable` for the
     * metadata of a topic that does not exist yet, which gives every topic the defaults of the broker.
     * {@see self::describeTopics()} does not do that any more: it asks with `allow_auto_topic_creation = false`.
     *
     * The request is sent to the active controller ({@see self::findController()}), the only broker that serves it,
     * and it is repeated ONCE against a freshly looked up controller when the answer says 41 (NotController) -
     * which is what a client sees when the controller moved between the lookup and the request.
     *
     * Every requested topic gets an entry in the result, in the order of `$newTopics`: `null` when the topic was
     * created (or validated, with `$validateOnly`), otherwise the exception of its error code - 36 TopicExists,
     * 37 InvalidPartitions, 38 InvalidReplicationFactor, 39 InvalidReplicaAssignment, 40 InvalidConfig, 42
     * InvalidRequest, 44 PolicyViolation - whose context carries the `error_message` the controller sent with it.
     * Nothing is thrown for a topic that could not be created: one failing topic of a request does not say anything
     * about the others, and a caller that wants an exception raises the one of the topic it cares about.
     *
     * CAVEAT: `$timeoutMs` is the time the CONTROLLER waits for the topic to exist before it answers, so a value of
     * 0 answers immediately, with the error code 7 (RequestTimedOut) for every topic - their creation has been
     * scheduled and finishes shortly afterwards. With the default of 30 seconds a successful answer means that the
     * topic exists on the controller; the other brokers learn about it with the next metadata update, so a Metadata
     * request may still answer 5 (LeaderNotAvailable) for a moment.
     *
     * @param list<NewTopic> $newTopics    Topics to create
     * @param int            $timeoutMs    How long the controller waits for the topics to be created
     * @param bool           $validateOnly Validate the request without creating anything (CreateTopics v1)
     *
     * @throws AllBrokersNotAvailableException If no broker of the cluster answered
     * @throws NotControllerException If no broker of the cluster is the active controller
     *
     * @return array<string, KafkaException|null> Error of every requested topic, null when it was created
     */
    public function createTopics(array $newTopics, int $timeoutMs = 30000, bool $validateOnly = false): array
    {
        return $this->onController(
            fn(Node $controller): array => $this->client()
                ->createTopics($controller, $newTopics, $timeoutMs, $validateOnly)
        );
    }

    /**
     * Deletes the given topics from the cluster (ApiKey 20, Kafka 0.10.1)
     *
     * The request is sent to the active controller and repeated once on 41 (NotController), exactly like
     * {@see self::createTopics()}. Every requested topic gets an entry in the result, in the order of `$topics`:
     * `null` when it was deleted, the exception of the error code otherwise - 3 UnknownTopicOrPartition for a topic
     * the cluster does not have, 29 TopicAuthorizationFailed when the client may not delete it.
     *
     * CAVEAT: deleting a topic is ASYNCHRONOUS. The controller writes the topic into `/admin/delete_topics` in
     * ZooKeeper and then removes its partitions from the brokers; `$timeoutMs` is how long it waits for that before
     * it answers, and with a timeout of 0 every topic comes back with the error code 7 (RequestTimedOut) although
     * its deletion is under way. Even a successful answer only means that the controller is done with it - the
     * topic disappears from the metadata of the other brokers a moment later, so a caller that waits for it should
     * poll {@see self::listTopics()}. A cluster whose brokers run with `delete.topic.enable=false` - the default of
     * Kafka 0.10 - accepts the request and never carries the deletion out.
     *
     * @param list<string> $topics    Names of the topics to delete
     * @param int          $timeoutMs How long the controller waits for the topics to be deleted
     *
     * @throws AllBrokersNotAvailableException If no broker of the cluster answered
     * @throws NotControllerException If no broker of the cluster is the active controller
     *
     * @return array<string, KafkaException|null> Error of every requested topic, null when it was deleted
     */
    public function deleteTopics(array $topics, int $timeoutMs = 30000): array
    {
        return $this->onController(
            fn(Node $controller): array => $this->client()->deleteTopics($controller, $topics, $timeoutMs)
        );
    }

    /**
     * Returns the broker that is the active controller of the cluster
     *
     * The topic administration apis are served by the active controller alone, and version 1 of the Metadata api
     * (Kafka 0.10.0) is what says which broker that is: every answer from that version on carries the
     * `ControllerId` of the metadata cache of the broker that answered
     * ({@see \Protocol\Kafka\Protocol\Request\MetadataResponse::$controllerId}), so one metadata refresh finds it -
     * on a 0.9 cluster the same lookup needed one probe request per broker.
     *
     * The metadata of the cluster is asked again ONCE when the id is missing, because a `-1`
     * ({@see \Protocol\Kafka\Protocol\Request\MetadataResponse::NO_CONTROLLER_ID}) is what a broker answers while
     * the cluster is electing a controller, and because the metadata this client holds may be older than the last
     * election. A broker whose id is not among the alive brokers of the same answer counts as "no controller" too:
     * that is what a client sees in the moment the controller goes down.
     *
     * @throws AllBrokersNotAvailableException If not a single broker of the cluster answered
     * @throws NotControllerException If the cluster has no active controller, i.e. it is electing one right now
     */
    public function findController(): Node
    {
        $controller = $this->cluster->controller();
        if ($controller === null) {
            // The cached metadata may predate the last controller election, so the cluster is asked once more
            $this->cluster->reload();
            $controller = $this->cluster->controller();
        }

        if ($controller === null) {
            throw new NotControllerException(
                [
                    'error' => 'The cluster does not have an active controller',
                    'nodes' => array_keys($this->cluster->nodes()),
                ]
            );
        }

        return $controller;
    }

    /**
     * Runs a request against the active controller and repeats it once if the answer says 41 (NotController)
     *
     * A controller election between the lookup and the request is the one failure of the topic administration apis
     * that a client can fix by itself, and one more lookup is enough for it: the answer of the second attempt is
     * reported as it is, whatever it says.
     *
     * The 41 is also the proof that the `ControllerId` this client holds is STALE - the broker it names says it is
     * not the controller any more - so the metadata is fetched again before the second lookup; without that the
     * second attempt would go to the very same broker and get the very same answer.
     *
     * @param Closure(Node): array<string, KafkaException|null> $request Sends the request to the given controller
     *
     * @return array<string, KafkaException|null>
     */
    private function onController(Closure $request): array
    {
        $result = $request($this->findController());
        foreach ($result as $error) {
            if ($error instanceof NotControllerException) {
                $this->cluster->reload();

                return $request($this->findController());
            }
        }

        return $result;
    }

    /**
     * Returns a low-level client for the cluster this instance administers
     */
    private function client(): Client
    {
        return $this->kafkaClient ??= new Client($this->cluster, $this->configuration);
    }

    /**
     * Deletes the records before an offset of each of the given partitions (ApiKey 21, Kafka 0.11, KIP-107)
     *
     * Until Kafka 0.11 the only way to get rid of records was to wait for the retention of the topic - by time or
     * by size - or to delete the topic itself. KIP-107 added the api that moves the **low watermark**
     * (`logStartOffset`) of a partition forward: everything BELOW the offset becomes unreadable at once, the log
     * cleaner removes the segments that are then completely below it, and the answer reports the new low watermark
     * of every partition. That watermark is what an Offsets request with
     * {@see \Protocol\Kafka\Protocol\Request\OffsetsRequest::EARLIEST} answers afterwards, and what a Fetch v5
     * carries as `log_start_offset`; a consumer that fetches below it is answered with 1 (OffsetOutOfRange).
     *
     * The offset of a partition is either a plain integer - the offset of the first record that has to survive - or
     * a {@see RecordsToDelete}; {@see RecordsToDelete::allRecords()} deletes everything up to the high watermark of
     * the partition. Deleting **at or below** the current low watermark is not an error and changes nothing.
     *
     * The api is served by the LEADER of each partition, so the request is split per leader and a partial failure
     * is reported as a {@see \Protocol\Kafka\Common\Errors\TopicPartitionRequestException} that carries the
     * partitions that did succeed.
     *
     * @param array<string, array<int, int|RecordsToDelete>> $topicPartitionOffsets Offset to delete before, as
     *        topic => partition => offset
     * @param int $timeoutMs How long the leader waits for the new low watermark to be replicated, in milliseconds
     *
     * @throws \Protocol\Kafka\Common\Errors\TopicPartitionRequestException If a partition could not be served -
     *         3 (UnknownTopicOrPartition) for a topic the broker does not know, 1 (OffsetOutOfRange) for an offset
     *         above the high watermark of the partition, 42 (InvalidRequest) for a negative offset other than -1
     *
     * @return array<string, array<int, DeletedRecords>> New low watermark as topic => partition => result
     */
    public function deleteRecords(array $topicPartitionOffsets, int $timeoutMs = 30000): array
    {
        $answered = $this->client()->deleteRecords($topicPartitionOffsets, $timeoutMs);

        $result = [];
        foreach ($answered as $topic => $partitions) {
            foreach ($partitions as $partitionId => $partitionResult) {
                $result[$topic][$partitionId] = DeletedRecords::fromResponsePartition($partitionResult);
            }
        }

        return $result;
    }

    /**
     * Reads the configuration of the given topics and brokers (ApiKey 32, Kafka 0.11, KIP-133)
     *
     * KIP-133 made what `kafka-configs.sh --describe` had to read out of ZooKeeper available through the protocol.
     * A resource is a topic or a broker ({@see ConfigResource::topic()} / {@see ConfigResource::broker()}), and the
     * result is indexed by {@see ConfigResource::key()}, because PHP cannot use an object as an array key.
     *
     * The two resource types are not asked of the same broker, which is why this method splits the request:
     *
     *  - a **topic** resource is answered by any broker, because `AdminManager.describeConfigs` @ 0.11.0.3 reads
     *    the entity config of the topic from ZooKeeper and merges it with the log defaults of the broker;
     *  - a **broker** resource is answered by THAT broker alone - it is its live `KafkaConfig` - and every other
     *    broker refuses the resource with the error code 42 and the message `Unexpected broker id, expected 0, but
     *    received 1`. This method therefore sends a broker resource to the node whose id it names, and a broker id
     *    that no node of the cluster has to any broker, so that the answer of the broker says what is wrong.
     *
     * `$configNames` filters the options of every resource of the call; `null`, the default, asks for all of them.
     * The value of a **sensitive** option is never sent by the broker and arrives as `null`.
     *
     * The request goes out as **version 2**, the version Kafka 2.0 added (KIP-219); its frame is the version 1 of
     * KIP-226 (Kafka 1.1) byte for byte, so every entry of the answer carries the {@see ConfigSource} its value
     * comes from instead of the bare `is_default` boolean of version 0, and `$includeSynonyms` asks the broker to
     * list every place it looked for that value ({@see ConfigEntry::$synonyms}). Without the flag the synonym list
     * of every entry is empty and nothing else changes. Two consequences of the version raise a caller of the 0.11
     * line should know about:
     *
     *  - `isDefault` now means "nobody configured it anywhere", not "the resource did not configure it": an option
     *    whose broker-level synonym stands in the `server.properties` is reported with the source
     *    `STATIC_BROKER_CONFIG`. {@see Config::ownValues()} is the set of options the resource itself carries;
     *  - a broker entry is only read-only when it is **not** dynamically updatable
     *    (`DynamicBrokerConfig.AllDynamicConfigs`), where a 0.11 broker reported every option of a broker resource
     *    as read-only.
     *
     * @param list<ConfigResource> $resources       Resources to describe
     * @param list<string>|null    $configNames     Options to read of every resource, null for all of them
     * @param bool                 $includeSynonyms Ask for the synonyms of every option (version 1, KIP-226)
     *
     * @throws KafkaException If the broker refused one of the resources - 42 (InvalidRequest) for an unknown
     *         resource type or a broker id that is not the one that answers, 17 (InvalidTopic) for an illegal topic
     *         name, **3 (UnknownTopicOrPartition) for a topic a 2.8.2 broker does not know**, where a 1.1.1 broker
     *         answered the log defaults of the broker with the error code 0; the message the broker sent is in the
     *         context of the exception
     * @throws AllBrokersNotAvailableException If no broker of the cluster answered
     *
     * @return array<string, Config> Configuration of every requested resource, indexed by its resource key
     */
    public function describeConfigs(
        array $resources,
        ?array $configNames = null,
        bool $includeSynonyms = false
    ): array {
        $result = [];
        foreach ($this->groupByConfigNode($resources) as [$nodeId, $nodeResources]) {
            $entries       = array_map(
                static fn(ConfigResource $resource): DescribeConfigsRequestResource
                    => DescribeConfigsRequestResource::fromConfigResource($resource, $configNames),
                $nodeResources
            );
            $createRequest = fn(int $correlationId): DescribeConfigsRequest => new DescribeConfigsRequest(
                $entries,
                $includeSynonyms,
                $this->clientId(),
                $correlationId
            );

            $node = $nodeId === null ? null : $this->nodeById($nodeId);

            /** @var DescribeConfigsResponse $response */
            $response = $node === null
                ? $this->sendAnyNode($createRequest, DescribeConfigsResponse::class)
                : $this->sendTo(
                    $node->getConnection($this->configuration),
                    $createRequest,
                    DescribeConfigsResponse::class,
                    ['node' => $nodeId]
                );

            foreach ($response->resources as $resourceResult) {
                $config = Config::fromResponseResource($resourceResult);
                if ($resourceResult->errorCode !== KafkaException::NO_ERROR) {
                    throw KafkaException::fromCode(
                        $resourceResult->errorCode,
                        ['resource' => $config->resource->key(), 'error' => $resourceResult->errorMessage]
                    );
                }
                $result[$config->resource->key()] = $config;
            }
        }

        return $result;
    }

    /**
     * Replaces the configuration of the given resources (ApiKey 33, Kafka 0.11, KIP-133)
     *
     * The argument maps a {@see ConfigResource::key()} - `topic:events` - to the WHOLE configuration the resource
     * should have afterwards: `AdminManager.alterConfigs` @ 0.11.0.3 builds a fresh `Properties` from the entries
     * and hands it to `AdminUtils.changeTopicConfig`, which REPLACES the ZooKeeper node of the topic. An option
     * that was set before and is not in the request is therefore reset to its default, which is what
     * {@see Config::nonDefaultValues()} exists for:
     *
     * <code>
     *   $key     = ConfigResource::topic('events')->key();
     *   $current = $admin->describeConfigs([ConfigResource::topic('events')])[$key]->ownValues();
     *   $admin->alterConfigs([$key => ['retention.ms' => '3600000'] + $current]);
     * </code>
     *
     * **A 1.1 broker alters a broker resource as well** (KIP-226), where a 0.11 broker refused every one of them
     * with the error code 42 and the message `AlterConfigs is only supported for topics, but resource type is
     * BROKER`. The wire format did not change for it; what changed is the broker:
     *
     *  - the resource `broker:<id>` is the live configuration of THAT broker and is only served by it, exactly like
     *    a DescribeConfigs of the same resource, so such a call has to be sent to the named node - this method
     *    sends every resource of one call to any broker, which means that a broker resource has to be altered in a
     *    call of its own against an {@see AdminClient} whose cluster the node answers, or through the default
     *    resource below;
     *  - the resource `broker:` - the **empty** name - is the cluster-wide default of KIP-226: the value is stored
     *    in ZooKeeper under `/config/brokers/<default>`, every broker of the cluster picks it up, and a
     *    DescribeConfigs reports it with the source `DYNAMIC_DEFAULT_BROKER_CONFIG`;
     *  - only the options of `DynamicBrokerConfig.AllDynamicConfigs` can be changed at runtime. Everything else is
     *    answered with 42 and `Cannot update these configs dynamically: Set(…)`, which names the offending options,
     *    and the whole resource is refused - the entries are validated together.
     *
     * Any broker of the cluster serves the request, there is no controller involved.
     *
     * Every requested resource gets an entry in the result, keyed like the argument: `null` when its configuration
     * was replaced (or validated, with `$validateOnly`), the exception of its error code otherwise. Nothing is
     * thrown for a resource that was refused, exactly like {@see self::createTopics()}.
     *
     * @param array<string, array<string, string|null>> $configs      Complete configuration of every resource, as
     *        resource key => option name => value
     * @param bool                                      $validateOnly Validate the request without changing anything
     *
     * @throws AllBrokersNotAvailableException If no broker of the cluster answered
     *
     * @return array<string, KafkaException|null> Error of every requested resource, null when it was altered
     */
    public function alterConfigs(array $configs, bool $validateOnly = false): array
    {
        $resources = [];
        foreach ($configs as $resourceKey => $entries) {
            $resources[] = AlterConfigsRequestResource::fromConfigResource(
                ConfigResource::fromKey((string) $resourceKey),
                $entries
            );
        }

        /** @var AlterConfigsResponse $response */
        $response = $this->sendAnyNode(
            fn(int $correlationId): AlterConfigsRequest => new AlterConfigsRequest(
                $resources,
                $validateOnly,
                $this->clientId(),
                $correlationId
            ),
            AlterConfigsResponse::class
        );

        $answered = [];
        foreach ($response->resources as $resourceResult) {
            $key            = ConfigResource::fromWire($resourceResult->resourceType, $resourceResult->resourceName)
                ->key();
            $answered[$key] = self::configResourceError($key, $resourceResult->errorCode, $resourceResult->errorMessage);
        }

        $result = [];
        foreach (array_keys($configs) as $resourceKey) {
            // A resource that was altered is answered with `null`, so the map has to be probed with
            // array_key_exists() and not with `??`, which would turn every success into an unknown error
            $result[$resourceKey] = array_key_exists($resourceKey, $answered)
                ? $answered[$resourceKey]
                : new UnknownErrorException(
                    ['resource' => $resourceKey, 'error' => 'The broker sent no result for this resource']
                );
        }

        return $result;
    }

    /**
     * Groups the resources of a DescribeConfigs call by the broker that has to answer them
     *
     * A broker resource is answered by the broker it names and by no other one, so it gets a group of its own; every
     * topic resource goes into the group `null`, which any broker of the cluster can serve. A broker resource whose
     * name is not a number is left in that group as well, so that the broker itself answers it with the 42 and the
     * message `Broker id must be an integer, but it is: …` instead of this client guessing an id.
     *
     * @param list<ConfigResource> $resources
     *
     * @return list<array{0: int|null, 1: list<ConfigResource>}> The node id (null for any broker) and its resources
     */
    private function groupByConfigNode(array $resources): array
    {
        $grouped = [];
        foreach ($resources as $resource) {
            $isNamedBroker      = $resource->type === ConfigResource::TYPE_BROKER
                && preg_match('/^-?\d+$/', $resource->name) === 1;
            $nodeId             = $isNamedBroker ? (int) $resource->name : null;
            $grouped[$nodeId ?? 'any'][] = $resource;
        }

        return array_map(
            static fn(int|string $nodeId): array => [$nodeId === 'any' ? null : (int) $nodeId, $grouped[$nodeId]],
            array_keys($grouped)
        );
    }

    /**
     * Returns the broker of the given node id, asking the cluster for its metadata once when it is not known yet
     *
     * A node id that no broker of the cluster has comes back as `null`, and the request is then sent to any broker:
     * the answer of the broker - 42 with `Unexpected broker id, expected 0, but received 7` - says exactly what is
     * wrong, which is more than this client could say about an id it has never seen.
     */
    private function nodeById(int $nodeId): ?Node
    {
        $nodes = $this->cluster->nodes();
        if (!isset($nodes[$nodeId])) {
            $this->cluster->reload();
            $nodes = $this->cluster->nodes();
        }

        return $nodes[$nodeId] ?? null;
    }

    /**
     * Turns the error code of one resource of an AlterConfigs answer into the exception of the caller
     */
    private static function configResourceError(
        string $resourceKey,
        int $errorCode,
        ?string $errorMessage
    ): ?KafkaException {
        if ($errorCode === KafkaException::NO_ERROR) {
            return null;
        }

        $context = ['resource' => $resourceKey];
        if ($errorMessage !== null) {
            $context['error'] = $errorMessage;
        }

        return KafkaException::fromCode($errorCode, $context);
    }

    /**
     * Raises the number of partitions of existing topics (ApiKey 37, Kafka 1.0, KIP-195)
     *
     * The last piece of `kafka-topics.sh --alter` that needed ZooKeeper before Kafka 1.0. Every entry of
     * `$newPartitions` maps a topic name to the number of partitions it should have **afterwards** - as a
     * {@see NewPartitions}, or as a plain integer for `NewPartitions::increaseTo($count)`:
     *
     * <code>
     *   $admin->createPartitions(['events' => 5, 'audit' => NewPartitions::increaseTo(2, [[0]])]);
     * </code>
     *
     * The api can only ever GROW a topic: a count that is not above the current one is answered with the error code
     * 37 (InvalidPartitions) and the message `Topic already has 3 partitions.`, because Kafka cannot merge two logs
     * and the keys of a compacted topic would change their partition. The optional assignment names the brokers of
     * every partition that is ADDED, in order, and has to have as many entries as partitions are added and as many
     * brokers per entry as the replication factor of the topic - anything else is 39 (InvalidReplicaAssignment).
     *
     * The request is sent to the active controller ({@see self::findController()}), the only broker that serves it,
     * and is repeated ONCE against a freshly looked up controller when the answer says 41 (NotController), exactly
     * like {@see self::createTopics()}. Every requested topic gets an entry in the result: `null` when its partition
     * count was raised (or validated, with `$validateOnly`), the exception of its error code otherwise - nothing is
     * thrown for a topic that was refused.
     *
     * CAVEAT: `$timeoutMs` is the time the CONTROLLER waits for the new partitions to exist before it answers, as in
     * `createTopics()`. A successful answer means the controller is done; the other brokers learn about the new
     * partitions with their next metadata update, so a Metadata request may answer 5 (LeaderNotAvailable) for them
     * for a moment.
     *
     * @param array<string, NewPartitions|int> $newPartitions Topics to grow, as topic name => new total count
     * @param int                              $timeoutMs     How long the controller waits for the new partitions
     * @param bool                             $validateOnly  Validate the request without adding anything
     *
     * @throws AllBrokersNotAvailableException If no broker of the cluster answered
     * @throws NotControllerException If no broker of the cluster is the active controller
     *
     * @return array<string, KafkaException|null> Error of every requested topic, null when it was grown
     */
    public function createPartitions(
        array $newPartitions,
        int $timeoutMs = 30000,
        bool $validateOnly = false
    ): array {
        return $this->onController(
            fn(Node $controller): array => $this->client()
                ->createPartitions($controller, $newPartitions, $timeoutMs, $validateOnly)
        );
    }

    /**
     * Makes the coordinator forget consumer groups and their committed offsets (ApiKey 42, Kafka 1.1, KIP-229)
     *
     * The counterpart of `kafka-consumer-groups.sh --delete`, which had to write to ZooKeeper before Kafka 1.1.
     * `GroupCoordinator.handleDeleteGroups` @ 1.1.1 removes the group from its cache and writes a tombstone for
     * every offset the group committed, so the group disappears from {@see self::listGroups()} and an OffsetFetch of
     * it answers -1 for every partition afterwards.
     *
     * **A group is only deletable when it has no member left.** An `Empty` group (every member left or timed out)
     * and a `Dead` one are deleted; a group with a live member is answered with 68 (NonEmptyGroup) and keeps its
     * offsets, and a group the coordinator has never heard of with 69 (GroupIdNotFound) - which is the one way to
     * tell "there was nothing to delete" from "it is still in use".
     *
     * Groups that share a coordinator are deleted with a single request, and a coordinator is looked up for every
     * group ({@see self::findCoordinator()}), because a broker answers a group it does not coordinate with 16
     * (NotCoordinatorForGroup). Every requested group gets an entry in the result, keyed by the group id: `null`
     * when it was deleted, the exception of its error code otherwise. Nothing is thrown for a group that was
     * refused - one group of a call says nothing about the others - and an empty list is answered with an empty
     * result without a single request.
     *
     * @param list<string> $groupIds Names of the groups to delete, duplicates are collapsed
     *
     * @throws \Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException If a coordinator could not be
     *         looked up at all
     *
     * @return array<string, KafkaException|null> Error of every requested group, null when it was deleted
     */
    public function deleteConsumerGroups(array $groupIds): array
    {
        $coordinators  = [];
        $groupsPerNode = [];
        foreach (array_unique($groupIds) as $groupId) {
            $coordinator                           = $this->findCoordinator($groupId);
            $coordinators[$coordinator->nodeId]    = $coordinator;
            $groupsPerNode[$coordinator->nodeId][] = $groupId;
        }

        $answered = [];
        foreach ($groupsPerNode as $nodeId => $groups) {
            /** @var DeleteGroupsResponse $response */
            $response = $this->sendTo(
                $coordinators[$nodeId]->getConnection($this->configuration),
                fn(int $correlationId): DeleteGroupsRequest => new DeleteGroupsRequest(
                    $groups,
                    $this->clientId(),
                    $correlationId
                ),
                DeleteGroupsResponse::class,
                ['node' => $nodeId, 'groups' => $groups]
            );

            foreach ($groups as $groupId) {
                $result             = $response->groups[$groupId] ?? null;
                $answered[$groupId] = $result === null
                    ? new UnknownErrorException(
                        ['groupId' => $groupId, 'error' => 'The coordinator sent no result for this group']
                    )
                    : self::groupError($groupId, $result->errorCode);
            }
        }

        $result = [];
        foreach ($groupIds as $groupId) {
            // A group that was deleted is answered with `null`, so the map has to be probed with array_key_exists()
            // and not with `??`, which would turn every success into an unknown error
            $result[$groupId] = array_key_exists($groupId, $answered)
                ? $answered[$groupId]
                : new UnknownErrorException(
                    ['groupId' => $groupId, 'error' => 'The coordinator sent no result for this group']
                );
        }

        return $result;
    }

    /**
     * Removes members from a consumer group without waiting for their session timeouts (ApiKey 13 v3, KIP-345)
     *
     * The administrative half of static membership: a **static** member does not leave its group when it shuts
     * down - that is what keeps its partitions across a restart - so an instance that is retired for good has to
     * be removed by hand, or the group waits a whole `session.timeout.ms` for it and then rebalances anyway.
     * `kafka-consumer-groups.sh --group G --delete-offsets`-style tooling and the Java
     * `Admin.removeMembersFromConsumerGroup()` send exactly this request, the **batch** LeaveGroup of version 3.
     *
     * A member is named by its `group.instance.id` ({@see MemberToRemove::byInstanceId()}), by its member id
     * ({@see MemberToRemove::byMemberId()}) or by both; a plain string in `$members` is an **instance id**, as in
     * the Java client, whose `MemberToRemove` takes nothing else. Every member of the batch gets an entry in the
     * result, keyed by {@see MemberToRemove::identity()} - `null` when it was removed, the exception of its error
     * code otherwise - and nothing is thrown for a member that was refused, because one member of a batch says
     * nothing about the others. 25 (`UnknownMemberId`) is what an instance id the group does not have is answered
     * with, and 82 (`FencedInstanceId`) a member id that lost its instance to another consumer.
     *
     * An **empty batch** is legal: it is sent, and the coordinator answers it with an empty member array, which is
     * how a caller can probe the group without removing anything.
     *
     * @param string                          $groupId Name of the group
     * @param iterable<MemberToRemove|string> $members Members to remove; a plain string is a `group.instance.id`
     *
     * @throws \Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException If the group moved to another coordinator
     *         between the lookup and this request
     * @throws \Protocol\Kafka\Common\Errors\GroupAuthorizationFailedException If the client may not read the group
     * @throws \Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException If the group is gone (`Dead`)
     *
     * @return array<string, KafkaException|null> Error of every requested member, null when it was removed
     */
    public function removeMembersFromConsumerGroup(string $groupId, iterable $members): array
    {
        $toRemove = [];
        foreach ($members as $member) {
            $toRemove[] = $member instanceof MemberToRemove ? $member : MemberToRemove::byInstanceId($member);
        }

        $coordinator = $this->findCoordinator($groupId);
        /** @var LeaveGroupResponse $response */
        $response = $this->sendTo(
            $coordinator->getConnection($this->configuration),
            fn(int $correlationId): LeaveGroupRequest => new LeaveGroupRequest(
                $groupId,
                array_map(static fn(MemberToRemove $member): LeaveGroupRequestMember => $member->toRequestMember(), $toRemove),
                $this->clientId(),
                $correlationId
            ),
            LeaveGroupResponse::class,
            ['groupId' => $groupId, 'members' => count($toRemove)]
        );

        // The top-level code is the one of the request as a whole; what happened to each member is in its entry
        if ($response->errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($response->errorCode, ['groupId' => $groupId]);
        }

        $result = [];
        foreach ($toRemove as $index => $member) {
            $answer = $response->members[$index] ?? null;
            $result[$member->identity()] = $answer === null
                ? new UnknownErrorException(
                    ['groupId' => $groupId, 'error' => 'The coordinator sent no result for this member']
                )
                : self::memberError($groupId, $member, $answer->errorCode);
        }

        return $result;
    }

    /**
     * Turns the error code of one member of a LeaveGroup v3 answer into the exception of the caller
     */
    private static function memberError(string $groupId, MemberToRemove $member, int $errorCode): ?KafkaException
    {
        return $errorCode === KafkaException::NO_ERROR
            ? null
            : KafkaException::fromCode($errorCode, [
                'groupId'         => $groupId,
                'memberId'        => $member->memberId,
                'groupInstanceId' => $member->groupInstanceId,
            ]);
    }

    /**
     * Turns the error code of one group of a DeleteGroups answer into the exception of the caller
     */
    private static function groupError(string $groupId, int $errorCode): ?KafkaException
    {
        return $errorCode === KafkaException::NO_ERROR
            ? null
            : KafkaException::fromCode($errorCode, ['groupId' => $groupId]);
    }

    /**
     * Reports what each log directory of the given brokers holds (ApiKey 35, Kafka 1.0, KIP-113)
     *
     * A broker of Kafka 1.x may have more than one data directory (`log.dirs`), and every replica lives in exactly
     * one of them. This api says which - and it is **broker-local**: `KafkaApis.handleDescribeLogDirsRequest` @
     * 1.1.1 asks the local `ReplicaManager`, so each broker only knows about its own disks and one request goes to
     * each of them. The result is therefore indexed by the broker id first and by the absolute path of the
     * directory second.
     *
     * `$topicPartitions` selects the replicas to report:
     *
     *  - `null`, the default, asks for **every** replica of every directory, which is what the Java admin client
     *    and `kafka-log-dirs.sh --describe` send. The answer of a busy broker is large - hundreds of kilobytes for
     *    a few thousand partitions - so name the partitions when they are known;
     *  - an **empty array** asks for no replica at all and answers the directories alone, which is the cheapest way
     *    to ask which disks a broker has and whether they are online;
     *  - a `topic => list of partition ids` map, or an iterable of {@see TopicPartition}, asks for those replicas.
     *
     * A replica that this broker does not have is not an error and simply produces no entry, and a directory that
     * is offline is reported with the error 56 (KafkaStorageError) in {@see LogDirInfo::$error} and no replica.
     *
     * **A 2.8.2 broker names every topic of a directory in the answer, whatever the request asked for.**
     * `ReplicaManager.describeLogDirs` @ 2.8.2 groups all logs of a directory by topic and applies the filter of
     * the request to the *partitions* inside those groups alone, so the answer of a request for one partition
     * carries one {@see LogDirInfo} per topic of the broker - the ones that were not asked for with an empty
     * replica list. A 1.1.1 broker answered the requested topics alone. The map is handed on as the broker sent
     * it: a caller reads the replicas it asked for out of it by name and may not assume that it holds nothing
     * else. The request goes out as **version 1**, the version Kafka 2.0 added (KIP-219), whose frame is the
     * version 0 of KIP-113.
     *
     * @param list<int>                                              $brokerIds       Brokers to ask, by node id
     * @param array<string, list<int>>|iterable<TopicPartition>|null $topicPartitions Replicas to report, null for
     *        every replica of every log directory
     *
     * @throws BrokerNotAvailableException If a requested broker id is not a node of the cluster
     *
     * @return array<int, array<string, LogDirInfo>> Directories of every asked broker, as broker id => path => info
     */
    public function describeLogDirs(array $brokerIds, ?array $topicPartitions = null): array
    {
        $topics = $topicPartitions === null ? null : self::normalizeTopicPartitions($topicPartitions);

        $result = [];
        foreach ($brokerIds as $brokerId) {
            $brokerId = (int) $brokerId;

            /** @var DescribeLogDirsResponse $response */
            $response = $this->sendToBroker(
                $brokerId,
                fn(int $correlationId): DescribeLogDirsRequest => new DescribeLogDirsRequest(
                    $topics,
                    $this->clientId(),
                    $correlationId
                ),
                DescribeLogDirsResponse::class
            );

            $directories = [];
            foreach ($response->logDirs as $logDir) {
                $directories[$logDir->logDir] = LogDirInfo::fromResponseLogDir($logDir);
            }
            $result[$brokerId] = $directories;
        }

        return $result;
    }

    /**
     * Moves the given replicas to another log directory of the broker that holds them (ApiKey 34, Kafka 1.0, KIP-113)
     *
     * The argument maps a {@see TopicPartitionReplica::key()} - `events-0-1` - to the **absolute** path of the
     * directory the replica should live in, and this method groups it by the broker each replica names, because the
     * api is broker-local like {@see self::describeLogDirs()}:
     *
     * <code>
     *   $admin->alterReplicaLogDirs([
     *       TopicPartitionReplica::of('events', 0, 1)->key() => '/mnt/disk-2/kafka-logs',
     *   ]);
     * </code>
     *
     * **The answer only says that the move was accepted.** The broker creates the future log and starts the
     * `ReplicaAlterLogDirsThread` while it handles the request, then answers 0; the copy runs in the background and
     * the replica is reported by {@see self::describeLogDirs()} in *both* directories - as the current log of the
     * source and as the log with {@see ReplicaInfo::$isFuture} of the destination - until the mover swaps it in.
     * Naming the directory the replica already sits in is not an error either: the broker answers 0 and creates
     * nothing.
     *
     * Every requested replica gets an entry in the result, keyed like the argument: `null` when the move was
     * accepted, the exception of its error code otherwise - 57 (LogDirNotFound) for a path that is not one of the
     * directories of `log.dirs` or that is relative, 9 (ReplicaNotAvailable) for a partition the broker does not
     * host, 56 (KafkaStorageError) for a directory that is offline. Nothing is thrown for a replica that was
     * refused, exactly like {@see self::alterConfigs()}.
     *
     * @param array<string, string> $replicaAssignment Destination of every replica, as replica key => absolute path
     *
     * @throws BrokerNotAvailableException If a replica names a broker id that is not a node of the cluster
     *
     * @return array<string, KafkaException|null> Error of every requested replica, null when the move was accepted
     */
    public function alterReplicaLogDirs(array $replicaAssignment): array
    {
        $requestByBroker = [];
        foreach ($replicaAssignment as $replicaKey => $logDir) {
            $replica = TopicPartitionReplica::fromKey((string) $replicaKey);

            $requestByBroker[$replica->brokerId][$logDir][$replica->topic][] = $replica->partition;
        }

        $answered = [];
        foreach ($requestByBroker as $brokerId => $logDirs) {
            /** @var AlterReplicaLogDirsResponse $response */
            $response = $this->sendToBroker(
                $brokerId,
                fn(int $correlationId): AlterReplicaLogDirsRequest => new AlterReplicaLogDirsRequest(
                    $logDirs,
                    $this->clientId(),
                    $correlationId
                ),
                AlterReplicaLogDirsResponse::class
            );

            foreach ($response->topics as $topicResult) {
                foreach ($topicResult->partitions as $partitionResult) {
                    $key = TopicPartitionReplica::of(
                        $topicResult->topic,
                        $partitionResult->partition,
                        $brokerId
                    )->key();

                    $answered[$key] = $partitionResult->errorCode === KafkaException::NO_ERROR
                        ? null
                        : KafkaException::fromCode($partitionResult->errorCode, ['replica' => $key]);
                }
            }
        }

        $result = [];
        foreach (array_keys($replicaAssignment) as $replicaKey) {
            $replicaKey = (string) $replicaKey;
            // A replica that was accepted is answered with `null`, so the map has to be probed with
            // array_key_exists() and not with `??`, which would turn every success into an unknown error
            $result[$replicaKey] = array_key_exists($replicaKey, $answered)
                ? $answered[$replicaKey]
                : new UnknownErrorException(
                    ['replica' => $replicaKey, 'error' => 'The broker sent no result for this replica']
                );
        }

        return $result;
    }

    /**
     * Sends one request to the broker of the given node id, which is the only one that can answer it
     *
     * The two JBOD apis of KIP-113 are about the disks of one broker, so there is no fallback to another node the
     * way {@see self::sendAnyNode()} has one: a broker id that the cluster does not have is a mistake of the
     * caller, and asking a different broker would answer with *its* directories instead.
     *
     * @param Closure(int): AbstractRequest  $createRequest Builds the request for a given correlation id
     * @param class-string<AbstractResponse> $responseClass Response class to unpack the answer with
     *
     * @throws BrokerNotAvailableException If the node id is not a broker of the cluster
     */
    private function sendToBroker(int $brokerId, Closure $createRequest, string $responseClass): AbstractResponse
    {
        $node = $this->nodeById($brokerId);
        if ($node === null) {
            throw new BrokerNotAvailableException(
                ['node' => $brokerId, 'error' => 'The cluster has no broker with this node id']
            );
        }

        return $this->sendTo(
            $node->getConnection($this->configuration),
            $createRequest,
            $responseClass,
            ['node' => $brokerId]
        );
    }

    /**
     * Issues a delegation token for the principal of this client (ApiKey 38, Kafka 1.1, KIP-48)
     *
     * KIP-48 gave a cluster a second kind of credential: a short-lived token that a client can hand to a worker
     * process instead of the credential it authenticated with. The **owner** of the token is not in the request -
     * it is the principal of the connection - which is why the api only works on a channel that authenticated
     * somebody: a PLAINTEXT connection, a one-way SSL one and a connection that itself authenticated with a token
     * are all answered with the error code 64 (`UnsupportedByAuthenticationException`) before the body is read.
     * The connection of this client is the one that {@see \Protocol\Kafka\Common\ClientConfig::SECURITY_PROTOCOL}
     * and the SASL options of its configuration describe.
     *
     * `$renewers` are the principals that may renew or expire the token besides its owner, as
     * {@see KafkaPrincipal} objects or as the `<type>:<name>` strings every Kafka tool prints. Only the type
     * {@see KafkaPrincipal::USER_TYPE} is accepted, anything else is the error code 67.
     *
     * `$maxLifeTimeMs` is a **period**, not a timestamp: the broker caps it at its own
     * `delegation.token.max.lifetime.ms` (7 days by default), and the default -1 asks for exactly that maximum.
     * The `expiryTimestamp` of the answer is the earlier of that maximum and
     * `now + delegation.token.expiry.time.ms` (24 hours by default), i.e. the moment the token has to be renewed
     * by ({@see self::renewDelegationToken()}).
     *
     * **A token this client issues cannot be used by this client.** Authenticating *with* a token is SASL/SCRAM
     * with the token id as the user name and the base64 hmac as the password, and this package speaks `PLAIN`
     * alone - see {@see DelegationToken}.
     *
     * @param list<KafkaPrincipal|string> $renewers      Principals that may renew the token besides its owner
     * @param int                         $maxLifeTimeMs Maximum lifetime in milliseconds, -1 for the maximum of
     *        the broker
     *
     * @throws KafkaException If the broker refused the request - 61 when it has no `delegation.token.master.key`,
     *         64 when the connection authenticated nobody, 67 for a renewer that is not a `User`
     * @throws AllBrokersNotAvailableException If no broker of the cluster answered
     */
    public function createDelegationToken(
        array $renewers = [],
        int $maxLifeTimeMs = CreateDelegationTokenRequest::DEFAULT_MAX_LIFE_TIME
    ): DelegationToken {
        // The answer does not repeat the renewers of the request, so the list of the caller is the only place the
        // information of the issued token can take them from - as the Scala `AdminClient.createToken` does as well
        $principals = KafkaPrincipal::listOf($renewers);

        /** @var CreateDelegationTokenResponse $response */
        $response = $this->sendAnyNode(
            fn(int $correlationId): CreateDelegationTokenRequest => new CreateDelegationTokenRequest(
                $principals,
                $maxLifeTimeMs,
                $this->clientId(),
                $correlationId
            ),
            CreateDelegationTokenResponse::class
        );

        if ($response->errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($response->errorCode, ['owner' => (string) $response->owner]);
        }

        return DelegationToken::fromCreateResponse($response, $principals);
    }

    /**
     * Moves the expiry of a delegation token forward (ApiKey 39, Kafka 1.1, KIP-48)
     *
     * A token is named by the raw bytes of its **hmac** - {@see DelegationToken::$hmac}, of which
     * {@see DelegationToken::hmacAsBase64String()} is the form the Kafka tools print - and never by its id.
     *
     * `$renewTimePeriodMs` is a period counted from *now*: the new expiry is `min(maxTimestamp, now + period)`, so
     * a renewal can never move the expiry past the maximum lifetime the token was issued with, and -1 asks for the
     * `delegation.token.expiry.time.ms` of the broker. Only the owner of the token and the principals its renewers
     * name may renew it, everybody else is answered with 63.
     *
     * @param string $hmac              Raw bytes of the HMAC of the token
     * @param int    $renewTimePeriodMs Milliseconds from now that the token should stay valid for
     *
     * @throws KafkaException If the broker refused the request - 62 for an hmac no token of the cluster has, 63
     *         for a principal that may not renew it, 66 for a token that is already past its expiry, 64 on a
     *         connection that authenticated nobody
     * @throws AllBrokersNotAvailableException If no broker of the cluster answered
     *
     * @return int Milliseconds since the epoch at which the token now expires
     */
    public function renewDelegationToken(
        string $hmac,
        int $renewTimePeriodMs = RenewDelegationTokenRequest::DEFAULT_RENEW_TIME_PERIOD
    ): int {
        /** @var RenewDelegationTokenResponse $response */
        $response = $this->sendAnyNode(
            fn(int $correlationId): RenewDelegationTokenRequest => new RenewDelegationTokenRequest(
                $hmac,
                $renewTimePeriodMs,
                $this->clientId(),
                $correlationId
            ),
            RenewDelegationTokenResponse::class
        );

        if ($response->errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($response->errorCode, ['renewTimePeriodMs' => $renewTimePeriodMs]);
        }

        return $response->expiryTimestamp;
    }

    /**
     * Shortens the life of a delegation token, or ends it now (ApiKey 40, Kafka 1.1, KIP-48)
     *
     * The api is the mirror image of {@see self::renewDelegationToken()} and the same principals may call it, but
     * the sign of the period decides what happens: a **negative** one
     * ({@see ExpireDelegationTokenRequest::EXPIRE_IMMEDIATELY}, the default) deletes the token from ZooKeeper and
     * from the token cache of every broker at once and answers with the clock of the broker, a non-negative one
     * sets the expiry to `min(maxTimestamp, now + period)` and leaves the token in place.
     *
     * A token that was deleted is answered with **62** (`DelegationTokenNotFoundException`) afterwards, not with
     * the 66 of a token that is merely past its expiry.
     *
     * @param string $hmac               Raw bytes of the HMAC of the token
     * @param int    $expiryTimePeriodMs Milliseconds from now the token should still live, negative to delete it
     *
     * @throws KafkaException If the broker refused the request - 62, 63, 64 and 66 as for the renew api
     * @throws AllBrokersNotAvailableException If no broker of the cluster answered
     *
     * @return int Milliseconds since the epoch at which the token expires, or expired
     */
    public function expireDelegationToken(
        string $hmac,
        int $expiryTimePeriodMs = ExpireDelegationTokenRequest::EXPIRE_IMMEDIATELY
    ): int {
        /** @var ExpireDelegationTokenResponse $response */
        $response = $this->sendAnyNode(
            fn(int $correlationId): ExpireDelegationTokenRequest => new ExpireDelegationTokenRequest(
                $hmac,
                $expiryTimePeriodMs,
                $this->clientId(),
                $correlationId
            ),
            ExpireDelegationTokenResponse::class
        );

        if ($response->errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($response->errorCode, ['expiryTimePeriodMs' => $expiryTimePeriodMs]);
        }

        return $response->expiryTimestamp;
    }

    /**
     * Lists the delegation tokens this client may see (ApiKey 41, Kafka 1.1, KIP-48)
     *
     * The argument is a nullable array and its three shapes are three different questions:
     *
     *  * `null`, the default, asks for every token the caller may see;
     *  * a non-empty list of owners asks for the tokens one of those principals owns or may renew;
     *  * an empty array asks for nothing and is answered with an empty result.
     *
     * On a cluster without an authorizer - which is what the container of this repository is - "may see" is
     * exactly "owns or may renew", so a client sees its own tokens and the ones it was named a renewer of. The
     * described entries carry the hmac as well, so a token that is visible can also be renewed and expired.
     *
     * @param list<KafkaPrincipal|string>|null $owners Owners to ask for, null for every visible token
     *
     * @throws KafkaException If the broker refused the request - 61 when it has no `delegation.token.master.key`,
     *         64 on a connection that authenticated nobody
     * @throws AllBrokersNotAvailableException If no broker of the cluster answered
     *
     * @return array<string, DelegationToken> Every visible token, indexed by its token id
     */
    public function describeDelegationToken(?array $owners = null): array
    {
        /** @var DescribeDelegationTokenResponse $response */
        $response = $this->sendAnyNode(
            fn(int $correlationId): DescribeDelegationTokenRequest => new DescribeDelegationTokenRequest(
                $owners,
                $this->clientId(),
                $correlationId
            ),
            DescribeDelegationTokenResponse::class
        );

        if ($response->errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($response->errorCode, ['owners' => $owners === null ? 'all' : count($owners)]);
        }

        $tokens = [];
        foreach ($response->tokenDetails as $tokenId => $token) {
            $tokens[$tokenId] = DelegationToken::fromResponseToken($token);
        }

        return $tokens;
    }
}
