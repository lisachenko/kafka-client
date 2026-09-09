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

namespace Protocol\Kafka\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\NodeV0;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Common\TopicMetadataV0;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequestV0;
use Protocol\Kafka\Protocol\Request\MetadataRequestV1;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\MetadataResponseV0;
use Protocol\Kafka\Protocol\Request\MetadataResponseV1;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the Metadata API v0, v1 and v2 against a real Kafka 0.10.2.2 broker.
 *
 * @see docs/protocol/0.10.2.md, section "Metadata API (key 3, v0, v1 and v2)"
 */
#[CoversClass(MetadataRequest::class)]
#[CoversClass(MetadataRequestV0::class)]
#[CoversClass(MetadataRequestV1::class)]
#[CoversClass(MetadataResponse::class)]
#[CoversClass(MetadataResponseV0::class)]
#[CoversClass(MetadataResponseV1::class)]
#[CoversClass(Node::class)]
#[CoversClass(NodeV0::class)]
#[CoversClass(TopicMetadata::class)]
#[CoversClass(TopicMetadataV0::class)]
#[CoversClass(PartitionMetadata::class)]
#[CoversClass(Cluster::class)]
final class MetadataApiTest extends IntegrationTestCase
{
    /**
     * Client id sent along with every request of this test class
     */
    private const string CLIENT_ID = 'kafka-client-t3-metadata';

    /**
     * The only topic a 0.10.2.2 broker flags as internal, `Topic.isInternal` @ 0.10.2.2
     */
    private const string INTERNAL_TOPIC = '__consumer_offsets';

    /**
     * How long to wait for the controller to elect the leaders of a fresh topic, in seconds
     */
    private const float LEADER_ELECTION_TIMEOUT = 30.0;

    public function testAutoCreatedTopicIsFirstAnnouncedWithoutALeader(): void
    {
        $topic  = self::uniqueTopicName('t3-metadata');
        $stream = $this->connect();

        new MetadataRequest([$topic], self::CLIENT_ID, 1)->writeTo($stream);
        $response = MetadataResponse::unpack($stream);

        self::assertSame(1, $response->getCorrelationId());
        self::assertArrayHasKey($topic, $response->topics);
        self::assertSame(
            5,
            $response->topics[$topic]->topicErrorCode,
            'a broker with auto.create.topics.enable answers LeaderNotAvailable for a topic it has just created'
        );
        self::assertSame([], $response->topics[$topic]->partitions);
        self::assertFalse($response->topics[$topic]->isInternal, 'a topic of an application is never internal');

        // ... and once the controller has elected the leaders, every partition of the topic is announced
        $topicMetadata = $this->awaitTopicWithLeaders($topic);

        self::assertSame(0, $topicMetadata->topicErrorCode);
        self::assertCount(3, $topicMetadata->partitions, 'the broker of the suite runs with num.partitions=3');
        foreach ($topicMetadata->partitions as $partitionId => $partition) {
            self::assertSame($partitionId, $partition->partitionId);
            self::assertGreaterThanOrEqual(0, $partition->leader);
            self::assertContains($partition->leader, $partition->replicas);
            self::assertContains($partition->leader, $partition->isr);
        }
    }

    public function testANullTopicArrayAsksForEveryTopicOfTheCluster(): void
    {
        $topic = self::uniqueTopicName('t3-metadata-all');
        $this->awaitTopicWithLeaders($topic);

        $response = $this->requestMetadata(null, 2);

        self::assertNotEmpty($response->brokers);
        foreach ($response->brokers as $nodeId => $broker) {
            self::assertSame($nodeId, $broker->nodeId);
            self::assertNotSame('', $broker->host);
            self::assertGreaterThan(0, $broker->port);
        }
        self::assertArrayHasKey($topic, $response->topics, 'a null topic array asks for every topic');
        self::assertArrayHasKey(
            self::INTERNAL_TOPIC,
            $response->topics,
            'and the internal topics of Kafka are part of that answer'
        );
        self::assertTrue($response->topics[self::INTERNAL_TOPIC]->isInternal);
        self::assertFalse($response->topics[$topic]->isInternal);
    }

    public function testAnEmptyTopicArrayAsksForNoTopicAtAll(): void
    {
        $response = $this->requestMetadata([], 3);

        self::assertSame([], $response->topics, 'an empty array is a request for NO topic from version 1 on');
        self::assertNotEmpty($response->brokers, 'the brokers of the cluster are answered all the same');
        self::assertNotNull($response->clusterId);
        self::assertNotNull($response->controllerId);

        // ... and the empty answer is not "this cluster has no topics": the very same connection lists them
        self::assertNotEmpty($this->requestMetadata(null, 4)->topics);
    }

    public function testAnEmptyTopicArrayCreatesNothing(): void
    {
        // Only the topics a request NAMES are auto-created, so a request for no topic can not create one either
        $topic = self::uniqueTopicName('t3-metadata-uncreated');

        self::assertSame([], $this->requestMetadata([], 5)->topics);
        self::assertArrayNotHasKey(
            $topic,
            $this->requestMetadata(null, 6)->topics,
            'the topic that was never named does not exist'
        );
    }

    public function testVersionZeroStillTreatsAnEmptyTopicArrayAsEveryTopic(): void
    {
        $topic = self::uniqueTopicName('t3-metadata-v0');
        $this->awaitTopicWithLeaders($topic);

        $stream = $this->connect();
        new MetadataRequestV0([], self::CLIENT_ID, 7)->writeTo($stream);
        $response = MetadataResponseV0::unpack($stream);

        self::assertArrayHasKey($topic, $response->topics, 'version 0 has no way to say "no topics"');
        self::assertNull($response->clusterId, 'and it carries neither a cluster id ...');
        self::assertNull($response->controllerId, '... nor a controller id');
        self::assertInstanceOf(NodeV0::class, $response->brokers[array_key_first($response->brokers)]);
        self::assertInstanceOf(TopicMetadataV0::class, $response->topics[$topic]);
        self::assertNull($response->topics[$topic]->isInternal, 'the flag is not in a version 0 answer');
    }

    public function testVersionOneAnswersTheControllerIdButNoClusterId(): void
    {
        $topic = self::uniqueTopicName('t3-metadata-v1');
        $this->awaitTopicWithLeaders($topic);

        $stream = $this->connect();
        new MetadataRequestV1([$topic], self::CLIENT_ID, 8)->writeTo($stream);
        $response = MetadataResponseV1::unpack($stream);

        self::assertSame(8, $response->getCorrelationId());
        self::assertNull($response->clusterId, 'the cluster id only travels in version 2');
        self::assertArrayHasKey($response->controllerId, $response->brokers);
        self::assertFalse($response->topics[$topic]->isInternal);
        self::assertCount(3, $response->topics[$topic]->partitions);
        foreach ($response->brokers as $broker) {
            self::assertNull($broker->rack, 'the broker of the container declares no broker.rack');
        }
    }

    public function testVersionTwoAnswersAClusterIdThatEveryRequestRepeats(): void
    {
        $first  = $this->requestMetadata([], 9);
        $second = $this->requestMetadata([], 10);

        self::assertNotNull($first->clusterId);
        self::assertSame(
            22,
            strlen($first->clusterId),
            'the id a 0.10.1 broker generates is a 22 character base64 encoding of a UUID'
        );
        self::assertSame($first->clusterId, $second->clusterId, 'it is the identity of the cluster, not of a request');
    }

    public function testTheControllerIdNamesABrokerOfTheCluster(): void
    {
        $response = $this->requestMetadata([], 11);

        self::assertNotSame(
            MetadataResponse::NO_CONTROLLER_ID,
            $response->controllerId,
            'the cluster of the suite has an elected controller'
        );
        self::assertArrayHasKey($response->controllerId, $response->brokers);
    }

    public function testTheInternalTopicIsFlaggedAsSuch(): void
    {
        $response = $this->requestMetadata([self::INTERNAL_TOPIC], 12);

        self::assertTrue($response->topics[self::INTERNAL_TOPIC]->isInternal);
        self::assertNotEmpty(
            $response->topics[self::INTERNAL_TOPIC]->partitions,
            'the broker of the suite runs with offsets.topic.num.partitions=5'
        );
    }

    public function testClusterResolvesTheLeaderOfEveryPartition(): void
    {
        $topic = self::uniqueTopicName('t3-metadata-cluster');
        $this->awaitTopicWithLeaders($topic);

        $cluster = Cluster::bootstrap($this->configuration(), $topic);

        self::assertContains($topic, $cluster->topics());
        self::assertCount(3, $cluster->partitionsForTopic($topic));

        foreach (array_keys($cluster->partitionsForTopic($topic)) as $partitionId) {
            $leader = $cluster->leaderFor($topic, $partitionId);

            self::assertSame($leader, $cluster->nodeById($leader->nodeId));
            self::assertSame($partitionId, $cluster->partition($topic, $partitionId)->partitionId);
        }
    }

    public function testClusterExposesTheControllerAndTheClusterId(): void
    {
        $topic = self::uniqueTopicName('t3-metadata-controller');
        $this->awaitTopicWithLeaders($topic);

        $cluster    = Cluster::bootstrap($this->configuration(), $topic);
        $controller = $cluster->controller();

        self::assertInstanceOf(Node::class, $controller);
        self::assertArrayHasKey($controller->nodeId, $cluster->nodes());
        self::assertSame($controller, $cluster->nodeById($controller->nodeId));
        self::assertNotNull($cluster->clusterId());
    }

    public function testClusterHidesTheInternalTopicsWhenTheConfigurationAsksForIt(): void
    {
        $configuration = ['exclude.internal.topics' => true] + $this->configuration();
        $cluster       = Cluster::bootstrap($configuration);

        self::assertNotContains(self::INTERNAL_TOPIC, $cluster->topics());
        self::assertContains(
            self::INTERNAL_TOPIC,
            $cluster->topics(false),
            'an explicit argument overrules the configuration'
        );
    }

    public function testClusterListsTheInternalTopicsByDefault(): void
    {
        $cluster = Cluster::bootstrap($this->configuration());

        self::assertContains(self::INTERNAL_TOPIC, $cluster->topics());
        self::assertNotContains(self::INTERNAL_TOPIC, $cluster->topics(true));
    }

    public function testAdminClientFindsTheControllerWithoutProbingTheBrokers(): void
    {
        $topic = self::uniqueTopicName('t3-metadata-admin');
        $this->awaitTopicWithLeaders($topic);

        $cluster = Cluster::bootstrap($this->configuration(), $topic);
        $admin   = new AdminClient($cluster, $this->configuration());

        $controller = $admin->findController();

        self::assertSame($this->requestMetadata([], 13)->controllerId, $controller->nodeId);
        self::assertArrayHasKey($controller->nodeId, $admin->findAllBrokers());

        // findAllBrokers() answers the racks of version 1 - null on a cluster that is not rack aware
        foreach ($admin->findAllBrokers() as $broker) {
            self::assertNull($broker->rack);
        }

        // ... and describeTopics() answers the internal flag of version 1
        $topics = $admin->describeTopics([$topic, self::INTERNAL_TOPIC]);

        self::assertFalse($topics[$topic]->isInternal);
        self::assertTrue($topics[self::INTERNAL_TOPIC]->isInternal);
        self::assertContains($topic, $admin->listTopics(), 'an empty topic list still asks for every topic');
    }

    public function testClusterMetadataSurvivesItsCacheFile(): void
    {
        $topic = self::uniqueTopicName('t3-metadata-cache');
        $this->awaitTopicWithLeaders($topic);

        // The file must not exist yet: an existing but empty cache file is not a valid cache entry
        $cacheFile     = sys_get_temp_dir() . '/t3-metadata-cache-' . bin2hex(random_bytes(6)) . '.php';
        $configuration = [
            ClientConfig::METADATA_CACHE_FILE => $cacheFile,
            ClientConfig::METADATA_MAX_AGE_MS => 300000,
        ] + $this->configuration();

        try {
            $cluster = Cluster::bootstrap($configuration, $topic);
            self::assertNotSame('', file_get_contents($cacheFile), 'the metadata is stored with var_export()');

            // The second cluster does not talk to the broker at all, it restores everything from the cache file
            $cached = Cluster::bootstrap($configuration, $topic);

            self::assertEquals($cluster->nodes(), $cached->nodes());
            self::assertEquals($cluster->partitionsForTopic($topic), $cached->partitionsForTopic($topic));
            self::assertInstanceOf(Node::class, $cached->leaderFor($topic, 0));
            self::assertSame($cluster->clusterId(), $cached->clusterId(), 'the cluster id survives the cache file');
            self::assertEquals($cluster->controller(), $cached->controller(), 'and so does the controller');
        } finally {
            unlink($cacheFile);
        }
    }

    /**
     * Sends a Metadata request of version 2 and returns the answer
     *
     * @param list<string>|null $topics Topics to ask for, `null` asks for every topic, `[]` for none
     */
    private function requestMetadata(?array $topics, int $correlationId): MetadataResponse
    {
        $stream = $this->connect();
        new MetadataRequest($topics, self::CLIENT_ID, $correlationId)->writeTo($stream);

        $response = MetadataResponse::unpack($stream);
        self::assertSame($correlationId, $response->getCorrelationId());

        return $response;
    }

    /**
     * Returns the client configuration of this test class
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID         => self::CLIENT_ID,
        ];
    }

    /**
     * Asks for the metadata of a topic until the controller has elected a leader for each of its partitions
     */
    private function awaitTopicWithLeaders(string $topic): TopicMetadata
    {
        return new TopicMetadataProbe(
            fn(): Stream => $this->connect(),
            self::LEADER_ELECTION_TIMEOUT,
            self::CLIENT_ID
        )->awaitTopicWithLeaders($topic);
    }
}
