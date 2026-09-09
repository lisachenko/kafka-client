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
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the Metadata API v0 against a real Kafka 0.9.0.1 broker.
 *
 * @see docs/protocol/0.10.2.md, section "Metadata API (key 3, v0)"
 */
#[CoversClass(MetadataRequest::class)]
#[CoversClass(MetadataResponse::class)]
#[CoversClass(Node::class)]
#[CoversClass(TopicMetadata::class)]
#[CoversClass(PartitionMetadata::class)]
#[CoversClass(Cluster::class)]
final class MetadataApiTest extends IntegrationTestCase
{
    /**
     * Client id sent along with every request of this test class
     */
    private const string CLIENT_ID = 'kafka-client-t4-metadata';

    /**
     * How long to wait for the controller to elect the leaders of a fresh topic, in seconds
     */
    private const float LEADER_ELECTION_TIMEOUT = 30.0;

    public function testAutoCreatedTopicIsFirstAnnouncedWithoutALeader(): void
    {
        $topic  = self::uniqueTopicName('t4-metadata');
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

    public function testRequestWithoutTopicsReturnsTheWholeCluster(): void
    {
        $topic = self::uniqueTopicName('t4-metadata-all');
        $this->awaitTopicWithLeaders($topic);

        $stream = $this->connect();
        new MetadataRequest([], self::CLIENT_ID, 2)->writeTo($stream);
        $response = MetadataResponse::unpack($stream);

        self::assertNotEmpty($response->brokers);
        foreach ($response->brokers as $nodeId => $broker) {
            self::assertSame($nodeId, $broker->nodeId);
            self::assertNotSame('', $broker->host);
            self::assertGreaterThan(0, $broker->port);
        }
        self::assertArrayHasKey($topic, $response->topics, 'an empty topic list asks for every topic');
    }

    public function testClusterResolvesTheLeaderOfEveryPartition(): void
    {
        $topic = self::uniqueTopicName('t4-metadata-cluster');
        $this->awaitTopicWithLeaders($topic);

        $cluster = Cluster::bootstrap([
            ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID         => self::CLIENT_ID,
        ]);

        self::assertContains($topic, $cluster->topics());
        self::assertCount(3, $cluster->partitionsForTopic($topic));

        foreach (array_keys($cluster->partitionsForTopic($topic)) as $partitionId) {
            $leader = $cluster->leaderFor($topic, $partitionId);

            self::assertSame($leader, $cluster->nodeById($leader->nodeId));
            self::assertSame($partitionId, $cluster->partition($topic, $partitionId)->partitionId);
        }
    }

    public function testClusterMetadataSurvivesItsCacheFile(): void
    {
        $topic = self::uniqueTopicName('t4-metadata-cache');
        $this->awaitTopicWithLeaders($topic);

        // The file must not exist yet: an existing but empty cache file is not a valid cache entry
        $cacheFile     = sys_get_temp_dir() . '/t4-metadata-cache-' . bin2hex(random_bytes(6)) . '.php';
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS   => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID           => self::CLIENT_ID,
            ClientConfig::METADATA_CACHE_FILE => $cacheFile,
            ClientConfig::METADATA_MAX_AGE_MS => 300000,
        ];

        try {
            $cluster = Cluster::bootstrap($configuration);
            self::assertNotSame('', file_get_contents($cacheFile), 'the metadata is stored with var_export()');

            // The second cluster does not talk to the broker at all, it restores everything from the cache file
            $cached = Cluster::bootstrap($configuration);

            self::assertEquals($cluster->nodes(), $cached->nodes());
            self::assertEquals($cluster->partitionsForTopic($topic), $cached->partitionsForTopic($topic));
            self::assertInstanceOf(Node::class, $cached->leaderFor($topic, 0));
        } finally {
            unlink($cacheFile);
        }
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
