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
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigException;
use Protocol\Kafka\Common\Errors\InvalidPartitionsException;
use Protocol\Kafka\Common\Errors\InvalidReplicationFactorException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\RequestTimedOutException;
use Protocol\Kafka\Common\Errors\TopicExistsException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequestV0;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponseV0;
use Protocol\Kafka\Protocol\Request\DeleteTopicsRequest;
use Protocol\Kafka\Protocol\Request\DeleteTopicsResponse;

/**
 * Exercises the CreateTopics (key 19) and DeleteTopics (key 20) apis against a real Kafka 0.10.2.2 broker.
 *
 * Both apis arrived with Kafka 0.10.1 and are served by the ACTIVE CONTROLLER only. The container of
 * `docker-compose.yml` runs a single broker, which is therefore always the controller, so the error code 41
 * (NotController) can not be produced here - it is exercised with a scripted two-broker cluster in
 * `tests/Unit/Admin/AdminClientTest.php`.
 *
 * @see docs/protocol/0.10.2.md, sections "CreateTopics API (key 19, v0 and v1)" and "DeleteTopics API (key 20, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(NewTopic::class)]
final class TopicAdminApiTest extends IntegrationTestCase
{
    /**
     * How long to wait for a topic to appear in or disappear from the metadata of the cluster, in seconds
     */
    private const float METADATA_TIMEOUT = 30.0;

    private Cluster $cluster;

    private AdminClient $admin;

    /**
     * Topics this test class created, deleted again after every test
     *
     * @var list<string>
     */
    private array $createdTopics = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    public function testTheControllerLookupReturnsABrokerOfTheCluster(): void
    {
        $controller = $this->admin->findController();

        self::assertArrayHasKey($controller->nodeId, $this->admin->findAllBrokers());
        self::assertNotSame('', $controller->host);
        self::assertGreaterThan(0, $controller->port);
    }

    public function testATopicIsCreatedWithThePartitionsAndReplicasOfTheRequest(): void
    {
        $topic = $this->topicName('create');

        $result = $this->admin->createTopics([new NewTopic($topic, 3, 1)]);

        self::assertSame([$topic => null], $result, 'the controller created the topic within the timeout');

        $metadata   = $this->awaitTopic($topic);
        $partitions = array_keys($metadata->partitions);
        sort($partitions);
        self::assertSame([0, 1, 2], $partitions, 'three partitions were created');
        foreach ($metadata->partitions as $partition) {
            self::assertSame([0], array_values($partition->replicas), 'the single broker hosts every partition');
        }
        self::assertContains($topic, $this->admin->listTopics());
    }

    public function testATopicIsCreatedFromAnExplicitReplicaAssignmentWithTopicLevelOptions(): void
    {
        $topic    = $this->topicName('assign');
        $brokerId = array_key_first($this->admin->findAllBrokers());

        $result = $this->admin->createTopics([
            NewTopic::withReplicaAssignment(
                $topic,
                [0 => [$brokerId], 1 => [$brokerId]],
                ['retention.ms' => '3600000']
            ),
        ]);

        self::assertSame([$topic => null], $result);

        $metadata   = $this->awaitTopic($topic);
        $partitions = array_keys($metadata->partitions);
        sort($partitions);
        self::assertSame([0, 1], $partitions, 'exactly the assigned partitions were created');
        foreach ($metadata->partitions as $partition) {
            self::assertSame([$brokerId], array_values($partition->replicas), 'the replicas that were asked for');
        }
    }

    public function testAValidateOnlyRequestLeavesNothingBehind(): void
    {
        $topic = $this->topicName('validate');

        $result = $this->admin->createTopics([new NewTopic($topic, 1, 1)], 30000, true);

        self::assertSame([$topic => null], $result, 'the request was valid');
        self::assertNotContains($topic, $this->admin->listTopics(), 'and nothing was written to ZooKeeper');
    }

    public function testCreatingATopicThatAlreadyExistsIsReportedAsTopicExists(): void
    {
        $topic = $this->topicName('exists');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));

        $result = $this->admin->createTopics([new NewTopic($topic, 1, 1)]);

        self::assertInstanceOf(TopicExistsException::class, $result[$topic]);
        self::assertSame(
            "Topic '{$topic}' already exists.",
            $result[$topic]->getContext()['error'] ?? null,
            'version 1 of the api carries the message of the exception the controller caught'
        );
    }

    public function testAReplicationFactorLargerThanTheClusterIsRefused(): void
    {
        $topic  = $this->topicName('factor');
        $result = $this->admin->createTopics([new NewTopic($topic, 1, 2)]);

        self::assertInstanceOf(InvalidReplicationFactorException::class, $result[$topic]);
        self::assertStringContainsString(
            'larger than available brokers',
            (string) ($result[$topic]->getContext()['error'] ?? '')
        );
        self::assertNotContains($topic, $this->admin->listTopics());
    }

    public function testATopicWithoutPartitionsIsRefused(): void
    {
        $topic  = $this->topicName('partitions');
        $result = $this->admin->createTopics([new NewTopic($topic, 0, 1)]);

        self::assertInstanceOf(InvalidPartitionsException::class, $result[$topic]);
        self::assertSame(
            'number of partitions must be larger than 0',
            $result[$topic]->getContext()['error'] ?? null
        );
    }

    public function testAnUnknownTopicLevelOptionIsRefused(): void
    {
        $topic  = $this->topicName('config');
        $result = $this->admin->createTopics([new NewTopic($topic, 1, 1, configs: ['no.such.option' => '1'])]);

        self::assertInstanceOf(InvalidConfigException::class, $result[$topic]);
        self::assertSame(
            'Unknown Log configuration no.such.option.',
            $result[$topic]->getContext()['error'] ?? null
        );
        self::assertNotContains($topic, $this->admin->listTopics());
    }

    public function testAnUnparsableOptionValueIsReportedAsAnUnknownError(): void
    {
        // LogConfig.validate() @ 0.10.2.2 throws Kafka's own InvalidConfigurationException (an ApiException, code
        // 40) for an unknown option NAME, but the plain ConfigException of the config framework for a value it can
        // not parse - and Errors.forException() has no code for that one, so it reaches the wire as -1
        $topic  = $this->topicName('config-value');
        $result = $this->admin->createTopics([new NewTopic($topic, 1, 1, configs: ['retention.ms' => 'soon'])]);

        self::assertNotNull($result[$topic]);
        self::assertStringContainsString(
            'Not a number of type LONG',
            (string) ($result[$topic]->getContext()['error'] ?? '')
        );
    }

    public function testCombiningPartitionsWithAnExplicitAssignmentIsRefused(): void
    {
        $topic  = $this->topicName('both');
        $result = $this->admin->createTopics([new NewTopic($topic, 1, 1, [0 => [0]])]);

        self::assertInstanceOf(InvalidRequestException::class, $result[$topic]);
        self::assertStringContainsString(
            'Both cannot be used at the same time',
            (string) ($result[$topic]->getContext()['error'] ?? '')
        );
    }

    public function testAnIllegalTopicNameIsRefused(): void
    {
        $result = $this->admin->createTopics([new NewTopic('t7 topics illegal name', 1, 1)]);

        self::assertInstanceOf(InvalidTopicException::class, $result['t7 topics illegal name']);
    }

    public function testATimeoutOfZeroAnswersRequestTimedOutWhileTheTopicIsCreatedAnyway(): void
    {
        $topic = $this->topicName('async');

        $result = $this->admin->createTopics([new NewTopic($topic, 1, 1)], 0);

        self::assertInstanceOf(
            RequestTimedOutException::class,
            $result[$topic],
            'a timeout of 0 answers before the controller is done, with the code 7'
        );
        $this->awaitTopic($topic);
    }

    public function testADeletedTopicDisappearsFromTheMetadataOfTheCluster(): void
    {
        $topic = $this->topicName('delete');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));
        $this->awaitTopic($topic);

        $result = $this->admin->deleteTopics([$topic]);
        $this->createdTopics = [];

        self::assertSame([$topic => null], $result, 'the controller finished the deletion within the timeout');
        $this->awaitTopicIsGone($topic);
    }

    public function testDeletingATopicTwiceIsAnsweredWithUnknownTopicOrPartition(): void
    {
        $topic = $this->topicName('twice');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));
        $this->awaitTopic($topic);

        self::assertSame([$topic => null], $this->admin->deleteTopics([$topic]));
        $this->createdTopics = [];
        $this->awaitTopicIsGone($topic);

        $result = $this->admin->deleteTopics([$topic]);

        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[$topic]);
    }

    public function testDeletingAnUnknownTopicIsReportedPerTopic(): void
    {
        $topic  = $this->topicName('never-created');
        $result = $this->admin->deleteTopics([$topic]);
        $this->createdTopics = [];

        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[$topic]);
        self::assertNotContains($topic, $this->admin->listTopics(), 'and the topic was not created by the request');
    }

    public function testDeleteTopicsWithAnEmptyTopicArrayIsAnsweredWithAnEmptyResult(): void
    {
        // This is why findController() can not probe with an empty request: KafkaApis maps over the topics of the
        // REQUEST, so a follower would answer it exactly like the controller does
        $stream = $this->connect();
        new DeleteTopicsRequest([], 0, 't7-topics', 7100)->writeTo($stream);

        $response = DeleteTopicsResponse::unpack($stream);

        self::assertSame(7100, $response->getCorrelationId());
        self::assertSame([], $response->topics, 'no entry at all, not an error code');
    }

    public function testTheControllerProbeOfTheLookupIsAnsweredWithoutSideEffects(): void
    {
        $probe  = '#kafka-client-controller-probe#';
        $stream = $this->connect();
        new DeleteTopicsRequest([$probe], 0, 't7-topics', 7101)->writeTo($stream);

        $response = DeleteTopicsResponse::unpack($stream);

        self::assertSame(
            KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
            $response->topics[$probe]->errorCode,
            'the controller answers 3 for a topic name that can not exist; a follower would answer 41'
        );
        self::assertNotContains($probe, $this->admin->listTopics());
    }

    public function testVersionZeroOfCreateTopicsIsStillServedByTheBroker(): void
    {
        $topic  = $this->topicName('v0');
        $stream = $this->connect();
        new CreateTopicsRequestV0([new NewTopic($topic, 1, 1)], 30000, 't7-topics', 7102)->writeTo($stream);

        $response = CreateTopicsResponseV0::unpack($stream);

        self::assertSame(7102, $response->getCorrelationId());
        self::assertSame(KafkaException::NO_ERROR, $response->topics[$topic]->errorCode);
        self::assertNull($response->topics[$topic]->errorMessage, 'version 0 carries no message at all');
        $this->awaitTopic($topic);
    }

    /**
     * Returns a topic name that is unique for this run and remembers it for the cleanup
     */
    private function topicName(string $purpose): string
    {
        $topic                 = self::uniqueTopicName("t7-topics-{$purpose}");
        $this->createdTopics[] = $topic;

        return $topic;
    }

    /**
     * Waits until the topic is in the metadata of the cluster with a leader for each of its partitions
     */
    private function awaitTopic(string $topic): TopicMetadata
    {
        $deadline = microtime(true) + self::METADATA_TIMEOUT;
        do {
            $metadata = $this->admin->describeTopics([$topic])[$topic] ?? null;
            if ($metadata !== null && $metadata->topicErrorCode === KafkaException::NO_ERROR
                && $metadata->partitions !== []) {
                return $metadata;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The topic {$topic} did not become available in time");
    }

    /**
     * Waits until the topic is gone from the metadata of the cluster
     *
     * The list of every topic is asked for instead of the topic itself: a Metadata request that NAMES an unknown
     * topic creates it again while `auto.create.topics.enable` is on.
     */
    private function awaitTopicIsGone(string $topic): void
    {
        $deadline = microtime(true) + self::METADATA_TIMEOUT;
        do {
            if (!in_array($topic, $this->admin->listTopics(), true)) {
                return;
            }
            usleep(100000);
        } while (microtime(true) < $deadline);

        self::fail("The topic {$topic} is still in the metadata of the cluster");
    }

    /**
     * Client configuration pointing at the broker under test
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 't7-topics',
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}
