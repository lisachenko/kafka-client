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
use Protocol\Kafka\Admin\ConfigSource;
use Protocol\Kafka\Admin\NewPartitions;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigException;
use Protocol\Kafka\Common\Errors\InvalidPartitionsException;
use Protocol\Kafka\Common\Errors\InvalidReplicaAssignmentException;
use Protocol\Kafka\Common\Errors\InvalidReplicationFactorException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\RequestTimedOutException;
use Protocol\Kafka\Common\Errors\TopicExistsException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequest;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequestV0;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequestV3;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponseV0;
use Protocol\Kafka\Protocol\Request\DeleteTopicsRequest;
use Protocol\Kafka\Protocol\Request\DeleteTopicsResponse;

/**
 * Exercises the topic administration apis - CreateTopics (19), DeleteTopics (20) and CreatePartitions (37) -
 * against a real Kafka 1.1.1 broker.
 *
 * The first two arrived with Kafka 0.10.1, the third with Kafka 1.0 (KIP-195), and all three are served by the
 * ACTIVE CONTROLLER only. The container of `docker-compose.yml` runs a single broker, which is therefore always the
 * controller, so the error code 41 (NotController) can not be produced here - it is exercised with a scripted
 * two-broker cluster in `tests/Unit/Admin/AdminClientTest.php`.
 *
 * @see docs/protocol/2.8.md, sections "CreateTopics API (key 19, v0 to v5)", "DeleteTopics API (key 20, v0 to v4)"
 *      and "CreatePartitions API (key 37, v0 and v1)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(NewTopic::class)]
#[CoversClass(NewPartitions::class)]
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

    /**
     * The -1/-1 of KIP-464: the BROKER chooses the partition count and the replication factor (CreateTopics v4)
     *
     * The image sets `num.partitions=3` and no `default.replication.factor`, so the Kafka default 1 applies - a
     * topic created this way is the same shape as the one above, without the client naming either number.
     */
    public function testATopicCanBeCreatedWithThePartitionDefaultsOfTheBroker(): void
    {
        $topic = $this->topicName('defaults');

        $result = $this->admin->createTopics([NewTopic::withBrokerDefaults($topic)]);

        self::assertSame([$topic => null], $result, 'the controller resolved the -1 against its own configuration');

        $metadata   = $this->awaitTopic($topic);
        $partitions = array_keys($metadata->partitions);
        sort($partitions);
        self::assertSame([0, 1, 2], $partitions, 'the `num.partitions=3` of the image');
        foreach ($metadata->partitions as $partition) {
            self::assertSame([0], array_values($partition->replicas), 'and the default replication factor 1');
        }
        self::assertGreaterThanOrEqual(
            4,
            CreateTopicsRequest::VERSION,
            'KIP-464 needs the version 4; this line sends the flexible 5 of KIP-482, which carries the same meaning'
        );
    }

    /**
     * What KIP-525 put into the answer of the **version 5**: the shape of the topic and its whole configuration
     *
     * `AdminClient::createTopicsWithResults()` is the same request as `createTopics()` with the entries of the
     * answer kept instead of only their errors, so that no DescribeConfigs has to follow the creation.
     */
    public function testTheAnswerOfVersionFiveCarriesTheShapeAndTheConfigurationOfTheNewTopic(): void
    {
        $topic = $this->topicName('kip525');

        $created = $this->admin->createTopicsWithResults([
            new NewTopic($topic, 2, 1, configs: ['retention.ms' => '3600000']),
        ])[$topic];

        self::assertNull($created->error, 'the topic was created');
        self::assertSame($topic, $created->topic);
        self::assertSame(2, $created->numPartitions, 'the partition count the request asked for');
        self::assertSame(1, $created->replicationFactor);
        self::assertSame(0, $created->configErrorCode, 'the broker read the configuration back');
        self::assertNotNull($created->config, 'and sent it');
        self::assertSame(
            '3600000',
            $created->config->value('retention.ms'),
            'the option the request set is in the answer with its value'
        );
        self::assertSame(
            ConfigSource::TOPIC_CONFIG,
            $created->config->get('retention.ms')->source,
            'and with the source a DescribeConfigs would report for it'
        );
        self::assertGreaterThan(
            10,
            count($created->config->entries),
            'every option of the topic is in there, not only the ones the request named'
        );
        self::assertNotNull(
            $created->config->get('cleanup.policy'),
            'an option the request never mentioned, with the value the topic inherited'
        );
        self::assertSame(5, CreateTopicsRequest::VERSION, 'the version KIP-525 needs');
    }

    /**
     * The -1/-1 of KIP-464 and the answer of KIP-525 together: the broker says what it chose
     */
    public function testTheBrokerDefaultsAreReportedBackByTheAnswerOfVersionFive(): void
    {
        $topic = $this->topicName('kip525-defaults');

        $created = $this->admin->createTopicsWithResults([NewTopic::withBrokerDefaults($topic)])[$topic];

        self::assertNull($created->error);
        self::assertSame(3, $created->numPartitions, 'the `num.partitions=3` of the image, reported back');
        self::assertSame(1, $created->replicationFactor, 'and the default replication factor');
    }

    /**
     * A client that sends a version below 4 refuses the shape itself - the broker of 2.8.2 would not
     *
     * `ZkAdminManager.createTopics` @ 2.8.2 resolves the -1 for every api version, so the guard has to be here:
     * `CreateTopicsRequest.Builder.build(version)` @ 2.8.2 throws for it, because a broker of Kafka 2.3 or below
     * has no such fallback and would answer the topic with 37 or 38.
     */
    public function testTheBrokerDefaultsAreRefusedBeforeAVersionThreeRequestIsEvenSent(): void
    {
        $topic = self::uniqueTopicName('t7-topics-never-created');

        try {
            new CreateTopicsRequestV3([NewTopic::withBrokerDefaults($topic)], 30000, false, 't7-topics', 1);
            self::fail('a version 3 request cannot carry the broker defaults of KIP-464');
        } catch (UnsupportedVersionException $exception) {
            self::assertStringContainsString(
                'only supported in CreateTopicRequest version 4+',
                (string) $exception->getContext()['error']
            );
        }

        self::assertNotContains($topic, $this->admin->listTopics(), 'and nothing reached the broker');
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
        // The text changed with Kafka 1.0, the code (37) did not: `AdminUtils.assignReplicasToBrokers` @ 0.11.0.3
        // threw "number of partitions must be larger than 0" and `AdminUtils`/`AdminZkClient` @ 1.1.1 throws
        // "Number of partitions must be larger than 0." - a capital N and a trailing dot
        self::assertSame(
            'Number of partitions must be larger than 0.',
            $result[$topic]->getContext()['error'] ?? null
        );
    }

    public function testAnUnknownTopicLevelOptionIsRefused(): void
    {
        $topic  = $this->topicName('config');
        $result = $this->admin->createTopics([new NewTopic($topic, 1, 1, configs: ['no.such.option' => '1'])]);

        self::assertInstanceOf(InvalidConfigException::class, $result[$topic]);
        // The text changed with Kafka 0.11: `LogConfig.validateNames()` @ 0.11.0.3 throws
        // "Unknown topic config name: <name>" where 0.10.2.2 threw the "Unknown Log configuration <name>." of the
        // generic `AbstractConfig`. The error code (40) and everything else about the answer are unchanged.
        self::assertSame(
            'Unknown topic config name: no.such.option',
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
        // KafkaApis maps over the topics of the REQUEST, so a follower answers an empty one exactly like the
        // controller does - which is why a client that has to PROBE for the controller can not use it
        // (findController() reads the ControllerId of Metadata v1 instead, see the protocol document)
        $stream = $this->connect();
        new DeleteTopicsRequest([], 0, 't7-topics', 7100)->writeTo($stream);

        $response = DeleteTopicsResponse::unpack($stream);

        self::assertSame(7100, $response->getCorrelationId());
        self::assertSame([], $response->topics, 'no entry at all, not an error code');
    }

    public function testADeleteTopicsProbeWithAnIllegalTopicNameIsAnsweredWithoutSideEffects(): void
    {
        // The probe a client without Metadata v1 has to use to find the controller: an illegal topic name, which
        // the controller answers with 3 while every other broker answers 41
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

    public function testThePartitionCountOfATopicIsRaised(): void
    {
        $topic = $this->topicName('grow');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));
        $this->awaitTopic($topic);

        $result = $this->admin->createPartitions([$topic => 3]);

        self::assertSame([$topic => null], $result, 'the controller added the two partitions within the timeout');
        $partitions = array_keys($this->awaitPartitionCount($topic, 3)->partitions);
        sort($partitions);
        self::assertSame([0, 1, 2], $partitions);
    }

    public function testTheNewPartitionsCanBePlacedOnNamedBrokers(): void
    {
        $topic = $this->topicName('assigned');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));
        $this->awaitTopic($topic);

        // One entry per ADDED partition, one broker id per replica - the container has the broker 0 alone
        $result = $this->admin->createPartitions([$topic => NewPartitions::increaseTo(2, [[0]])]);

        self::assertSame([$topic => null], $result);
        $metadata = $this->awaitPartitionCount($topic, 2);
        self::assertSame([0], array_values($metadata->partitions[1]->replicas));
    }

    public function testAValidatedCreatePartitionsAddsNothing(): void
    {
        $topic = $this->topicName('validate-partitions');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));
        $this->awaitTopic($topic);

        $result = $this->admin->createPartitions([$topic => 4], 30000, true);

        self::assertSame([$topic => null], $result, 'the request is valid');
        self::assertCount(
            1,
            $this->admin->describeTopics([$topic])[$topic]->partitions,
            'and the topic still has the partition it was created with'
        );
    }

    public function testATopicCanNotBeShrunk(): void
    {
        $topic = $this->topicName('shrink');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 3, 1)]));
        $this->awaitTopic($topic);

        $fewer = $this->admin->createPartitions([$topic => 2]);
        $same  = $this->admin->createPartitions([$topic => 3]);

        self::assertInstanceOf(InvalidPartitionsException::class, $fewer[$topic]);
        self::assertStringContainsString(
            'Topic currently has 3 partitions, which is higher than the requested 2.',
            $fewer[$topic]->getMessage()
        );
        self::assertInstanceOf(InvalidPartitionsException::class, $same[$topic]);
        self::assertStringContainsString('Topic already has 3 partitions.', $same[$topic]->getMessage());
        self::assertCount(3, $this->admin->describeTopics([$topic])[$topic]->partitions, 'and nothing changed');
    }

    public function testGrowingATopicThatDoesNotExistIsReportedPerTopic(): void
    {
        $topic = self::uniqueTopicName('t7-topics-never-created');

        $result = $this->admin->createPartitions([$topic => 3]);

        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[$topic]);
        self::assertStringContainsString("The topic '{$topic}' does not exist.", $result[$topic]->getMessage());
        self::assertNotContains($topic, $this->admin->listTopics(), 'and the request did not create it either');
    }

    public function testAnAssignmentThatDoesNotMatchTheAddedPartitionsIsRefused(): void
    {
        $topic = $this->topicName('bad-assignment');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));
        $this->awaitTopic($topic);

        $tooFew  = $this->admin->createPartitions([$topic => NewPartitions::increaseTo(3, [[0]])]);
        $unknown = $this->admin->createPartitions([$topic => NewPartitions::increaseTo(2, [[7]])]);

        self::assertInstanceOf(InvalidReplicaAssignmentException::class, $tooFew[$topic]);
        self::assertStringContainsString(
            'Increasing the number of partitions by 2 but 1 assignments provided.',
            $tooFew[$topic]->getMessage()
        );
        self::assertInstanceOf(InvalidReplicaAssignmentException::class, $unknown[$topic]);
        self::assertStringContainsString(
            'Unknown broker(s) in replica assignment: 7.',
            $unknown[$topic]->getMessage()
        );
        self::assertCount(1, $this->admin->describeTopics([$topic])[$topic]->partitions);
    }

    public function testACreatePartitionsTimeoutOfZeroAnswersRequestTimedOutWhileThePartitionsAreAddedAnyway(): void
    {
        $topic = $this->topicName('partitions-timeout');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));
        $this->awaitTopic($topic);

        $result = $this->admin->createPartitions([$topic => 2], 0);

        self::assertInstanceOf(RequestTimedOutException::class, $result[$topic]);
        self::assertCount(
            2,
            $this->awaitPartitionCount($topic, 2)->partitions,
            'the answer only says that the controller was not done yet'
        );
    }

    public function testCreatePartitionsWithoutATopicIsAnsweredWithAnEmptyResult(): void
    {
        self::assertSame([], $this->admin->createPartitions([]));
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
     * Waits until the topic has the expected number of partitions in the metadata of the cluster
     *
     * The controller answers a CreatePartitions as soon as IT has the new partitions; the other brokers - and the
     * metadata cache of the one that answers a Metadata request - learn about them a moment later.
     */
    private function awaitPartitionCount(string $topic, int $expected): TopicMetadata
    {
        $deadline = microtime(true) + self::METADATA_TIMEOUT;
        do {
            $metadata = $this->admin->describeTopics([$topic])[$topic] ?? null;
            if ($metadata !== null && $metadata->topicErrorCode === KafkaException::NO_ERROR
                && count($metadata->partitions) === $expected) {
                return $metadata;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The topic {$topic} did not reach {$expected} partitions in time");
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
