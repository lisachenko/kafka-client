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
use Protocol\Kafka\Protocol\Data\CreateTopicsResponseTopic;
use Protocol\Kafka\Protocol\Data\DeleteTopicsRequestTopic;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequest;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequestV0;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequestV3;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponseV0;
use Protocol\Kafka\Protocol\Request\DeleteTopicsRequest;
use Protocol\Kafka\Protocol\Request\DeleteTopicsResponse;

/**
 * Exercises the topic administration apis - CreateTopics (19), DeleteTopics (20) and CreatePartitions (37) -
 * against the Kafka 3.9.2 KRaft node of this line.
 *
 * The first two arrived with Kafka 0.10.1, the third with Kafka 1.0 (KIP-195), and all three are served by the
 * ACTIVE CONTROLLER only. The node of `docker-compose.yml` is a single combined broker/controller, which is
 * therefore always the controller, so the error code 41 (NotController) can not be produced here - it is exercised
 * with a scripted two-broker cluster in `tests/Unit/Admin/AdminClientTest.php`.
 *
 * Every refusal of the three apis is worded by the KRaft controller now - `ReplicationControlManager` @ 3.9.2 for
 * CreateTopics and CreatePartitions, `ControllerApis.deleteTopics` @ 3.9.2 for DeleteTopics - where the 2.8.2
 * broker answered with the wording of `ZkAdminManager` and `AdminZkClient`; the messages below are the ones the
 * node really sent, and the node id of the image is **1**, not the 0 of the ZooKeeper images of the lines below.
 *
 * @see docs/protocol/3.9.md, sections "CreateTopics API (key 19, v0 to v7)", "DeleteTopics API (key 20, v0 to v6)"
 *      and "CreatePartitions API (key 37, v0 to v3)"
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
            self::assertSame(
                [$this->brokerId()],
                array_values($partition->replicas),
                'the single broker hosts every partition'
            );
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
            self::assertSame(
                [$this->brokerId()],
                array_values($partition->replicas),
                'and the default replication factor 1'
            );
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
        self::assertGreaterThanOrEqual(
            5,
            CreateTopicsRequest::VERSION,
            'the answer of KIP-525 arrived with the version 5 and every version above it carries it'
        );
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
        self::assertNotContains($topic, $this->admin->listTopics(), 'and nothing reached the metadata log');
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
        // The code (38) is the one of every line below; the sentence is the KRaft one. `createTopic()` of
        // `ReplicationControlManager` @ 3.9.2 wraps whatever the replica placer throws into "Unable to replicate
        // the partition <n> time(s): <reason>", and the reason of `StripedReplicaPlacer.
        // throwInvalidReplicationFactorIfTooFewBrokers()` names the registered brokers - where `AdminUtils.
        // assignReplicasToBrokers` @ 2.8.2 said "replication factor: 2 larger than available brokers: 1"
        self::assertSame(
            'Unable to replicate the partition 2 time(s): The target replication factor of 2 cannot be reached '
            . 'because only 1 broker(s) are registered.',
            $result[$topic]->getContext()['error'] ?? null
        );
        self::assertNotContains($topic, $this->admin->listTopics());
    }

    public function testATopicWithoutPartitionsIsRefused(): void
    {
        $topic  = $this->topicName('partitions');
        $result = $this->admin->createTopics([new NewTopic($topic, 0, 1)]);

        self::assertInstanceOf(InvalidPartitionsException::class, $result[$topic]);
        // The text changed with every controller, the code (37) never did: `AdminUtils.assignReplicasToBrokers`
        // @ 0.11.0.3 threw "number of partitions must be larger than 0", `AdminZkClient` @ 1.1.1 and @ 2.8.2
        // "Number of partitions must be larger than 0." - and `ReplicationControlManager.createTopic()` @ 3.9.2
        // refuses the 0 itself, before any placement, with a sentence that says nothing about a lower bound
        self::assertSame(
            'Number of partitions was set to an invalid non-positive value.',
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

    /**
     * A value the `ConfigDef` cannot parse is the code **40** on a KRaft node, where 2.8.2 answered -1
     *
     * `LogConfig.validate()` @ 2.8.2 threw Kafka's own `InvalidConfigurationException` (an `ApiException`, code 40)
     * for an unknown option NAME, but the plain `ConfigException` of the config framework for a value it could not
     * parse - and `Errors.forException()` has no code for that one, so it reached the wire as -1.
     * `ConfigurationControlManager.validateAlterConfig()` @ 3.9.2 catches the `ConfigException` by its type and
     * answers `new ApiError(INVALID_CONFIG, e.getMessage())`, i.e. the same 40 as the unknown name.
     */
    public function testAnUnparsableOptionValueIsRefusedWithForty(): void
    {
        $topic  = $this->topicName('config-value');
        $result = $this->admin->createTopics([new NewTopic($topic, 1, 1, configs: ['retention.ms' => 'soon'])]);

        self::assertInstanceOf(InvalidConfigException::class, $result[$topic]);
        self::assertSame(
            'Invalid value soon for configuration retention.ms: Not a number of type LONG',
            $result[$topic]->getContext()['error'] ?? null
        );
        self::assertNotContains($topic, $this->admin->listTopics());
    }

    public function testCombiningPartitionsWithAnExplicitAssignmentIsRefused(): void
    {
        $topic  = $this->topicName('both');
        $result = $this->admin->createTopics([new NewTopic($topic, 1, 1, [0 => [0]])]);

        self::assertInstanceOf(InvalidRequestException::class, $result[$topic]);
        // The KRaft controller reads the assignment first and then demands the -1/-1 of KIP-464 next to it, one
        // field at a time, instead of the "Both cannot be used at the same time." of `ZkAdminManager` @ 2.8.2:
        // the replication factor is checked before the partition count, so this is the only sentence of the two
        // that a request carrying 1/1 and an assignment can get
        self::assertSame(
            'A manual partition assignment was specified, but replication factor was not set to -1.',
            $result[$topic]->getContext()['error'] ?? null
        );
    }

    public function testAnIllegalTopicNameIsRefused(): void
    {
        $result = $this->admin->createTopics([new NewTopic('t7 topics illegal name', 1, 1)]);

        self::assertInstanceOf(InvalidTopicException::class, $result['t7 topics illegal name']);
    }

    /**
     * A timeout of 0 is the code 7 and nothing else happens - the background creation of a ZooKeeper controller
     * is gone
     *
     * `ZkAdminManager.createTopics()` @ 2.8.2 wrote the topic into ZooKeeper and only THEN waited for the timeout,
     * so a 0 answered 7 for a topic that appeared a moment later. `ControllerApis` @ 3.9.2 turns the `timeout_ms`
     * into a DEADLINE (`requestTimeoutMsToDeadlineNs`) that it hands to the `QuorumController` with the event: a
     * deadline that has already passed expires the event before its records are written, so the answer is the same
     * code 7 and the metadata log never sees the topic.
     */
    public function testATimeoutOfZeroAnswersRequestTimedOutAndCreatesNothing(): void
    {
        $topic = $this->topicName('async');

        $result = $this->admin->createTopics([new NewTopic($topic, 1, 1)], 0);

        self::assertInstanceOf(
            RequestTimedOutException::class,
            $result[$topic],
            'a timeout of 0 expires the controller event, with the code 7'
        );
        self::assertNull($result[$topic]->getContext()['error'] ?? null, 'and the answer carries no message');
        $this->assertTopicStaysAway($topic);
    }

    /**
     * The topic id of KIP-516, which Kafka 2.8 put into the answer of the version 7
     */
    public function testTheAnswerOfVersionSevenCarriesTheIdTheControllerGaveTheTopic(): void
    {
        $topic = $this->topicName('kip516');

        $created = $this->admin->createTopicsWithResults([new NewTopic($topic, 1, 1)])[$topic];

        self::assertNull($created->error, 'the topic was created');
        self::assertSame(16, strlen($created->topicId), 'a topic id is 16 raw bytes, not a string of them');
        self::assertNotSame(
            CreateTopicsResponseTopic::NO_TOPIC_ID,
            $created->topicId,
            'and the controller filled it with the id of the new topic'
        );

        $refused = $this->admin->createTopicsWithResults([new NewTopic($topic, 1, 1)])[$topic];

        self::assertInstanceOf(TopicExistsException::class, $refused->error);
        self::assertSame(
            CreateTopicsResponseTopic::NO_TOPIC_ID,
            $refused->topicId,
            'a topic the controller refused has the zero id - nothing was created to have one'
        );
        self::assertGreaterThanOrEqual(
            7,
            CreateTopicsRequest::VERSION,
            'the id arrived with the version 7 and every version above it carries it'
        );
    }

    /**
     * The other half of KIP-516: a DeleteTopics v6 request names its topic by that id instead of by its name
     */
    public function testATopicIsDeletedByTheIdItsCreationAnswered(): void
    {
        $topic = $this->topicName('kip516-delete');
        $id    = $this->admin->createTopicsWithResults([new NewTopic($topic, 1, 1)])[$topic]->topicId;
        $this->awaitTopic($topic);

        $stream = $this->connect();
        new DeleteTopicsRequest([new DeleteTopicsRequestTopic(null, $id)], 30000, 't7-topics', 7102)
            ->writeTo($stream);
        $response = DeleteTopicsResponse::unpack($stream);
        $this->createdTopics = [];

        $result = $response->topics[$topic] ?? null;
        self::assertNotNull($result, 'the controller resolved the id to the name of the topic it deleted');
        self::assertSame(0, $result->errorCode);
        self::assertSame($id, $result->topicId, 'and echoed the id the request carried');
        $this->awaitTopicIsGone($topic);
    }

    public function testAnIdNoTopicOfTheClusterCarriesIsAnsweredWithUnknownTopicId(): void
    {
        $unknownId = (string) hex2bin('0123456789abcdef0123456789abcdef');

        $stream = $this->connect();
        new DeleteTopicsRequest([new DeleteTopicsRequestTopic(null, $unknownId)], 30000, 't7-topics', 7103)
            ->writeTo($stream);
        $response = DeleteTopicsResponse::unpack($stream);

        // The name of the entry is null, so the answer is not keyed by a topic name at all
        $result = $response->topics[0];
        self::assertNull($result->topic, 'the controller could not resolve the id to a name');
        self::assertSame($unknownId, $result->topicId);
        self::assertSame(
            KafkaException::UNKNOWN_TOPIC_ID,
            $result->errorCode,
            'the error code 100 Kafka 2.8 added for an id, where a name it does not know is still the 3'
        );
    }

    /**
     * A name and an id in one entry is refused per ENTRY on a KRaft node, and the rest of the request is carried out
     *
     * `ZkAdminManager` @ 2.8.2 let the `InvalidRequestException` of the malformed entry escape
     * `handleDeleteTopicsRequest`, which answered the whole request with the code 42 and deleted nothing.
     * `ControllerApis.deleteTopics()` @ 3.9.2 collects the entries first and appends a response of its own for
     * every one it cannot read - "You may not specify both topic name and topic id.", and "Neither topic name nor
     * id were specified." / "Duplicate topic name." / "Duplicate topic id." for the other three shapes - while the
     * entries it CAN read are deleted.
     */
    public function testATopicThatIsNamedByItsNameAndItsIdAtOnceIsRefusedPerEntry(): void
    {
        $topic = $this->topicName('kip516-both');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));
        $this->awaitTopic($topic);

        $stream = $this->connect();
        new DeleteTopicsRequest(
            [
                new DeleteTopicsRequestTopic('t7-topics-never-created', (string) hex2bin(str_repeat('ab', 16))),
                new DeleteTopicsRequestTopic($topic),
            ],
            30000,
            't7-topics',
            7104
        )->writeTo($stream);
        $response = DeleteTopicsResponse::unpack($stream);
        $this->createdTopics = [];

        $malformed = $response->topics['t7-topics-never-created'];
        self::assertSame(KafkaException::INVALID_REQUEST, $malformed->errorCode);
        self::assertSame('You may not specify both topic name and topic id.', $malformed->errorMessage);
        self::assertSame(
            KafkaException::NO_ERROR,
            $response->topics[$topic]->errorCode,
            'and the entry that named its topic properly was deleted all the same'
        );
        $this->awaitTopicIsGone($topic);
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

    /**
     * The controller probe of a client without Metadata v1: an illegal topic name, answered with 3
     *
     * The probe of the lines below sent it with `timeout_ms = 0`, because nothing has to be deleted for the answer
     * to say who the controller is. On a KRaft node that timeout is a deadline that has already passed, so the
     * whole request is answered with the code **7** before any name is looked at - the probe needs a real timeout
     * here, and the answer of the node to the name itself is the 3 with the message of
     * `UnknownTopicOrPartitionException` (there is no follower to answer 41 on a one-node cluster).
     */
    public function testADeleteTopicsProbeWithAnIllegalTopicNameIsAnsweredWithoutSideEffects(): void
    {
        $probe  = '#kafka-client-controller-probe#';
        $stream = $this->connect();
        new DeleteTopicsRequest([$probe], 30000, 't7-topics', 7101)->writeTo($stream);

        $response = DeleteTopicsResponse::unpack($stream);

        self::assertSame(
            KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
            $response->topics[$probe]->errorCode,
            'the controller answers 3 for a topic name that can not exist; a follower would answer 41'
        );
        self::assertSame('This server does not host this topic-partition.', $response->topics[$probe]->errorMessage);
        self::assertNotContains($probe, $this->admin->listTopics());

        $stream = $this->connect();
        new DeleteTopicsRequest([$probe], 0, 't7-topics', 7105)->writeTo($stream);
        $expired = DeleteTopicsResponse::unpack($stream);

        self::assertSame(
            KafkaException::REQUEST_TIMED_OUT,
            $expired->topics[$probe]->errorCode,
            'while the timeout of 0 the lines below used never gets as far as the name'
        );
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

        // One entry per ADDED partition, one broker id per replica - the node of this line is the broker 1 alone
        $brokerId = $this->brokerId();
        $result   = $this->admin->createPartitions([$topic => NewPartitions::increaseTo(2, [[$brokerId]])]);

        self::assertSame([$topic => null], $result);
        $metadata = $this->awaitPartitionCount($topic, 2);
        self::assertSame([$brokerId], array_values($metadata->partitions[1]->replicas));
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

        // `ReplicationControlManager.createPartitions()` @ 3.9.2 names the topic in the shrink sentence and counts
        // in "partition(s)" in both, where `ZkAdminManager` @ 2.8.2 said "Topic currently has 3 partitions, which
        // is higher than the requested 2." and "Topic already has 3 partitions."
        self::assertInstanceOf(InvalidPartitionsException::class, $fewer[$topic]);
        self::assertSame(
            "The topic {$topic} currently has 3 partition(s); 2 would not be an increase.",
            $fewer[$topic]->getContext()['error'] ?? null
        );
        self::assertInstanceOf(InvalidPartitionsException::class, $same[$topic]);
        self::assertSame('Topic already has 3 partition(s).', $same[$topic]->getContext()['error'] ?? null);
        self::assertCount(3, $this->admin->describeTopics([$topic])[$topic]->partitions, 'and nothing changed');
    }

    public function testGrowingATopicThatDoesNotExistIsReportedPerTopic(): void
    {
        $topic = self::uniqueTopicName('t7-topics-never-created');

        $result = $this->admin->createPartitions([$topic => 3]);

        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[$topic]);
        // `ReplicationControlManager.createPartitions()` @ 3.9.2 throws the bare
        // `new UnknownTopicOrPartitionException()` for a name it does not find, so `ApiError.fromThrowable` sends
        // the DEFAULT message of the error code and the answer carries no sentence about the topic at all - where
        // `ZkAdminManager` @ 2.8.2 built "The topic '<name>' does not exist."
        self::assertNull(
            $result[$topic]->getContext()['error'] ?? null,
            'the controller sends the default message of the code 3, which this client does not repeat'
        );
        self::assertStringStartsWith(
            'This server does not host this topic-partition.',
            $result[$topic]->getMessage(),
            'so the exception carries nothing but the sentence of its own code'
        );
        self::assertNotContains($topic, $this->admin->listTopics(), 'and the request did not create it either');
    }

    public function testAnAssignmentThatDoesNotMatchTheAddedPartitionsIsRefused(): void
    {
        $topic = $this->topicName('bad-assignment');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));
        $this->awaitTopic($topic);

        $tooFew  = $this->admin->createPartitions([$topic => NewPartitions::increaseTo(3, [[$this->brokerId()]])]);
        $unknown = $this->admin->createPartitions([$topic => NewPartitions::increaseTo(2, [[7]])]);

        // Both sentences are the KRaft controller's: `createPartitions()` @ 3.9.2 counts the ADDED partitions
        // against the assignments it was given, and `validateManualPartitionAssignment()` names the one broker id
        // it could not resolve - where `ZkAdminManager` @ 2.8.2 said "Increasing the number of partitions by 2 but
        // 1 assignments provided." and "Unknown broker(s) in replica assignment: 7."
        self::assertInstanceOf(InvalidReplicaAssignmentException::class, $tooFew[$topic]);
        self::assertSame(
            'Attempted to add 2 additional partition(s), but only 1 assignment(s) were specified.',
            $tooFew[$topic]->getContext()['error'] ?? null
        );
        self::assertInstanceOf(InvalidReplicaAssignmentException::class, $unknown[$topic]);
        self::assertSame(
            'The manual partition assignment includes broker 7, but no such broker is registered.',
            $unknown[$topic]->getContext()['error'] ?? null
        );
        self::assertCount(1, $this->admin->describeTopics([$topic])[$topic]->partitions);
    }

    /**
     * The same deadline as for CreateTopics: a timeout of 0 is the code 7 and the topic keeps its partitions
     *
     * @see self::testATimeoutOfZeroAnswersRequestTimedOutAndCreatesNothing()
     */
    public function testACreatePartitionsTimeoutOfZeroAnswersRequestTimedOutAndAddsNothing(): void
    {
        $topic = $this->topicName('partitions-timeout');
        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));
        $this->awaitTopic($topic);

        $result = $this->admin->createPartitions([$topic => 2], 0);

        self::assertInstanceOf(RequestTimedOutException::class, $result[$topic]);
        self::assertNull($result[$topic]->getContext()['error'] ?? null, 'and the answer carries no message');
        $deadline = microtime(true) + 3.0;
        do {
            self::assertCount(
                1,
                $this->admin->describeTopics([$topic])[$topic]->partitions,
                'the expired event wrote nothing, so the topic still has the partition it was created with'
            );
            usleep(200000);
        } while (microtime(true) < $deadline);
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
     * Returns the id of the one broker of the node, which is the `node.id=1` of the image and not the 0 of the
     * ZooKeeper images of the lines below
     */
    private function brokerId(): int
    {
        return (int) array_key_first($this->admin->findAllBrokers());
    }

    /**
     * Asserts that a topic the controller refused does not appear in the metadata within the next few seconds
     *
     * A ZooKeeper controller wrote the topic BEFORE it waited for the timeout, so a refusal with the code 7 was
     * followed by the topic showing up; a KRaft controller expires the event with its records unwritten. Proving
     * that nothing happens needs a window rather than a moment, and three seconds are two orders of magnitude more
     * than the node takes to publish a topic it really did create (measured: ~40 ms).
     */
    private function assertTopicStaysAway(string $topic): void
    {
        $deadline = microtime(true) + 3.0;
        do {
            self::assertNotContains($topic, $this->admin->listTopics(), 'and the controller created nothing');
            usleep(200000);
        } while (microtime(true) < $deadline);
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
