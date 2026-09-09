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

namespace Protocol\Kafka\Tests\Unit\Admin;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\AllBrokersNotAvailableException;
use Protocol\Kafka\Common\Errors\BrokerNotAvailableException;
use Protocol\Kafka\Common\Errors\GroupLoadInProgressException;
use Protocol\Kafka\Common\Errors\InvalidGroupIdException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NotControllerException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\TopicExistsException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Errors\UnsupportedForMessageFormatException;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequest;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequest;
use Protocol\Kafka\Protocol\Request\DeleteTopicsRequest;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Tests\Compliance\VectorFile;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * Tests the way the AdminClient maps the answers of a broker onto its return values.
 *
 * The canned answers are the documented wire vectors of `docs/protocol/vectors`, i.e. frames that a real Kafka
 * broker sent, replayed by a scripted broker connection - so this suite and the compliance suite cannot
 * disagree about what a broker says. The scripted connection echoes the correlation id of each request the way a
 * broker does, which is what the client validates the answer against.
 *
 * @see docs/protocol/0.10.2.md, section "Wire vectors"
 */
#[CoversClass(AdminClient::class)]
final class AdminClientTest extends TestCase
{
    /**
     * Name of the topic the wire vectors were recorded for
     */
    private const string TOPIC = 't10-vectors';

    /**
     * Name of the consumer group the wire vectors were recorded for
     */
    private const string GROUP = 't10-vectors-group';

    /**
     * Name of the consumer group that the DescribeGroups and ListGroups vectors were recorded for
     */
    private const string ADMIN_GROUP = 't4-vectors-group';

    /**
     * Group of the DescribeGroups vector that no broker of the cluster knows
     */
    private const string UNKNOWN_GROUP = 't4-vectors-unknown-group';

    /**
     * ListGroups answer of a coordinator that is still reading `__consumer_offsets`: error code 14, no groups
     */
    private const string LOADING_GROUPS_RESPONSE = '0000000a' . '00000000' . '000e' . '00000000';

    /**
     * DescribeGroups answer of a broker that is not the coordinator of `t4-vectors-group`: the group error code 16
     */
    private const string NOT_COORDINATOR_RESPONSE = '00000026' . '00000000' . '00000001'
        . '0010' . '0010' . '74342d766563746f72732d67726f7570' . '0000' . '0000' . '0000' . '00000000';

    /**
     * DescribeGroups answer without an entry for the group that was asked about
     */
    private const string EMPTY_GROUPS_RESPONSE = '00000008' . '00000000' . '00000000';

    /**
     * Address the cluster is bootstrapped from
     */
    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    /**
     * Address of the single broker that the metadata vector announces
     */
    private const string BROKER_ADDRESS = 'tcp://127.0.0.1:9092';

    /**
     * Address of the second broker of the two-broker cluster that the controller lookup is exercised on
     */
    private const string SECOND_BROKER_ADDRESS = 'tcp://127.0.0.1:9093';

    private ScriptedConnections $brokers;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testFindAllBrokersReturnsTheBrokersOfTheMetadataResponse(): void
    {
        $broker = $this->scriptBroker(self::vector('metadata', 'metadata.response.v2.single-topic'));
        $admin  = $this->adminClient();

        $brokers = $admin->findAllBrokers();

        self::assertSame([0], array_keys($brokers));
        self::assertSame('127.0.0.1', $brokers[0]->host);
        self::assertSame(9092, $brokers[0]->port);
        self::assertSame(
            [self::requestFrame(new MetadataRequest([], 't10', $broker->getReceivedCorrelationIds()[0]))],
            $broker->getReceivedFrames(),
            'findAllBrokers() asks for NO topic at all, which version 1 of the api writes as an empty array'
        );
    }

    public function testListTopicsReturnsTheTopicNames(): void
    {
        $this->scriptBroker(self::vector('metadata', 'metadata.response.v2.single-topic'));

        self::assertSame([self::TOPIC], $this->adminClient()->listTopics());
    }

    public function testDescribeTopicsReturnsTheMetadataOfEachTopic(): void
    {
        $broker = $this->scriptBroker(self::vector('metadata', 'metadata.response.v2.single-topic'));

        $topics = $this->adminClient()->describeTopics([self::TOPIC]);

        self::assertSame([self::TOPIC], array_keys($topics));
        self::assertSame(0, $topics[self::TOPIC]->topicErrorCode);
        self::assertSame([2, 1, 0], array_keys($topics[self::TOPIC]->partitions));
        self::assertSame(0, $topics[self::TOPIC]->partitions[0]->leader);
        self::assertSame(
            [self::requestFrame(new MetadataRequest([self::TOPIC], 't10', $broker->getReceivedCorrelationIds()[0]))],
            $broker->getReceivedFrames()
        );
    }

    public function testListOffsetsMapsThePartitionOffsetsOfEveryTopic(): void
    {
        // Version 1 answers one offset per partition; the latest offset comes with the timestamp -1
        $broker = $this->scriptBroker(ResponseFrame::offsets(1, [self::TOPIC => [0 => [0, -1, 2]]]));

        $offsets = $this->adminClient()->listOffsets([self::TOPIC => [0]]);

        self::assertSame([self::TOPIC => [0 => 2]], $offsets, 'the log end offset of the partition');
        self::assertSame(
            [self::requestFrame(new OffsetsRequest(
                [self::TOPIC => [0 => OffsetsRequest::LATEST]],
                OffsetsRequest::CONSUMER_REPLICA_ID,
                't10',
                $broker->getReceivedCorrelationIds()[0]
            ))],
            $broker->getReceivedFrames(),
            'an ordinary client asks the leader of the partition with the replica id -1'
        );
    }

    public function testListOffsetsReportsAnOffsetOfMinusOneWhenNoMessageMatchesTheTimestamp(): void
    {
        $broker = $this->scriptBroker(ResponseFrame::offsets(1, [self::TOPIC => [0 => [0, -1, -1]]]));

        $offsets = $this->adminClient()->listOffsets([self::TOPIC => [0]], 1600000000000);

        self::assertSame([self::TOPIC => [0 => -1]], $offsets, 'nothing matched, and that is not an error');
        self::assertSame(
            [self::requestFrame(new OffsetsRequest(
                [self::TOPIC => [0 => 1600000000000]],
                OffsetsRequest::CONSUMER_REPLICA_ID,
                't10',
                $broker->getReceivedCorrelationIds()[0]
            ))],
            $broker->getReceivedFrames()
        );
    }

    public function testListOffsetsThrowsThePartitionErrorOfTheBroker(): void
    {
        $this->scriptBroker(ResponseFrame::offsets(1, [self::TOPIC => [0 => [3, -1, -1]]]));

        $this->expectExceptionMessage('This server does not host this topic-partition');

        // The leader of the partition is looked up in the cluster metadata, so the request has to name a known one
        $this->adminClient()->listOffsets([self::TOPIC => [0]]);
    }

    public function testListOffsetsThrowsWhenTheTopicHasNoMessageTimestampsToSearch(): void
    {
        // Error code 43, UnsupportedForMessageFormat: the topic runs with message.format.version below 0.10.0
        $this->scriptBroker(ResponseFrame::offsets(1, [self::TOPIC => [0 => [43, -1, -1]]]));

        $this->expectException(UnsupportedForMessageFormatException::class);

        $this->adminClient()->listOffsets([self::TOPIC => [0]], 1600000000000);
    }

    public function testListGroupOffsetsAsksTheCoordinatorAndReturnsTheCommittedOffsets(): void
    {
        $broker = $this->scriptBroker(
            self::vector('group-coordinator', 'groupcoordinator.response.v0'),
            self::vector('offset-fetch', 'offsetfetch.response.v1')
        );

        $topics = $this->adminClient()->listGroupOffsets(self::GROUP, [self::TOPIC => [0]]);

        self::assertSame([self::TOPIC], array_keys($topics));
        self::assertSame(1, $topics[self::TOPIC]->partitions[0]->offset);
        self::assertSame(0, $topics[self::TOPIC]->partitions[0]->errorCode);

        [$lookupId, $fetchId] = $broker->getReceivedCorrelationIds();
        self::assertSame(
            [
                self::requestFrame(new GroupCoordinatorRequest(self::GROUP, 't10', $lookupId)),
                self::requestFrame(new OffsetFetchRequest(self::GROUP, [self::TOPIC => [0]], 't10', $fetchId)),
            ],
            $broker->getReceivedFrames(),
            'the coordinator lookup comes first, the OffsetFetch v1 goes to the coordinator it named'
        );
        self::assertNotSame($lookupId, $fetchId, 'every request carries its own correlation id');
    }

    public function testListGroupOffsetsAcceptsAPartitionThatWasNeverCommitted(): void
    {
        // Version 0 reads from ZooKeeper: any broker answers it, and a partition without a committed offset comes
        // back with the offset -1 and the error code 3
        $broker = $this->scriptBroker(self::vector('offset-fetch', 'offsetfetch.response.v0.no-committed-offset'));
        $admin  = $this->adminClient([ClientConfig::OFFSETS_STORAGE => ClientConfig::OFFSETS_STORAGE_ZOOKEEPER]);

        $topics = $admin->listGroupOffsets(self::GROUP, [self::TOPIC => [1]]);

        self::assertSame(-1, $topics[self::TOPIC]->partitions[1]->offset);
        self::assertSame(3, $topics[self::TOPIC]->partitions[1]->errorCode);
        self::assertSame(
            [self::requestFrame(
                new OffsetFetchRequestV0(self::GROUP, [self::TOPIC => [1]], 't10', $broker->getReceivedCorrelationIds()[0])
            )],
            $broker->getReceivedFrames(),
            'the ZooKeeper-backed version 0 needs no coordinator lookup'
        );
    }

    public function testFindCoordinatorResolvesTheNodeOfTheCluster(): void
    {
        $this->scriptBroker(self::vector('group-coordinator', 'groupcoordinator.response.v0'));

        $coordinator = $this->adminClient()->findCoordinator(self::GROUP);

        self::assertSame(0, $coordinator->nodeId);
        self::assertSame('127.0.0.1', $coordinator->host);
        self::assertSame(9092, $coordinator->port);
    }

    public function testControlledShutdownThrowsTheErrorCodeOfTheController(): void
    {
        // A 0.9.0.1 controller answers an unknown broker id with the code 8, where 0.8.2.2 answered -1
        $broker = $this->scriptBroker(self::vector('controlled-shutdown', 'controlledshutdown.response.v1'));
        $admin  = $this->adminClient();

        try {
            $admin->controlledShutdown(4242);
            self::fail('An unknown broker id has to be reported as an error');
        } catch (BrokerNotAvailableException $exception) {
            self::assertStringContainsString('4242', $exception->getMessage());
        }

        // The admin client sends version 1, the version whose header carries the client id
        self::assertSame(
            [self::requestFrame(new ControlledShutdownRequest(4242, 't10', $broker->getReceivedCorrelationIds()[0]))],
            $broker->getReceivedFrames()
        );
    }

    public function testARequestIsTriedOnEveryBrokerBeforeItIsGivenUp(): void
    {
        // The metadata answer names one broker, and nothing is scripted for it: connecting to it fails
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection(
                self::vector('metadata', 'metadata.response.v2.single-topic')
            ))
            ->install();

        $this->expectException(AllBrokersNotAvailableException::class);

        $this->adminClient()->findAllBrokers();
    }

    public function testListGroupsReturnsTheGroupsTheBrokerCoordinates(): void
    {
        $broker = $this->scriptBroker(
            self::vector('metadata', 'metadata.response.v2.single-topic'),
            self::vector('list-groups', 'listgroups.response.v0')
        );
        $admin  = $this->adminClient();
        $node   = $admin->findAllBrokers()[0];

        $groups = $admin->listGroups($node);

        self::assertSame([self::ADMIN_GROUP], array_keys($groups), 'the groups are indexed by their id');
        self::assertSame('consumer', $groups[self::ADMIN_GROUP]->protocolType);
        self::assertSame(
            self::requestFrame(new ListGroupsRequest('t10', $broker->getReceivedCorrelationIds()[1])),
            $broker->getReceivedFrames()[1],
            'the request of this api is the header and nothing else'
        );
    }

    public function testListGroupsThrowsTheErrorCodeOfTheCoordinator(): void
    {
        $this->scriptBroker(
            self::vector('metadata', 'metadata.response.v2.single-topic'),
            (string) hex2bin(self::LOADING_GROUPS_RESPONSE)
        );
        $admin = $this->adminClient();

        $this->expectException(GroupLoadInProgressException::class);

        $admin->listGroups($admin->findAllBrokers()[0]);
    }

    public function testListAllGroupsAsksEveryBrokerOfTheCluster(): void
    {
        // The metadata vector announces a single broker, which is the only one that has to be asked
        $broker = $this->scriptBroker(
            self::vector('metadata', 'metadata.response.v2.single-topic'),
            self::vector('list-groups', 'listgroups.response.v0')
        );

        $groups = $this->adminClient()->listAllGroups();

        self::assertSame([self::ADMIN_GROUP], array_keys($groups));
        self::assertSame(2, $broker->getRequestCount(), 'one Metadata request and one ListGroups request per broker');
    }

    public function testDescribeGroupAsksTheCoordinatorOfTheGroup(): void
    {
        $broker = $this->scriptBroker(
            self::vector('group-coordinator', 'groupcoordinator.response.v0'),
            self::vector('describe-groups', 'describegroups.response.v0.stable')
        );

        $group = $this->adminClient()->describeGroup(self::ADMIN_GROUP);

        self::assertSame(self::ADMIN_GROUP, $group->groupId);
        self::assertSame(DescribeGroupResponseMetadata::STATE_STABLE, $group->state);
        self::assertSame('consumer', $group->protocolType);
        self::assertSame('range', $group->protocol);
        self::assertCount(1, $group->members);

        [$lookupId, $describeId] = $broker->getReceivedCorrelationIds();
        self::assertSame(
            [
                self::requestFrame(new GroupCoordinatorRequest(self::ADMIN_GROUP, 't10', $lookupId)),
                self::requestFrame(new DescribeGroupsRequest([self::ADMIN_GROUP], 't10', $describeId)),
            ],
            $broker->getReceivedFrames(),
            'the coordinator lookup comes first, the DescribeGroups goes to the coordinator it named'
        );
    }

    public function testDescribeGroupReportsAnUnknownGroupAsDead(): void
    {
        $this->scriptBroker(
            self::vector('group-coordinator', 'groupcoordinator.response.v0'),
            self::vector('describe-groups', 'describegroups.response.v0.dead')
        );

        $group = $this->adminClient()->describeGroup(self::UNKNOWN_GROUP);

        self::assertSame(DescribeGroupResponseMetadata::STATE_DEAD, $group->state, 'this is not an error');
        self::assertSame(0, $group->errorCode);
        self::assertSame([], $group->members);
    }

    public function testDescribeGroupThrowsTheErrorCodeOfTheGroup(): void
    {
        $this->scriptBroker(
            self::vector('group-coordinator', 'groupcoordinator.response.v0'),
            (string) hex2bin(self::NOT_COORDINATOR_RESPONSE)
        );

        $this->expectException(NotCoordinatorForGroupException::class);

        $this->adminClient()->describeGroup(self::ADMIN_GROUP);
    }

    public function testDescribeGroupThrowsWhenTheAnswerHasNoEntryForTheGroup(): void
    {
        $this->scriptBroker(
            self::vector('group-coordinator', 'groupcoordinator.response.v0'),
            (string) hex2bin(self::EMPTY_GROUPS_RESPONSE)
        );

        $this->expectException(InvalidGroupIdException::class);

        $this->adminClient()->describeGroup(self::ADMIN_GROUP);
    }

    public function testDescribeGroupsAsksTheGroupsOfOneCoordinatorWithASingleRequest(): void
    {
        $broker = $this->scriptBroker(
            self::vector('group-coordinator', 'groupcoordinator.response.v0'),
            self::vector('describe-groups', 'describegroups.response.v0.stable')
        );

        $groups = $this->adminClient()->describeGroups([self::ADMIN_GROUP, self::ADMIN_GROUP]);

        self::assertSame([self::ADMIN_GROUP], array_keys($groups), 'a group is asked about only once');
        self::assertSame(2, $broker->getRequestCount(), 'one coordinator lookup and one DescribeGroups request');
    }

    public function testFindControllerReturnsTheBrokerTheMetadataNamesAsTheController(): void
    {
        [$first, $second] = $this->scriptCluster([], [], self::metadataResponse(1));

        $controller = $this->adminClient()->findController();

        self::assertSame(1, $controller->nodeId, 'the ControllerId of the Metadata answer names the controller');
        self::assertSame(0, $first->getRequestCount(), 'no broker is probed any more, the answer already said it');
        self::assertSame(0, $second->getRequestCount());
        self::assertSame(
            1,
            $this->brokers->getConnectionCount(self::BOOTSTRAP_ADDRESS),
            'the metadata of the bootstrap is enough, the lookup opens no further connection'
        );
    }

    public function testFindControllerAsksForTheMetadataOnceMoreWhenItNamesNoController(): void
    {
        // -1 is what a broker answers while the cluster is electing a controller, so the client asks once more
        $this->scriptCluster([], [], self::metadataResponse(-1), self::metadataResponse(1));

        self::assertSame(1, $this->adminClient()->findController()->nodeId);
    }

    public function testFindControllerFailsWhileTheClusterHasNoController(): void
    {
        $this->scriptCluster([], [], self::metadataResponse(-1), self::metadataResponse(-1));

        $this->expectException(NotControllerException::class);

        $this->adminClient()->findController();
    }

    public function testCreateTopicsReportsTheErrorOfEveryTopicOfTheAnswer(): void
    {
        [$controller] = $this->scriptCluster(
            [
                self::createTopicsResponse([
                    't7-created' => [KafkaException::NO_ERROR, null],
                    't7-exists'  => [KafkaException::TOPIC_ALREADY_EXISTS, "Topic 't7-exists' already exists."],
                ]),
            ],
            []
        );

        $result = $this->adminClient()->createTopics([
            new NewTopic('t7-created', 1, 1),
            new NewTopic('t7-exists', 1, 1),
        ]);

        self::assertSame(['t7-created', 't7-exists'], array_keys($result), 'in the order of the request');
        self::assertNull($result['t7-created']);
        self::assertInstanceOf(TopicExistsException::class, $result['t7-exists']);
        self::assertSame(
            ['topic' => 't7-exists', 'error' => "Topic 't7-exists' already exists."],
            $result['t7-exists']->getContext(),
            'the error message of version 1 travels into the context of the exception'
        );
        self::assertSame(
            self::requestFrame(
                new CreateTopicsRequest(
                    [new NewTopic('t7-created', 1, 1), new NewTopic('t7-exists', 1, 1)],
                    30000,
                    false,
                    't10',
                    $controller->getReceivedCorrelationIds()[0]
                )
            ),
            $controller->getReceivedFrames()[0],
            'the request goes out as CreateTopics v1 with the default timeout'
        );
    }

    public function testCreateTopicsIsRepeatedOnceAgainstAFreshlyLookedUpController(): void
    {
        // The controller moved between the lookup and the request: the answer is 41 for every topic, and a second
        // lookup finds the broker that is the controller now
        [$first, $second] = $this->scriptCluster(
            [self::createTopicsResponse(['t7-moved' => [KafkaException::NOT_CONTROLLER, null]])],
            [self::createTopicsResponse(['t7-moved' => [KafkaException::NO_ERROR, null]])],
            self::metadataResponse(0),
            self::metadataResponse(1)
        );

        $result = $this->adminClient()->createTopics([new NewTopic('t7-moved', 1, 1)]);

        self::assertSame(['t7-moved' => null], $result);
        self::assertSame(1, $first->getRequestCount(), 'the broker that was the controller answered 41 once');
        self::assertSame(1, $second->getRequestCount(), 'and the repeated request went to the new controller');
        self::assertSame(
            2,
            $this->brokers->getConnectionCount(self::BOOTSTRAP_ADDRESS),
            'the 41 proves the ControllerId is stale, so the metadata is fetched again before the second lookup'
        );
    }

    public function testTheAnswerOfTheSecondControllerIsReportedAsItIs(): void
    {
        [$first] = $this->scriptCluster(
            [
                self::createTopicsResponse(['t7-moved' => [KafkaException::NOT_CONTROLLER, null]]),
                self::createTopicsResponse(['t7-moved' => [KafkaException::NOT_CONTROLLER, null]]),
            ],
            [],
            self::metadataResponse(0),
            self::metadataResponse(0)
        );

        $result = $this->adminClient()->createTopics([new NewTopic('t7-moved', 1, 1)]);

        self::assertInstanceOf(NotControllerException::class, $result['t7-moved']);
        self::assertSame(2, $first->getRequestCount(), 'the request was repeated once and not a third time');
    }

    public function testDeleteTopicsReportsTheErrorOfEveryTopicOfTheAnswer(): void
    {
        [$controller] = $this->scriptCluster(
            [
                self::deleteTopicsResponse([
                    't7-deleted' => KafkaException::NO_ERROR,
                    't7-unknown' => KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
                ]),
            ],
            []
        );

        $result = $this->adminClient()->deleteTopics(['t7-deleted', 't7-unknown'], 5000);

        self::assertSame(['t7-deleted', 't7-unknown'], array_keys($result));
        self::assertNull($result['t7-deleted']);
        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result['t7-unknown']);
        self::assertSame(
            self::requestFrame(
                new DeleteTopicsRequest(
                    ['t7-deleted', 't7-unknown'],
                    5000,
                    't10',
                    $controller->getReceivedCorrelationIds()[0]
                )
            ),
            $controller->getReceivedFrames()[0]
        );
    }

    public function testATopicTheControllerDidNotAnswerForIsNotReportedAsCreated(): void
    {
        $this->scriptCluster([self::createTopicsResponse([])], []);

        $result = $this->adminClient()->createTopics([new NewTopic('t7-missing', 1, 1)]);

        self::assertInstanceOf(UnknownErrorException::class, $result['t7-missing']);
    }

    /**
     * Scripts a cluster of two brokers and returns the connections that will be handed out for them
     *
     * The Metadata answers go to the BOOTSTRAP connection, because that is where the cluster asks for them - the
     * one of the bootstrap plus one for every refresh the test expects. The brokers themselves are only sent the
     * requests of the api under test: which of them is the controller is part of the metadata now, so nothing is
     * sent to a broker just to find that out.
     *
     * @param list<string> $firstBrokerResponses  Answers of the broker with the node id 0
     * @param list<string> $secondBrokerResponses Answers of the broker with the node id 1
     * @param string       ...$metadataAnswers    Metadata answers of the bootstrap, in order
     *
     * @return array{0: BrokerConnection, 1: BrokerConnection}
     */
    private function scriptCluster(
        array $firstBrokerResponses,
        array $secondBrokerResponses,
        string ...$metadataAnswers
    ): array {
        $first     = new BrokerConnection(...$firstBrokerResponses);
        $second    = new BrokerConnection(...$secondBrokerResponses);
        $answers   = $metadataAnswers !== [] ? $metadataAnswers : [self::metadataResponse()];
        // Cluster::reload() opens a connection of its own for every attempt, so each answer needs one
        $bootstrap = array_map(
            static fn(string $answer): BrokerConnection => new BrokerConnection($answer),
            $answers
        );

        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, ...$bootstrap)
            ->on(self::BROKER_ADDRESS, $first)
            ->on(self::SECOND_BROKER_ADDRESS, $second)
            ->install();

        return [$first, $second];
    }

    /**
     * Builds the Metadata answer of a cluster of two brokers without a single topic
     *
     * @param int $controllerId Broker id the answer names as the controller, -1 for a cluster that elects one
     */
    private static function metadataResponse(int $controllerId = 0): string
    {
        return ResponseFrame::metadata(
            0,
            [[0, '127.0.0.1', 9092], [1, '127.0.0.1', 9093]],
            controllerId: $controllerId
        );
    }

    /**
     * Builds a CreateTopics answer of version 1
     *
     * @param array<string, array{0: int, 1: string|null}> $topics Error code and message of every topic
     */
    private static function createTopicsResponse(array $topics): string
    {
        $body = pack('N', count($topics));
        foreach ($topics as $topic => [$errorCode, $errorMessage]) {
            $body .= pack('n', strlen((string) $topic)) . $topic . pack('n', $errorCode);
            $body .= $errorMessage === null
                ? pack('n', 0xFFFF)
                : pack('n', strlen($errorMessage)) . $errorMessage;
        }

        return ResponseFrame::of(0, $body);
    }

    /**
     * Builds a DeleteTopics answer of version 0
     *
     * @param array<string, int> $topics Error code of every topic
     */
    private static function deleteTopicsResponse(array $topics): string
    {
        $body = pack('N', count($topics));
        foreach ($topics as $topic => $errorCode) {
            $body .= pack('n', strlen((string) $topic)) . $topic . pack('n', $errorCode);
        }

        return ResponseFrame::of(0, $body);
    }

    /**
     * Scripts the answers of the single broker of the cluster and installs the connections
     */
    private function scriptBroker(string ...$responses): BrokerConnection
    {
        $broker = new BrokerConnection(...$responses);
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection(
                self::vector('metadata', 'metadata.response.v2.single-topic')
            ))
            ->on(self::BROKER_ADDRESS, $broker)
            ->install();

        return $broker;
    }

    /**
     * Builds an admin client on the cluster that the scripted brokers answer for
     *
     * @param array<string, mixed> $overrides
     */
    private function adminClient(array $overrides = []): AdminClient
    {
        $configuration = $overrides + [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't10',
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
        ];

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * Returns the frame of a request as a scripted broker records it, i.e. without the leading Size field
     */
    private static function requestFrame(AbstractRequest $request): string
    {
        return substr((string) $request, 4);
    }

    /**
     * Returns the raw frame of a documented wire vector
     */
    private static function vector(string $api, string $id): string
    {
        foreach (VectorFile::read($api)['vectors'] as $vector) {
            if ($vector['id'] === $id) {
                return (string) hex2bin($vector['hex']);
            }
        }

        self::fail("There is no wire vector {$id} in docs/protocol/vectors/{$api}.json");
    }
}
