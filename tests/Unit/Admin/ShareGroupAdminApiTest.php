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
use Protocol\Kafka\Admin\ListShareGroupOffsetsSpec;
use Protocol\Kafka\Admin\SharePartitionOffsetInfo;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\GroupAuthorizationFailedException;
use Protocol\Kafka\Common\Errors\GroupIdNotFoundException;
use Protocol\Kafka\Common\Errors\GroupNotEmptyException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AlterShareGroupOffsetsRequest;
use Protocol\Kafka\Protocol\Request\DeleteGroupsRequest;
use Protocol\Kafka\Protocol\Request\DeleteShareGroupOffsetsRequest;
use Protocol\Kafka\Protocol\Request\DescribeShareGroupOffsetsRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Tests\Compliance\VectorFile;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * Tests the way the share-group admin methods map the answers of a coordinator onto their return values.
 *
 * The answers are the wire vectors the 4.1 and 4.2 waves captured on the 4.3.1 node, replayed by a scripted broker
 * connection that echoes the correlation id of every request; the few answers no vector holds - a group error, a
 * partition error - are built by hand in the same layout.
 *
 * @see docs/protocol/4.3.md, section "The share-group admin methods"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(ListShareGroupOffsetsSpec::class)]
#[CoversClass(SharePartitionOffsetInfo::class)]
final class ShareGroupAdminApiTest extends TestCase
{
    /**
     * The share group of the 4.2 vectors of DescribeShareGroupOffsets v1, and the group of them that does not exist
     */
    private const string SHARE_GROUP_42 = 't4-42-vectors-share';

    private const string NOBODY_42 = 't4-42-vectors-nobody';

    private const string TOPIC_42 = 't4-42-vectors';

    /**
     * The groups and topics of the 4.1 vectors of AlterShareGroupOffsets and DeleteShareGroupOffsets
     */
    private const string SHARE_GROUP_41 = 't4-41-vectors-share';

    private const string CLASSIC_41 = 't4-41-vectors-classic';

    private const string NOBODY_41 = 't4-41-vectors-nobody';

    private const string TOPIC_41 = 't4-41-vectors';

    private const string MISSING_41 = 't4-41-vectors-missing';

    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    private const string BROKER_ADDRESS = 'tcp://127.0.0.1:9092';

    private const string CLIENT_ID = 't4-s';

    private ScriptedConnections $brokers;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testListShareGroupsAsksEveryBrokerForTheShareTypeAlone(): void
    {
        // The metadata answer names the one broker of the cluster, the only one that has to be asked
        $broker = $this->scriptBroker(
            ResponseFrame::metadata(0, [[0, '127.0.0.1', 9092]], []),
            ResponseFrame::listGroups(0, ['t4-s-share' => ['share', 'Stable', 'share']])
        );

        $groups = $this->adminClient()->listShareGroups(['Stable']);

        self::assertSame(['t4-s-share'], array_keys($groups));
        self::assertSame(
            ['share', 'Stable', ListGroupResponseProtocol::TYPE_SHARE],
            [$groups['t4-s-share']->protocolType, $groups['t4-s-share']->groupState, $groups['t4-s-share']->groupType]
        );
        self::assertSame(
            self::requestFrame(new ListGroupsRequest(
                self::CLIENT_ID,
                $broker->getReceivedCorrelationIds()[1],
                ['Stable'],
                [ListGroupResponseProtocol::TYPE_SHARE]
            )),
            $broker->getReceivedFrames()[1],
            'the `withTypes(SHARE)` of `ListGroupsOptions.forShareGroups()`, applied by the coordinator'
        );
    }

    public function testListShareGroupOffsetsAsksEveryGroupOfOneCoordinatorInOneRequest(): void
    {
        $broker = $this->scriptBroker(
            self::findCoordinators([self::SHARE_GROUP_42, self::NOBODY_42]),
            self::vector('describe-share-group-offsets', 'describesharegroupoffsets.response.v1.named')
        );

        $offsets = $this->adminClient()->listShareGroupOffsets([
            self::SHARE_GROUP_42 => new ListShareGroupOffsetsSpec([new TopicPartition(self::TOPIC_42, 0)]),
            self::NOBODY_42      => [self::TOPIC_42 => [0]],
        ]);

        self::assertEquals(
            [
                self::SHARE_GROUP_42 => [self::TOPIC_42 => [0 => new SharePartitionOffsetInfo(4, 0, 3)]],
                self::NOBODY_42      => [self::TOPIC_42 => [0 => null]],
            ],
            $offsets,
            'the start offset, the leader epoch and the lag of KIP-1226; null for the -1 of absent state'
        );
        self::assertSame(2, $broker->getRequestCount(), 'one FindCoordinator for both groups, one describe');
        self::assertSame(
            self::requestFrame(new DescribeShareGroupOffsetsRequest(
                [self::SHARE_GROUP_42 => [self::TOPIC_42 => [0]], self::NOBODY_42 => [self::TOPIC_42 => [0]]],
                self::CLIENT_ID,
                $broker->getReceivedCorrelationIds()[1]
            )),
            $broker->getReceivedFrames()[1]
        );
    }

    public function testListShareGroupOffsetsAsksForEveryPartitionWithoutASpec(): void
    {
        $broker = $this->scriptBroker(
            self::findCoordinators([self::SHARE_GROUP_42]),
            self::vector('describe-share-group-offsets', 'describesharegroupoffsets.response.v1')
        );

        $offsets = $this->adminClient()->listShareGroupOffsets([self::SHARE_GROUP_42 => null]);

        self::assertEquals([self::SHARE_GROUP_42 => [self::TOPIC_42 => [0 => new SharePartitionOffsetInfo(4, 0, 3)]]], $offsets);
        self::assertSame(
            self::requestFrame(new DescribeShareGroupOffsetsRequest(
                [self::SHARE_GROUP_42 => null],
                self::CLIENT_ID,
                $broker->getReceivedCorrelationIds()[1]
            )),
            $broker->getReceivedFrames()[1],
            'the null topic array'
        );
    }

    public function testListShareGroupOffsetsSkipsAPartitionErrorAndThrowsAGroupError(): void
    {
        $this->scriptBroker(
            self::findCoordinators(['t4-s-partial']),
            self::describeOffsetsResponse(['t4-s-partial' => [0, [self::TOPIC_42 => [0 => [5, 0, 1, 0], 1 => [-1, -1, -1, 15]]]]]),
        );
        self::assertEquals(
            ['t4-s-partial' => [self::TOPIC_42 => [0 => new SharePartitionOffsetInfo(5, 0, 1)]]],
            $this->adminClient()->listShareGroupOffsets(['t4-s-partial' => null]),
            'a partition answered with an error is left out, as the Java `ListShareGroupOffsetsHandler` does'
        );

        ScriptedConnections::uninstall();
        $this->brokers = new ScriptedConnections();
        $this->scriptBroker(
            self::findCoordinators(['t4-s-denied']),
            self::describeOffsetsResponse(['t4-s-denied' => [30, []]], 'Not authorized.'),
        );
        try {
            $this->adminClient()->listShareGroupOffsets(['t4-s-denied' => null]);
            self::fail('The group error was not thrown');
        } catch (GroupAuthorizationFailedException $exception) {
            self::assertSame(['groupId' => 't4-s-denied', 'error' => 'Not authorized.'], $exception->getContext());
        }
    }

    public function testListingNoGroupAtAllSendsNothing(): void
    {
        $broker = $this->scriptBroker();

        self::assertSame([], $this->adminClient()->listShareGroupOffsets([]));
        self::assertSame(0, $broker->getRequestCount());
    }

    public function testAlterShareGroupOffsetsReportsEveryPartition(): void
    {
        $broker = $this->scriptBroker(
            ResponseFrame::groupCoordinator(0, 0, 0, '127.0.0.1', 9092, self::SHARE_GROUP_41),
            self::vector('alter-share-group-offsets', 'altersharegroupoffsets.response.v0')
        );
        $offsets = [self::TOPIC_41 => [0 => 3, 1 => 0, 2 => 0], self::MISSING_41 => [0 => 0]];

        $result = $this->adminClient()->alterShareGroupOffsets(self::SHARE_GROUP_41, $offsets);

        self::assertSame([self::TOPIC_41, self::MISSING_41], array_keys($result), 'in the order of the call');
        self::assertNull($result[self::TOPIC_41][0]);
        self::assertNull($result[self::TOPIC_41][1]);
        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[self::TOPIC_41][2]);
        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[self::MISSING_41][0]);
        self::assertSame(
            [
                'groupId'   => self::SHARE_GROUP_41,
                'topic'     => self::MISSING_41,
                'partition' => 0,
                'error'     => 'This server does not host this topic-partition.',
            ],
            $result[self::MISSING_41][0]->getContext()
        );
        self::assertSame(
            self::requestFrame(new AlterShareGroupOffsetsRequest(
                self::SHARE_GROUP_41,
                $offsets,
                self::CLIENT_ID,
                $broker->getReceivedCorrelationIds()[1]
            )),
            $broker->getReceivedFrames()[1]
        );
    }

    public function testAlterShareGroupOffsetsReportsARefusedGroupForEveryPartition(): void
    {
        $this->scriptBroker(
            ResponseFrame::groupCoordinator(0, 0, 0, '127.0.0.1', 9092, self::CLASSIC_41),
            self::vector('alter-share-group-offsets', 'altersharegroupoffsets.response.v0.not-a-share-group')
        );

        $result = $this->adminClient()->alterShareGroupOffsets(self::CLASSIC_41, [self::TOPIC_41 => [0 => 0, 1 => 0]]);

        foreach ([0, 1] as $partition) {
            self::assertInstanceOf(GroupIdNotFoundException::class, $result[self::TOPIC_41][$partition]);
            self::assertSame(
                'Group t4-41-vectors-classic is not a share group.',
                $result[self::TOPIC_41][$partition]->getContext()['error'] ?? null
            );
        }
    }

    public function testAPartitionTheCoordinatorDidNotAnswerForIsAnUnknownError(): void
    {
        $this->scriptBroker(
            ResponseFrame::groupCoordinator(0, 0, 0, '127.0.0.1', 9092, self::SHARE_GROUP_41),
            self::vector('alter-share-group-offsets', 'altersharegroupoffsets.response.v0')
        );

        $result = $this->adminClient()->alterShareGroupOffsets(self::SHARE_GROUP_41, [self::TOPIC_41 => [7 => 0]]);

        self::assertInstanceOf(UnknownErrorException::class, $result[self::TOPIC_41][7]);
    }

    public function testAlteringOrDeletingNothingSendsNothing(): void
    {
        $broker = $this->scriptBroker();
        $admin  = $this->adminClient();

        self::assertSame([], $admin->alterShareGroupOffsets(self::SHARE_GROUP_41, []));
        self::assertSame([], $admin->deleteShareGroupOffsets(self::SHARE_GROUP_41, []));
        self::assertSame(0, $broker->getRequestCount());
    }

    public function testDeleteShareGroupOffsetsReportsEveryTopic(): void
    {
        $broker = $this->scriptBroker(
            ResponseFrame::groupCoordinator(0, 0, 0, '127.0.0.1', 9092, self::SHARE_GROUP_41),
            self::vector('delete-share-group-offsets', 'deletesharegroupoffsets.response.v0')
        );

        $result = $this->adminClient()->deleteShareGroupOffsets(
            self::SHARE_GROUP_41,
            [self::TOPIC_41, self::MISSING_41, self::TOPIC_41]
        );

        self::assertSame([self::TOPIC_41, self::MISSING_41], array_keys($result), 'duplicates are collapsed');
        self::assertNull($result[self::TOPIC_41]);
        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[self::MISSING_41]);
        self::assertSame(
            self::requestFrame(new DeleteShareGroupOffsetsRequest(
                self::SHARE_GROUP_41,
                [self::TOPIC_41, self::MISSING_41],
                self::CLIENT_ID,
                $broker->getReceivedCorrelationIds()[1]
            )),
            $broker->getReceivedFrames()[1]
        );
    }

    public function testDeleteShareGroupOffsetsThrowsTheRefusalOfTheGroup(): void
    {
        $this->scriptBroker(
            ResponseFrame::groupCoordinator(0, 0, 0, '127.0.0.1', 9092, self::NOBODY_41),
            self::vector('delete-share-group-offsets', 'deletesharegroupoffsets.response.v0.unknown-group')
        );

        try {
            $this->adminClient()->deleteShareGroupOffsets(self::NOBODY_41, [self::TOPIC_41]);
            self::fail('The 69 of a group that does not exist was not thrown');
        } catch (GroupIdNotFoundException $exception) {
            self::assertSame(
                ['groupId' => self::NOBODY_41, 'error' => 'Group t4-41-vectors-nobody not found.'],
                $exception->getContext()
            );
        }
    }

    public function testDeleteShareGroupsIsDeleteGroups(): void
    {
        $broker = $this->scriptBroker(
            ResponseFrame::groupCoordinator(0, 0, 0, '127.0.0.1', 9092, 't4-s-a'),
            ResponseFrame::groupCoordinator(0, 0, 0, '127.0.0.1', 9092, 't4-s-b'),
            ResponseFrame::groupCoordinator(0, 0, 0, '127.0.0.1', 9092, 't4-s-c'),
            self::deleteGroupsResponse([
                't4-s-a' => KafkaException::NO_ERROR,
                't4-s-b' => KafkaException::NON_EMPTY_GROUP,
                't4-s-c' => KafkaException::GROUP_ID_NOT_FOUND,
            ])
        );

        $result = $this->adminClient()->deleteShareGroups(['t4-s-a', 't4-s-b', 't4-s-c']);

        self::assertNull($result['t4-s-a']);
        self::assertInstanceOf(GroupNotEmptyException::class, $result['t4-s-b']);
        self::assertInstanceOf(GroupIdNotFoundException::class, $result['t4-s-c']);
        self::assertSame(
            self::requestFrame(new DeleteGroupsRequest(
                ['t4-s-a', 't4-s-b', 't4-s-c'],
                self::CLIENT_ID,
                $broker->getReceivedCorrelationIds()[3]
            )),
            $broker->getReceivedFrames()[3]
        );
    }

    public function testTheInfoOfAPartitionFollowsTheJavaMapping(): void
    {
        $partition                 = new DescribeShareGroupOffsetsResponsePartition();
        $partition->partitionIndex = 0;
        $partition->startOffset    = -1;
        self::assertNull(SharePartitionOffsetInfo::fromResponsePartition($partition), 'no start offset, no info');

        $partition->startOffset = 12;
        $partition->leaderEpoch = -1;
        $partition->lag         = DescribeShareGroupOffsetsResponsePartition::UNINITIALIZED_LAG;
        self::assertEquals(
            new SharePartitionOffsetInfo(12, null, null),
            SharePartitionOffsetInfo::fromResponsePartition($partition),
            'the empty Optionals of a negative leader epoch and lag'
        );
    }

    public function testASpecTakesPartitionsInEveryShape(): void
    {
        self::assertNull(ListShareGroupOffsetsSpec::allPartitions()->topicPartitions);
        self::assertNull(new ListShareGroupOffsetsSpec()->topicPartitions);
        self::assertSame(
            ['a' => [0, 2], 'b' => [1]],
            new ListShareGroupOffsetsSpec([new TopicPartition('a', 0), new TopicPartition('b', 1), new TopicPartition('a', 2)])->topicPartitions
        );
        self::assertSame(['a' => [0, 1]], new ListShareGroupOffsetsSpec(['a' => [0, 1]])->topicPartitions);
        self::assertSame([], new ListShareGroupOffsetsSpec([])->topicPartitions, 'no partition at all');
    }

    /**
     * Builds a FindCoordinator v4 answer that names the broker 0 as the coordinator of every key
     *
     * @param list<string> $keys
     */
    private static function findCoordinators(array $keys): string
    {
        $body = pack('N', 0) . ResponseFrame::compactCount(count($keys));
        foreach ($keys as $key) {
            $body .= ResponseFrame::compactString($key)
                . pack('N', 0)
                . ResponseFrame::compactString('127.0.0.1')
                . pack('N', 9092)
                . pack('n', 0)
                . ResponseFrame::compactString('')
                . ResponseFrame::tagBuffer();
        }

        return ResponseFrame::flexible(0, $body);
    }

    /**
     * Builds a DescribeShareGroupOffsets v1 answer
     *
     * @param array<string, array{int, array<string, array<int, array{int, int, int, int}>>}> $groups Error code and
     *        topic => partition => [start offset, leader epoch, lag, error code] of every group
     */
    private static function describeOffsetsResponse(array $groups, ?string $groupErrorMessage = null): string
    {
        $body = pack('N', 0) . ResponseFrame::compactCount(count($groups));
        foreach ($groups as $groupId => [$errorCode, $topics]) {
            $body .= ResponseFrame::compactString((string) $groupId) . ResponseFrame::compactCount(count($topics));
            foreach ($topics as $topic => $partitions) {
                $body .= ResponseFrame::compactString((string) $topic)
                    . ResponseFrame::topicIdOf((string) $topic)
                    . ResponseFrame::compactCount(count($partitions));
                foreach ($partitions as $index => [$startOffset, $leaderEpoch, $lag, $partitionError]) {
                    $body .= pack('N', $index) . pack('J', $startOffset) . pack('N', $leaderEpoch) . pack('J', $lag)
                        . pack('n', $partitionError) . ResponseFrame::compactString(null) . ResponseFrame::tagBuffer();
                }
                $body .= ResponseFrame::tagBuffer();
            }
            $body .= pack('n', $errorCode) . ResponseFrame::compactString($groupErrorMessage) . ResponseFrame::tagBuffer();
        }

        return ResponseFrame::flexible(0, $body);
    }

    /**
     * Builds a DeleteGroups v2 answer
     *
     * @param array<string, int> $groups Error code of every group
     */
    private static function deleteGroupsResponse(array $groups): string
    {
        $body = pack('N', 0) . ResponseFrame::compactCount(count($groups));
        foreach ($groups as $groupId => $errorCode) {
            $body .= ResponseFrame::compactString((string) $groupId) . pack('n', $errorCode) . ResponseFrame::tagBuffer();
        }

        return ResponseFrame::flexible(0, $body);
    }

    /**
     * Scripts the answers of the single broker of the cluster and installs the connections
     */
    private function scriptBroker(string ...$responses): BrokerConnection
    {
        $broker = new BrokerConnection(...$responses);
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection(ResponseFrame::metadata(0, [[0, '127.0.0.1', 9092]], [])))
            ->on(self::BROKER_ADDRESS, $broker)
            ->install();

        return $broker;
    }

    private function adminClient(): AdminClient
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
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
