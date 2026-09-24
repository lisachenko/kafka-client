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

namespace Protocol\Kafka\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\FencedMemberEpochException;
use Protocol\Kafka\Common\Errors\GroupIdNotFoundException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\InvalidShareSessionEpochException;
use Protocol\Kafka\Common\Errors\ShareSessionNotFoundException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgementBatch;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\ShareAcknowledgeRequest;
use Protocol\Kafka\Protocol\Request\ShareFetchRequest;
use Protocol\Kafka\Protocol\Request\ShareGroupDescribeRequest;
use Protocol\Kafka\Protocol\Request\ShareGroupHeartbeatRequest;
use Protocol\Kafka\Tests\Compliance\MessageFields;
use Protocol\Kafka\Tests\Compliance\VectorFile;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * The share-group methods of `Client` and `AdminClient` (Kafka 4.1, KIP-932), driven against the answers the 4.3.1
 * node really gave: every scripted answer below is a vector of `share-*.json`, and the frames the methods send are
 * read back field by field.
 *
 * @see docs/protocol/4.3.md, section "ShareGroupHeartbeat API (key 76, v1)"
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1)"
 * @see docs/protocol/4.3.md, section "ShareAcknowledge API (key 79, v1)"
 * @see docs/protocol/4.3.md, section "ShareGroupDescribe API (key 77, v1)"
 */
#[CoversClass(Client::class)]
#[CoversClass(AdminClient::class)]
final class ShareGroupClientTest extends TestCase
{
    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    private const string NODE_ADDRESS = 'tcp://kafka-1:9092';

    private const string TOPIC = 'orders';

    private const string GROUP_ID = 't3-41-share';

    private const string MEMBER_ID = '6B5qa6GHQKSbm2LmJRQrCg';

    private ScriptedConnections $brokers;

    private ?Cluster $cluster = null;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testTheJoinIsTheEpochZeroWithTheWholeSubscription(): void
    {
        $node   = $this->script(self::vector('share-group-heartbeat', 'sharegroupheartbeat.v1.join.response'));
        $answer = $this->client()->joinShareGroup($this->node(), self::GROUP_ID, self::MEMBER_ID, [self::TOPIC], 'rack-a');

        self::assertSame(self::MEMBER_ID, $answer->memberId);
        self::assertSame([], $answer->assignment?->partitionsByTopicId());

        $sent = $this->sent($node, 0, ShareGroupHeartbeatRequest::class);
        self::assertSame(ApiKeys::SHARE_GROUP_HEARTBEAT, $sent['apiKey']);
        self::assertSame(1, $sent['apiVersion']);
        self::assertSame(
            ['t3-41-share', self::MEMBER_ID, 0, 'rack-a', [self::TOPIC]],
            [$sent['groupId'], $sent['memberId'], $sent['memberEpoch'], $sent['rackId'], $sent['subscribedTopicNames']]
        );
    }

    public function testASteadyHeartbeatAndTheLeaveCarryNullsAndTheirEpoch(): void
    {
        $node = $this->script(
            self::vector('share-group-heartbeat', 'sharegroupheartbeat.v1.steady.response'),
            self::vector('share-group-heartbeat', 'sharegroupheartbeat.v1.leave.response')
        );

        $steady = $this->client()->shareGroupHeartbeat($this->node(), self::GROUP_ID, self::MEMBER_ID, 3);
        $leave  = $this->client()->leaveShareGroup($this->node(), self::GROUP_ID, self::MEMBER_ID);

        self::assertNull($steady->assignment);
        self::assertSame(-1, $leave->memberEpoch);

        $heartbeat = $this->sent($node, 0, ShareGroupHeartbeatRequest::class);
        self::assertSame([3, null, null], [$heartbeat['memberEpoch'], $heartbeat['rackId'], $heartbeat['subscribedTopicNames']]);
        $left = $this->sent($node, 1, ShareGroupHeartbeatRequest::class);
        self::assertSame(ShareGroupHeartbeatRequest::LEAVE_MEMBER_EPOCH, $left['memberEpoch']);
    }

    public function testAFencedHeartbeatIsThrownWithTheSentenceOfTheCoordinator(): void
    {
        $this->script(self::vector('share-group-heartbeat', 'sharegroupheartbeat.v1.fenced.response'));

        try {
            $this->client()->shareGroupHeartbeat($this->node(), self::GROUP_ID, self::MEMBER_ID, 13);
            self::fail('the 110 is thrown');
        } catch (FencedMemberEpochException $fenced) {
            $context = $fenced->getContext();
            self::assertSame(self::GROUP_ID, $context['groupId']);
            self::assertSame(13, $context['memberEpoch']);
            self::assertStringStartsWith('The share group member has a greater member epoch (13)', (string) $context['error']);
        }
    }

    public function testAJoinWithoutAMemberIdIsTheFortyTwoWithoutASentence(): void
    {
        $this->script(self::vector('share-group-heartbeat', 'sharegroupheartbeat.v1.join-without-member-id.response'));

        try {
            $this->client()->joinShareGroup($this->node(), self::GROUP_ID, '', [self::TOPIC]);
            self::fail('the 42 is thrown');
        } catch (InvalidRequestException $refused) {
            self::assertArrayNotHasKey('error', $refused->getContext(), 'the node gives no message');
        }
    }

    public function testAShareFetchSendsThePartitionsTheAcknowledgementsAndTheForgottenPartitions(): void
    {
        $node    = $this->script(self::vector('share-fetch', 'sharefetch.v1.acknowledge-and-redeliver.response'));
        $topicId = str_repeat("\x07", 16);
        $release = ShareAcknowledgementBatch::of(3, 5, ShareAcknowledgementBatch::RELEASE);

        $answer = $this->client()->shareFetch(
            $this->node(),
            self::GROUP_ID,
            self::MEMBER_ID,
            1,
            [$topicId => [0]],
            [$topicId => [0 => [$release]]],
            250,
            1,
            100,
            50,
            [$topicId => [4]]
        );

        self::assertSame(30000, $answer->acquisitionLockTimeoutMs);
        self::assertCount(4, $answer->responses[0]->partitions[0]->acquiredRecords());

        $sent = $this->sent($node, 0, ShareFetchRequest::class);
        self::assertSame([ApiKeys::SHARE_FETCH, 1], [$sent['apiKey'], $sent['apiVersion']]);
        self::assertSame(
            [1, 250, 1, ShareFetchRequest::DEFAULT_MAX_BYTES, 100, 50],
            [$sent['shareSessionEpoch'], $sent['maxWaitMs'], $sent['minBytes'], $sent['maxBytes'], $sent['maxRecords'], $sent['batchSize']]
        );
        self::assertSame(
            [[
                'topicId'    => ['$bytes' => bin2hex($topicId)],
                'partitions' => [[
                    'partitionIndex'         => 0,
                    'acknowledgementBatches' => [['firstOffset' => 3, 'lastOffset' => 5, 'acknowledgeTypes' => [2]]],
                ]],
            ]],
            $sent['topics']
        );
        self::assertSame([['topicId' => ['$bytes' => bin2hex($topicId)], 'partitions' => [4]]], $sent['forgottenTopicsData']);
    }

    public function testTheSessionCodesOfAShareFetchAreThrown(): void
    {
        $this->script(
            self::vector('share-fetch', 'sharefetch.v1.wrong-epoch.response'),
            self::vector('share-fetch', 'sharefetch.v1.session-not-found.response')
        );

        try {
            $this->client()->shareFetch($this->node(), self::GROUP_ID, self::MEMBER_ID, 5, []);
            self::fail('the 123 is thrown');
        } catch (InvalidShareSessionEpochException $wrongEpoch) {
            self::assertSame(5, $wrongEpoch->getContext()['shareSessionEpoch']);
        }

        $this->expectException(ShareSessionNotFoundException::class);
        $this->client()->shareFetch($this->node(), self::GROUP_ID, self::MEMBER_ID, 5, []);
    }

    public function testAPartitionErrorOfShareAcknowledgeStaysInTheAnswer(): void
    {
        $node    = $this->script(
            self::vector('share-acknowledge', 'shareacknowledge.v1.accept-twice.response'),
            self::vector('share-acknowledge', 'shareacknowledge.v1.close.response')
        );
        $topicId = str_repeat("\x07", 16);

        $answer = $this->client()->shareAcknowledge($this->node(), self::GROUP_ID, self::MEMBER_ID, 3, [
            $topicId => [0 => [ShareAcknowledgementBatch::of(3, 3, ShareAcknowledgementBatch::ACCEPT)]],
        ]);
        $closed = $this->client()->shareAcknowledge($this->node(), self::GROUP_ID, self::MEMBER_ID, ShareFetchRequest::FINAL_EPOCH);

        self::assertSame(121, $answer->responses[0]->partitions[0]->errorCode, 'the 121 of the partition is not thrown');
        self::assertSame([], $closed->responses);

        $close = $this->sent($node, 1, ShareAcknowledgeRequest::class);
        self::assertSame([ApiKeys::SHARE_ACKNOWLEDGE, -1, []], [$close['apiKey'], $close['shareSessionEpoch'], $close['topics']]);
    }

    public function testAShareGroupIsDescribedByItsCoordinator(): void
    {
        $node = $this->script(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092, 't3-41-vectors-group'),
            self::vector('share-group-describe', 'sharegroupdescribe.v1.stable.response')
        );

        $group = $this->admin()->describeShareGroup('t3-41-vectors-group', true);

        self::assertSame('Stable', $group->groupState);
        self::assertSame([self::MEMBER_ID], array_keys($group->members));
        self::assertSame(3400, $group->authorizedOperations);

        $sent = $this->sent($node, 1, ShareGroupDescribeRequest::class);
        self::assertSame(
            [ApiKeys::SHARE_GROUP_DESCRIBE, 1, ['t3-41-vectors-group'], true],
            [$sent['apiKey'], $sent['apiVersion'], $sent['groupIds'], $sent['includeAuthorizedOperations']]
        );
    }

    public function testAnUnknownShareGroupIsTheSixtyNineWithItsSentence(): void
    {
        $this->script(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092, 't3-41-vectors-none'),
            self::vector('share-group-describe', 'sharegroupdescribe.v1.unknown-group.response')
        );

        try {
            $this->admin()->describeShareGroups(['t3-41-vectors-none']);
            self::fail('the 69 is thrown');
        } catch (GroupIdNotFoundException $unknown) {
            self::assertSame('Group t3-41-vectors-none not found.', $unknown->getContext()['error']);
        }
    }

    /**
     * Scripts the one node of the cluster and returns its connection
     */
    private function script(string ...$answers): BrokerConnection
    {
        $node = new BrokerConnection(...$answers);
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection(
                ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0]])
            ))
            ->on(self::NODE_ADDRESS, $node)
            ->install();

        return $node;
    }

    /**
     * Reads back the fields of the n-th frame a scripted node received
     *
     * @param class-string<AbstractRequest> $class
     *
     * @return array<string, mixed>
     */
    private function sent(BrokerConnection $node, int $index, string $class): array
    {
        $frame = $node->getReceivedFrames()[$index];

        return MessageFields::of($class::unpack(new StringStream(pack('N', strlen($frame)) . $frame)));
    }

    private function node(): Node
    {
        $node = $this->cluster()->nodeById(0);
        self::assertNotNull($node);

        return $node;
    }

    private function client(): Client
    {
        return new Client($this->cluster(), $this->configuration());
    }

    private function admin(): AdminClient
    {
        return new AdminClient($this->cluster(), $this->configuration());
    }

    private function cluster(): Cluster
    {
        return $this->cluster ??= Cluster::bootstrap($this->configuration());
    }

    /**
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't3-client',
            ClientConfig::REQUEST_TIMEOUT_MS        => 500,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
            ClientConfig::RETRIES                   => 0,
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * The bytes of one answer the 4.3.1 node gave, as a vector file holds them
     */
    private static function vector(string $api, string $id): string
    {
        foreach (VectorFile::read($api)['vectors'] as $vector) {
            if ($vector['id'] === $id) {
                return (string) hex2bin((string) $vector['hex']);
            }
        }

        self::fail("no vector {$id} in {$api}.json");
    }
}
