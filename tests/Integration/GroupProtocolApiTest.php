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
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupRequestV6;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupResponseV6;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupRequestV4;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;
use Protocol\Kafka\Protocol\Request\SyncGroupResponseV4;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * The protocol type and the protocol name of KIP-559 (Kafka 2.5) against a real Kafka 2.8.2 broker.
 *
 * Up to Kafka 2.4 the coordinator alone knew what a group ran on. KIP-559 put it on the wire in both directions:
 * **JoinGroup v7** answers the `protocol_type` of the group next to the `protocol_name` it selected - and made the
 * name nullable, so that an error answer carries `null` twice where a version 6 carries the empty string of
 * `GroupCoordinator.NoProtocol` - and **SyncGroup v5** carries the same pair in the request, where it is nullable
 * on the wire and **mandatory** in the broker: `KafkaApis.handleSyncGroupRequest` @ 2.8.2 answers **23**
 * (`InconsistentGroupProtocol`) to a version 5 that leaves either of them out, before it looks at the group at
 * all, and `GroupCoordinator.handleSyncGroup` answers the same code when they name a protocol the generation did
 * not settle on.
 *
 * Every group of this class is named `t3-25-…`, so that the tests can run next to the other suites on the shared
 * container.
 *
 * @see docs/protocol/2.8.md, sections "The protocol type and name of KIP-559 (Kafka 2.5)", "JoinGroup API (key 11,
 *      v0 to v7)" and "SyncGroup API (key 14, v0 to v5)"
 */
#[CoversClass(JoinGroupRequest::class)]
#[CoversClass(JoinGroupResponse::class)]
#[CoversClass(JoinGroupRequestV6::class)]
#[CoversClass(JoinGroupResponseV6::class)]
#[CoversClass(SyncGroupRequest::class)]
#[CoversClass(SyncGroupResponse::class)]
#[CoversClass(SyncGroupRequestV4::class)]
#[CoversClass(SyncGroupResponseV4::class)]
final class GroupProtocolApiTest extends IntegrationTestCase
{
    /**
     * Client id of this class, which is also the prefix of the member id the coordinator generates
     */
    private const string CLIENT_ID = 'kafka-client-t3-25';

    /**
     * Protocol type of a consumer group, whose member metadata a 2.x coordinator parses itself
     */
    private const string PROTOCOL_TYPE = 'consumer';

    /**
     * Name of the assignor the members of these groups offer
     */
    private const string PROTOCOL_NAME = 'range';

    private const int SESSION_TIMEOUT_MS = 10000;

    private const int REBALANCE_TIMEOUT_MS = 15000;

    private const int REQUEST_TIMEOUT_MS = 30000;

    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * The cluster is resolved once: every test of this class talks to the same brokers
     */
    private static ?Cluster $sharedCluster = null;

    /**
     * Topic of the current test, whose name the member metadata of every join carries
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t3-25-protocol');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), self::TOPIC_TIMEOUT, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    protected function tearDown(): void
    {
        // The shared container keeps every topic a test leaves behind, and enough of them take a log directory
        // down; the property is unset when setUp() skipped the test before it created one
        if (isset($this->topic)) {
            try {
                new AdminClient($this->cluster(), $this->configuration())->deleteTopics([$this->topic]);
            } catch (KafkaException) {
                // A broker that can not delete the topic right now must not fail the test that just passed
            }
        }

        parent::tearDown();
    }

    /**
     * The answer of an accepted JoinGroup v7 names the protocol type of the group next to the protocol it selected
     */
    public function testTheAnswerOfAVersionSevenJoinNamesTheProtocolTypeOfTheGroup(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $joined  = $this->joinWithAssignedMemberId($stream, $groupId, 1101);

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode);
        self::assertSame(self::PROTOCOL_TYPE, $joined->protocolType, 'the protocol type every member sent');
        self::assertSame(self::PROTOCOL_NAME, $joined->groupProtocol, 'the protocol the coordinator selected');

        $this->leave($stream, $groupId, $joined->memberId, 1102);
    }

    /**
     * And the 79 of KIP-394 - the refusal of a first join - carries a null in both fields from version 7 on
     */
    public function testTheRefusalOfAFirstJoinCarriesANullProtocolTypeAndName(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $refused = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, 1103);

        self::assertSame(KafkaException::MEMBER_ID_REQUIRED, $refused->errorCode);
        self::assertNull($refused->protocolType, 'an error answer of a version 7 has no protocol type');
        self::assertNull($refused->groupProtocol, 'and the protocol name became nullable for the same reason');
        self::assertNotSame(
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            $refused->memberId,
            'the assigned member id, which is the whole point of the 79'
        );
    }

    /**
     * The same refusal of a version 6 carries the empty string of `GroupCoordinator.NoProtocol` instead
     */
    public function testTheVersionSixRefusalCarriesTheEmptyProtocolNameInstead(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        new JoinGroupRequestV6(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => new Subscription([$this->topic])->pack()],
            self::CLIENT_ID,
            1104
        )->writeTo($stream);

        $refused = JoinGroupResponseV6::unpack($stream);

        self::assertSame(KafkaException::MEMBER_ID_REQUIRED, $refused->errorCode);
        self::assertSame('', $refused->groupProtocol, 'the field is a plain string below version 7');
        self::assertNull($refused->protocolType, 'the version has no such field, so the property stays null');
    }

    /**
     * A SyncGroup v5 that echoes what the join reported is accepted, and the answer repeats the pair
     */
    public function testASyncOfVersionFiveNamesTheProtocolOfTheGenerationOnBothSides(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $joined  = $this->joinWithAssignedMemberId($stream, $groupId, 1105);

        $synced = $this->sync($stream, $groupId, $joined, self::PROTOCOL_TYPE, $joined->groupProtocol, 1106);

        self::assertSame(KafkaException::NO_ERROR, $synced->errorCode);
        self::assertSame(self::PROTOCOL_TYPE, $synced->protocolType);
        self::assertSame(self::PROTOCOL_NAME, $synced->protocolName);
        self::assertNotSame('', $synced->memberAssignment, 'the leader got the assignment it published');

        $this->leave($stream, $groupId, $joined->memberId, 1107);
    }

    /**
     * A version 5 that names neither is refused with 23, and the membership of the sender survives it
     */
    public function testASyncOfVersionFiveWithoutAProtocolIsRefusedWithTwentyThree(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $joined  = $this->joinWithAssignedMemberId($stream, $groupId, 1108);

        $refused = $this->sync($stream, $groupId, $joined, null, null, 1109);

        self::assertSame(KafkaException::INCONSISTENT_GROUP_PROTOCOL, $refused->errorCode);
        self::assertNull($refused->protocolType);
        self::assertNull($refused->protocolName);
        self::assertSame('', $refused->memberAssignment, 'an error answer carries no assignment');

        // The refusal happens before the coordinator is asked anything, so the member is still in its generation
        $accepted = $this->sync($stream, $groupId, $joined, self::PROTOCOL_TYPE, self::PROTOCOL_NAME, 1110);

        self::assertSame(KafkaException::NO_ERROR, $accepted->errorCode, 'the membership survived the refusal');

        $this->leave($stream, $groupId, $joined->memberId, 1111);
    }

    /**
     * And so is one that names a protocol the generation did not settle on - one check later, same code
     */
    public function testASyncThatNamesAnotherProtocolIsRefusedWithTheSameCode(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $joined  = $this->joinWithAssignedMemberId($stream, $groupId, 1112);

        $wrongName = $this->sync($stream, $groupId, $joined, self::PROTOCOL_TYPE, 'roundrobin', 1113);
        $wrongType = $this->sync($stream, $groupId, $joined, 'connect', self::PROTOCOL_NAME, 1114);

        self::assertSame(KafkaException::INCONSISTENT_GROUP_PROTOCOL, $wrongName->errorCode);
        self::assertSame(KafkaException::INCONSISTENT_GROUP_PROTOCOL, $wrongType->errorCode);

        $this->leave($stream, $groupId, $joined->memberId, 1115);
    }

    /**
     * The version below is still served and still needs no protocol at all
     */
    public function testASyncOfVersionFourNeedsNoProtocolAndIsAccepted(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $joined  = $this->joinWithAssignedMemberId($stream, $groupId, 1116);

        new SyncGroupRequestV4(
            $groupId,
            $joined->generationId,
            $joined->memberId,
            [$joined->memberId => new MemberAssignment([$this->topic => [0]])->pack()],
            self::CLIENT_ID,
            1117
        )->writeTo($stream);

        $synced = SyncGroupResponseV4::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $synced->errorCode, 'the check is skipped below version 5');
        self::assertNull($synced->protocolType, 'the version has neither field');
        self::assertNull($synced->protocolName);

        $this->leave($stream, $groupId, $joined->memberId, 1118);
    }

    /**
     * Sends a JoinGroup v7 and answers the 79 of KIP-394 with the member id it carries
     */
    private function joinWithAssignedMemberId(Stream $stream, string $groupId, int $correlationId): JoinGroupResponse
    {
        $refused = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $correlationId);

        self::assertSame(KafkaException::MEMBER_ID_REQUIRED, $refused->errorCode);

        $joined = $this->join($stream, $groupId, $refused->memberId, $correlationId + 1000);

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode);
        self::assertSame($joined->memberId, $joined->leaderId, 'the only member of the group is its leader');

        return $joined;
    }

    private function join(Stream $stream, string $groupId, string $memberId, int $correlationId): JoinGroupResponse
    {
        new JoinGroupRequest(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            $memberId,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => new Subscription([$this->topic])->pack()],
            self::CLIENT_ID,
            $correlationId
        )->writeTo($stream);

        return JoinGroupResponse::unpack($stream);
    }

    /**
     * Publishes the whole topic to the leader of the generation, with the protocol pair of KIP-559
     */
    private function sync(
        Stream $stream,
        string $groupId,
        JoinGroupResponse $member,
        ?string $protocolType,
        ?string $protocolName,
        int $correlationId
    ): SyncGroupResponse {
        new SyncGroupRequest(
            $groupId,
            $member->generationId,
            $member->memberId,
            [$member->memberId => new MemberAssignment([$this->topic => [0]])->pack()],
            self::CLIENT_ID,
            $correlationId,
            null,
            $protocolType,
            $protocolName
        )->writeTo($stream);

        return SyncGroupResponse::unpack($stream);
    }

    /**
     * Removes the member from its group, so that the group is empty when the test ends
     */
    private function leave(Stream $stream, string $groupId, string $memberId, int $correlationId): void
    {
        new LeaveGroupRequest($groupId, $memberId, self::CLIENT_ID, $correlationId)->writeTo($stream);

        LeaveGroupResponse::unpack($stream);
    }

    /**
     * Opens the connection to the coordinator of the given group
     */
    private function coordinatorStream(string $groupId): Stream
    {
        $configuration = $this->configuration();

        return new CoordinatorLookup($this->cluster(), $configuration)
            ->findCoordinator($groupId)
            ->getConnection($configuration);
    }

    private function cluster(): Cluster
    {
        return self::$sharedCluster ??= Cluster::bootstrap($this->configuration());
    }

    /**
     * @return array<string, mixed> Client configuration of this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,

            ConsumerConfig::SESSION_TIMEOUT_MS      => self::SESSION_TIMEOUT_MS,
            ConsumerConfig::MAX_POLL_INTERVAL_MS    => self::REBALANCE_TIMEOUT_MS,
            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * Builds a consumer group name that is unique for this test run
     */
    private static function uniqueGroupName(): string
    {
        return 't3-25-protocol-group-' . bin2hex(random_bytes(6));
    }
}
