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
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\RangeAssignor;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Tests\Fixture\RawApiProbe;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;
use RuntimeException;

/**
 * Runs the `protocol_type = "consumer"` structures through a real Kafka 0.9.0.1 broker.
 *
 * The coordinator never parses the `member_metadata` and `member_assignment` fields, it only moves them between the
 * members of a group, so what this test establishes is exactly that: the {@see Subscription} this client packs
 * comes back to the leader of the group byte for byte in the JoinGroup response, and the {@see MemberAssignment}
 * the leader publishes with SyncGroup comes back to the member it was meant for, byte for byte.
 *
 * The frames are built by hand ({@see RawApiProbe}) because the request classes of the group apis belong to
 * another ticket; this suite covers the payloads they will carry, not the framing.
 *
 * @see docs/protocol/0.9.0.md, section "Consumer group protocol (protocol_type = consumer)"
 */
#[CoversClass(Subscription::class)]
#[CoversClass(MemberAssignment::class)]
#[CoversClass(RangeAssignor::class)]
final class ConsumerProtocolTest extends IntegrationTestCase
{
    /**
     * Client id of this test class, also the prefix of the member id the coordinator hands out
     */
    private const string CLIENT_ID = 't6-consumer-protocol';

    /**
     * Session timeout of the member, within the group.min.session.timeout.ms of the container
     */
    private const int SESSION_TIMEOUT_MS = 6000;

    /**
     * Topic that this test class subscribes to, with a unique name so that other suites do not disturb it
     */
    private static string $topic = '';

    /**
     * Group of one test method: every test joins a group of its own, so that no member of another one is left in it
     */
    private string $groupId;

    /**
     * Number of partitions of the topic, as the cluster metadata reports them
     */
    private int $partitionCount;

    protected function setUp(): void
    {
        parent::setUp();

        if (self::$topic === '') {
            self::$topic = 't6-consumer-protocol-' . bin2hex(random_bytes(6));
        }
        $this->groupId = 't6-consumer-group-' . bin2hex(random_bytes(6));

        $metadata = new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders(self::$topic);

        $this->partitionCount = count($metadata->partitions);
    }

    public function testTheCoordinatorRelaysTheSubscriptionOfAMemberToTheLeader(): void
    {
        $probe        = new RawApiProbe(self::firstBootstrapServer());
        $assignor     = new RangeAssignor();
        $subscription = $assignor->subscription([self::$topic]);

        $join = $this->joinGroup($probe, $assignor->name(), $subscription);

        self::assertSame(0, $join['errorCode'], 'The broker refused the join of a `consumer` group');
        self::assertSame($assignor->name(), $join['groupProtocol'], 'The coordinator chose another group protocol');
        self::assertSame(
            $join['memberId'],
            $join['leaderId'],
            'The only member of a fresh group has to be its leader'
        );
        self::assertSame([$join['memberId']], array_keys($join['members']));

        // The metadata the coordinator hands to the leader is the byte array the member sent, untouched
        self::assertSame($subscription->pack(), $join['members'][$join['memberId']]);
        self::assertSame([self::$topic], Subscription::unpack($join['members'][$join['memberId']])->topics);

        $this->leaveGroup($probe, $join['memberId']);
    }

    public function testTheAssignmentOfTheLeaderComesBackToItsMemberByteForByte(): void
    {
        $probe    = new RawApiProbe(self::firstBootstrapServer());
        $assignor = new RangeAssignor();

        $join = $this->joinGroup($probe, $assignor->name(), $assignor->subscription([self::$topic]));
        self::assertSame(0, $join['errorCode']);

        // The leader of the group runs the assignor on the subscriptions the coordinator gave it, exactly as
        // KafkaConsumer will: there is a single member, so it gets every partition of the topic
        $subscriptions = [];
        foreach ($join['members'] as $memberId => $metadata) {
            $subscriptions[$memberId] = Subscription::unpack($metadata);
        }
        $assignments = $assignor->assign([self::$topic => $this->partitionCount], $subscriptions);

        $sync = $this->syncGroup($probe, $join['memberId'], $join['generationId'], $assignments);

        self::assertSame(0, $sync['errorCode'], 'The broker refused the assignment of the leader');
        self::assertSame($assignments[$join['memberId']]->pack(), $sync['memberAssignment']);

        $assignment = MemberAssignment::unpack($sync['memberAssignment']);
        self::assertSame(MemberAssignment::VERSION, $assignment->version);
        self::assertSame([self::$topic => range(0, $this->partitionCount - 1)], $assignment->partitions());
        self::assertSame('', $assignment->userData);

        $this->leaveGroup($probe, $join['memberId']);
    }

    public function testAMemberWithoutPartitionsGetsAnEmptyAssignmentBack(): void
    {
        $probe    = new RawApiProbe(self::firstBootstrapServer());
        $assignor = new RangeAssignor();

        $join = $this->joinGroup($probe, $assignor->name(), $assignor->subscription([self::$topic]));
        self::assertSame(0, $join['errorCode']);

        // A group with more members than partitions is the situation in which the leader publishes the empty
        // structure; here the leader simply keeps nothing for itself
        $empty = new MemberAssignment();
        $sync  = $this->syncGroup($probe, $join['memberId'], $join['generationId'], [$join['memberId'] => $empty]);

        self::assertSame(0, $sync['errorCode']);
        self::assertSame($empty->pack(), $sync['memberAssignment']);
        self::assertSame([], MemberAssignment::unpack($sync['memberAssignment'])->partitions());

        $this->leaveGroup($probe, $join['memberId']);
    }

    /**
     * Joins the group with a raw JoinGroup v0 frame and returns the fields of the response
     *
     * @return array{errorCode: int, generationId: int, groupProtocol: string, leaderId: string, memberId: string,
     *               members: array<string, string>}
     */
    private function joinGroup(RawApiProbe $probe, string $protocolName, Subscription $subscription): array
    {
        $body = RawApiProbe::string($this->groupId)
            . RawApiProbe::int32(self::SESSION_TIMEOUT_MS)
            . RawApiProbe::string('')
            . RawApiProbe::string('consumer')
            . RawApiProbe::int32(1)
            . RawApiProbe::string($protocolName)
            . RawApiProbe::bytes($subscription->pack());

        $reader = self::reader($probe->send(ApiKeys::JOIN_GROUP, 0, $body, 1));

        $response = [
            'errorCode'     => $reader('int16'),
            'generationId'  => $reader('int32'),
            'groupProtocol' => $reader('string'),
            'leaderId'      => $reader('string'),
            'memberId'      => $reader('string'),
            'members'       => [],
        ];
        $memberCount = $reader('int32');
        for ($member = 0; $member < $memberCount; $member++) {
            $response['members'][$reader('string')] = (string) $reader('bytes');
        }

        return $response;
    }

    /**
     * Publishes the assignment of the leader with a raw SyncGroup v0 frame and returns the fields of the response
     *
     * @param array<string, MemberAssignment> $assignments Assignment of every member, by its member id
     *
     * @return array{errorCode: int, memberAssignment: string}
     */
    private function syncGroup(RawApiProbe $probe, string $memberId, int $generationId, array $assignments): array
    {
        $body = RawApiProbe::string($this->groupId)
            . RawApiProbe::int32($generationId)
            . RawApiProbe::string($memberId)
            . RawApiProbe::int32(count($assignments));
        foreach ($assignments as $assignedMemberId => $assignment) {
            $body .= RawApiProbe::string((string) $assignedMemberId) . RawApiProbe::bytes($assignment->pack());
        }

        $reader = self::reader($probe->send(ApiKeys::SYNC_GROUP, 0, $body, 2));

        return ['errorCode' => $reader('int16'), 'memberAssignment' => (string) $reader('bytes')];
    }

    /**
     * Leaves the group with a raw LeaveGroup v0 frame, so that the next test does not wait for a session timeout
     */
    private function leaveGroup(RawApiProbe $probe, string $memberId): void
    {
        $body   = RawApiProbe::string($this->groupId) . RawApiProbe::string($memberId);
        $reader = self::reader($probe->send(ApiKeys::LEAVE_GROUP, 0, $body, 3));

        self::assertSame(0, $reader('int16'), 'The broker refused to let the member leave the group');
    }

    /**
     * Returns a reader over the body of an answered frame, one protocol primitive per call
     *
     * @param array{status: string, correlationId: int|null, body: string} $answer Result of {@see RawApiProbe::send()}
     *
     * @return \Closure(string): (int|string|null)
     */
    private static function reader(array $answer): \Closure
    {
        if ($answer['status'] !== RawApiProbe::ANSWERED) {
            throw new RuntimeException("The broker did not answer the request: {$answer['status']}");
        }

        $body     = $answer['body'];
        $position = 0;

        return static function (string $type) use ($body, &$position): int|string|null {
            switch ($type) {
                case 'int16':
                    $value = (int) unpack('n', substr($body, $position, 2))[1];
                    $position += 2;

                    return $value > 0x7FFF ? $value - 0x10000 : $value;

                case 'int32':
                    $value = (int) unpack('N', substr($body, $position, 4))[1];
                    $position += 4;

                    return $value > 0x7FFFFFFF ? $value - 0x100000000 : $value;

                case 'string':
                    $length = (int) unpack('n', substr($body, $position, 2))[1];
                    $position += 2;
                    $value = substr($body, $position, $length);
                    $position += $length;

                    return $value;

                case 'bytes':
                    $length = (int) unpack('N', substr($body, $position, 4))[1];
                    $position += 4;
                    if ($length === 0xFFFFFFFF) {
                        return null;
                    }
                    $value = substr($body, $position, $length);
                    $position += $length;

                    return $value;
            }

            throw new RuntimeException("Unknown primitive type {$type}");
        };
    }
}
