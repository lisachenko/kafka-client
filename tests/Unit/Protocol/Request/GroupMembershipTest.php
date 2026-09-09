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

namespace Protocol\Kafka\Tests\Unit\Protocol\Request;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\JoinGroupRequestProtocol;
use Protocol\Kafka\Protocol\Data\JoinGroupResponseMember;
use Protocol\Kafka\Protocol\Data\SyncGroupRequestMember;
use Protocol\Kafka\Protocol\Request\HeartbeatRequest;
use Protocol\Kafka\Protocol\Request\HeartbeatResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupRequestV0;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;

/**
 * Byte-exact tests for the four apis of the group membership protocol (keys 11 to 14).
 *
 * JoinGroup is the only one of them that Kafka 0.10 changed: its version 1 inserted the `RebalanceTimeout` after
 * the `SessionTimeout`, and SyncGroup, Heartbeat and LeaveGroup are still at version 0.
 *
 * The member metadata of JoinGroup and the assignments of SyncGroup are opaque byte arrays to these apis - the
 * coordinator never parses them - so every test here uses arbitrary bytes for them, including a NUL byte, and only
 * checks that they survive the round trip untouched.
 *
 * @see docs/protocol/0.10.2.md, sections "JoinGroup API (key 11, v0 and v1)", "SyncGroup API (key 14, v0)",
 *      "Heartbeat API (key 12, v0)" and "LeaveGroup API (key 13, v0)"
 */
#[CoversClass(JoinGroupRequest::class)]
#[CoversClass(JoinGroupRequestV0::class)]
#[CoversClass(JoinGroupResponse::class)]
#[CoversClass(SyncGroupRequest::class)]
#[CoversClass(SyncGroupResponse::class)]
#[CoversClass(HeartbeatRequest::class)]
#[CoversClass(HeartbeatResponse::class)]
#[CoversClass(LeaveGroupRequest::class)]
#[CoversClass(LeaveGroupResponse::class)]
#[CoversClass(JoinGroupRequestProtocol::class)]
#[CoversClass(JoinGroupResponseMember::class)]
#[CoversClass(SyncGroupRequestMember::class)]
final class GroupMembershipTest extends TestCase
{
    /**
     * Metadata of a member, two arbitrary bytes with the highest bit set in one of them
     */
    private const string METADATA = "\x00\xff";

    /**
     * Assignment of a member, three arbitrary bytes
     */
    private const string ASSIGNMENT = "\x01\x00\x02";

    /**
     * JoinGroup request v0 for the group "my-group", correlation id 1, client id "test".
     *
     *   Size             => 00 00 00 3d (61 bytes)
     *   ApiKey           => 00 0b
     *   ApiVersion       => 00 01
     *   CorrelationId    => 00 00 00 01
     *   ClientId         => 00 04 "test"
     *   GroupId          => 00 08 "my-group"
     *   SessionTimeout   => 00 00 75 30 (30000)
     *   RebalanceTimeout => 00 04 93 e0 (300000, the default of max.poll.interval.ms)
     *   MemberId         => 00 00 (empty: this client has none yet)
     *   ProtocolType     => 00 08 "consumer"
     *   GroupProtocols   => 00 00 00 01
     *     ProtocolName     => 00 05 "range"
     *     ProtocolMetadata => 00 00 00 02 00 ff
     */
    private const string JOIN_REQUEST_HEX = '0000003d'
        . '000b'
        . '0001'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00007530'
        . '000493e0'
        . '0000'
        . '0008' . '636f6e73756d6572'
        . '00000001'
        . '0005' . '72616e6765'
        . '00000002' . '00ff';

    /**
     * The same request as a version 0 frame: the RebalanceTimeout is gone and only the header version differs
     */
    private const string JOIN_REQUEST_V0_HEX = '00000039'
        . '000b'
        . '0000'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00007530'
        . '0000'
        . '0008' . '636f6e73756d6572'
        . '00000001'
        . '0005' . '72616e6765'
        . '00000002' . '00ff';

    /**
     * JoinGroup response v0 for the leader of a group of two members.
     *
     *   Size          => 00 00 00 3b (59 bytes)
     *   CorrelationId => 00 00 00 01
     *   ErrorCode     => 00 00
     *   GenerationId  => 00 00 00 02
     *   GroupProtocol => 00 05 "range"
     *   LeaderId      => 00 05 "one-1"
     *   MemberId      => 00 05 "one-1"
     *   Members       => 00 00 00 02
     *     "one-1" => 00 00 00 02 00 ff
     *     "two-2" => 00 00 00 00 (a member that sent no metadata at all)
     */
    private const string JOIN_RESPONSE_HEX = '0000003b'
        . '00000001'
        . '0000'
        . '00000002'
        . '0005' . '72616e6765'
        . '0005' . '6f6e652d31'
        . '0005' . '6f6e652d31'
        . '00000002'
        . '0005' . '6f6e652d31' . '00000002' . '00ff'
        . '0005' . '74776f2d32' . '00000000';

    /**
     * SyncGroup request v0 of the leader, which assigns three bytes to itself.
     *
     *   Size            => 00 00 00 35 (53 bytes)
     *   ApiKey          => 00 0e
     *   ApiVersion      => 00 00
     *   CorrelationId   => 00 00 00 02
     *   ClientId        => 00 04 "test"
     *   GroupId         => 00 08 "my-group"
     *   GenerationId    => 00 00 00 02
     *   MemberId        => 00 05 "one-1"
     *   GroupAssignment => 00 00 00 01
     *     MemberId         => 00 05 "one-1"
     *     MemberAssignment => 00 00 00 03 01 00 02
     */
    private const string SYNC_REQUEST_HEX = '00000035'
        . '000e'
        . '0000'
        . '00000002'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31'
        . '00000001'
        . '0005' . '6f6e652d31' . '00000003' . '010002';

    /**
     * SyncGroup response v0 with the assignment of the member, and without the ThrottleTimeMs of version 1.
     *
     *   Size             => 00 00 00 0d (13 bytes)
     *   CorrelationId    => 00 00 00 02
     *   ErrorCode        => 00 00
     *   MemberAssignment => 00 00 00 03 01 00 02
     */
    private const string SYNC_RESPONSE_HEX = '0000000d' . '00000002' . '0000' . '00000003' . '010002';

    /**
     * Heartbeat request v0 of the member "one-1" in the generation 2.
     *
     *   Size              => 00 00 00 23 (35 bytes)
     *   ApiKey            => 00 0c
     *   ApiVersion        => 00 00
     *   CorrelationId     => 00 00 00 03
     *   ClientId          => 00 04 "test"
     *   GroupId           => 00 08 "my-group"
     *   GroupGenerationId => 00 00 00 02
     *   MemberId          => 00 05 "one-1"
     */
    private const string HEARTBEAT_REQUEST_HEX = '00000023'
        . '000c'
        . '0000'
        . '00000003'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31';

    /**
     * LeaveGroup request v0 of the member "one-1".
     *
     *   Size          => 00 00 00 1f (31 bytes)
     *   ApiKey        => 00 0d
     *   ApiVersion    => 00 00
     *   CorrelationId => 00 00 00 04
     *   ClientId      => 00 04 "test"
     *   GroupId       => 00 08 "my-group"
     *   MemberId      => 00 05 "one-1"
     */
    private const string LEAVE_REQUEST_HEX = '0000001f'
        . '000d'
        . '0000'
        . '00000004'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '0005' . '6f6e652d31';

    public function testJoinGroupRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new JoinGroupRequest(
            'my-group',
            30000,
            300000,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            'consumer',
            ['range' => self::METADATA],
            'test',
            1
        );

        self::assertSame(self::JOIN_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::JOIN_GROUP, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion(), 'the RebalanceTimeout of Kafka 0.10.1 makes this version 1');
    }

    public function testJoinGroupRequestV0CarriesNoRebalanceTimeout(): void
    {
        $request = new JoinGroupRequestV0(
            'my-group',
            30000,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            'consumer',
            ['range' => self::METADATA],
            'test',
            1
        );

        self::assertSame(self::JOIN_REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());
        self::assertArrayNotHasKey(
            'rebalanceTimeout',
            JoinGroupRequestV0::getScheme(),
            'the field arrived with the version 1, and a 0.10.2.2 broker then uses the session timeout instead'
        );
        self::assertArrayHasKey('rebalanceTimeout', JoinGroupRequest::getScheme());
    }

    public function testJoinGroupRequestKeepsTheOrderOfTheOfferedProtocols(): void
    {
        $request = new JoinGroupRequest(
            'my-group',
            30000,
            300000,
            'one-1',
            'consumer',
            ['roundrobin' => 'first', 'range' => 'second'],
            'test',
            1
        );

        $frame = (string) $request;

        self::assertLessThan(
            strpos($frame, 'range'),
            strpos($frame, 'roundrobin'),
            'the coordinator picks the first protocol every member supports, so the order is meaningful'
        );
    }

    public function testJoinGroupRequestAcceptsAnAlreadyBuiltProtocol(): void
    {
        $request = new JoinGroupRequest(
            'my-group',
            30000,
            300000,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            'consumer',
            ['range' => new JoinGroupRequestProtocol('range', self::METADATA)],
            'test',
            1
        );

        self::assertSame(self::JOIN_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testJoinGroupResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = JoinGroupResponse::unpack(new StringStream((string) hex2bin(self::JOIN_RESPONSE_HEX)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertSame(2, $response->generationId);
        self::assertSame('range', $response->groupProtocol);
        self::assertSame('one-1', $response->leaderId);
        self::assertSame('one-1', $response->memberId);
        self::assertSame(['one-1', 'two-2'], array_keys($response->members));
        self::assertSame(self::METADATA, $response->members['one-1']->metadata);
        self::assertSame('', $response->members['two-2']->metadata, 'an empty byte array is not null');
    }

    /**
     * The answer to a member that is not the leader carries an empty member array, and that is the only way for it
     * to know that it does not have to compute the assignment of the generation
     */
    public function testJoinGroupResponseOfAFollowerCarriesNoMembers(): void
    {
        $frame = '00000023'
            . '00000001'
            . '0000'
            . '00000002'
            . '0005' . '72616e6765'
            . '0005' . '6f6e652d31'
            . '0005' . '74776f2d32'
            . '00000000';

        $response = JoinGroupResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame('one-1', $response->leaderId);
        self::assertSame('two-2', $response->memberId);
        self::assertSame([], $response->members);
    }

    /**
     * An error answer of the coordinator, byte for byte as a 0.9.0.1 broker sends it: the generation 0 - and not
     * the UNKNOWN_GENERATION_ID of -1 that the Java client uses - an empty protocol, an empty leader and the member
     * id that was sent back
     */
    public function testJoinGroupErrorResponseCarriesTheGenerationZeroAndNoLeader(): void
    {
        $frame = '00000014' . '00000001' . '001a' . '00000000' . '0000' . '0000' . '0000' . '00000000';

        $response = JoinGroupResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(26, $response->errorCode, 'the session timeout was outside the range of the broker');
        self::assertSame(0, $response->generationId);
        self::assertSame('', $response->groupProtocol);
        self::assertSame('', $response->leaderId);
        self::assertSame('', $response->memberId);
        self::assertSame([], $response->members);
    }

    public function testSyncGroupRequestOfTheLeaderIsPackedAccordingToTheSpec(): void
    {
        $request = new SyncGroupRequest('my-group', 2, 'one-1', ['one-1' => self::ASSIGNMENT], 'test', 2);

        self::assertSame(self::SYNC_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::SYNC_GROUP, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
    }

    public function testSyncGroupRequestAcceptsAnAlreadyBuiltMember(): void
    {
        $request = new SyncGroupRequest(
            'my-group',
            2,
            'one-1',
            ['one-1' => new SyncGroupRequestMember('one-1', self::ASSIGNMENT)],
            'test',
            2
        );

        self::assertSame(self::SYNC_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testSyncGroupRequestOfAFollowerCarriesAnEmptyAssignmentArray(): void
    {
        $request = new SyncGroupRequest('my-group', 2, 'two-2', [], 'test', 2);

        $expected = '00000027'
            . '000e'
            . '0000'
            . '00000002'
            . '0004' . '74657374'
            . '0008' . '6d792d67726f7570'
            . '00000002'
            . '0005' . '74776f2d32'
            . '00000000';

        self::assertSame($expected, bin2hex((string) $request));
    }

    public function testSyncGroupResponseIsUnpackedWithoutAThrottleTime(): void
    {
        $response = SyncGroupResponse::unpack(new StringStream((string) hex2bin(self::SYNC_RESPONSE_HEX)));

        self::assertSame(2, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertSame(self::ASSIGNMENT, $response->memberAssignment);
    }

    /**
     * A member the leader did not mention is given an empty assignment by the coordinator, and so is every member
     * of an answer that carries an error code
     */
    public function testSyncGroupResponseWithoutAnAssignmentIsUnpackedAsAnEmptyString(): void
    {
        $response = SyncGroupResponse::unpack(
            new StringStream((string) hex2bin('0000000a' . '00000002' . '0016' . '00000000'))
        );

        self::assertSame(22, $response->errorCode);
        self::assertSame('', $response->memberAssignment);
    }

    public function testHeartbeatRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new HeartbeatRequest('my-group', 2, 'one-1', 'test', 3);

        self::assertSame(self::HEARTBEAT_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::HEARTBEAT, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
    }

    public function testHeartbeatResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = HeartbeatResponse::unpack(
            new StringStream((string) hex2bin('00000006' . '00000003' . '001b'))
        );

        self::assertSame(3, $response->getCorrelationId());
        self::assertSame(27, $response->errorCode, 'the group is rebalancing, the member has to rejoin');
    }

    public function testLeaveGroupRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new LeaveGroupRequest('my-group', 'one-1', 'test', 4);

        self::assertSame(self::LEAVE_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::LEAVE_GROUP, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
    }

    public function testLeaveGroupResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = LeaveGroupResponse::unpack(
            new StringStream((string) hex2bin('00000006' . '00000004' . '0019'))
        );

        self::assertSame(4, $response->getCorrelationId());
        self::assertSame(25, $response->errorCode, 'the coordinator does not know this member');
    }
}
