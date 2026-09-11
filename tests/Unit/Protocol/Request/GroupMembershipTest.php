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
use Protocol\Kafka\Protocol\Data\JoinGroupResponseMemberV0;
use Protocol\Kafka\Protocol\Data\LeaveGroupRequestMember;
use Protocol\Kafka\Protocol\Data\LeaveGroupResponseMember;
use Protocol\Kafka\Protocol\Data\SyncGroupRequestMember;
use Protocol\Kafka\Protocol\Request\HeartbeatRequest;
use Protocol\Kafka\Protocol\Request\HeartbeatRequestV0;
use Protocol\Kafka\Protocol\Request\HeartbeatRequestV1;
use Protocol\Kafka\Protocol\Request\HeartbeatRequestV2;
use Protocol\Kafka\Protocol\Request\HeartbeatResponse;
use Protocol\Kafka\Protocol\Request\HeartbeatResponseV0;
use Protocol\Kafka\Protocol\Request\HeartbeatResponseV1;
use Protocol\Kafka\Protocol\Request\HeartbeatResponseV2;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupRequestV0;
use Protocol\Kafka\Protocol\Request\JoinGroupRequestV1;
use Protocol\Kafka\Protocol\Request\JoinGroupRequestV2;
use Protocol\Kafka\Protocol\Request\JoinGroupRequestV3;
use Protocol\Kafka\Protocol\Request\JoinGroupRequestV4;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupResponseV0;
use Protocol\Kafka\Protocol\Request\JoinGroupResponseV1;
use Protocol\Kafka\Protocol\Request\JoinGroupResponseV2;
use Protocol\Kafka\Protocol\Request\JoinGroupResponseV3;
use Protocol\Kafka\Protocol\Request\JoinGroupResponseV4;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequestV0;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequestV1;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequestV2;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponseV0;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponseV1;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponseV2;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupRequestV0;
use Protocol\Kafka\Protocol\Request\SyncGroupRequestV1;
use Protocol\Kafka\Protocol\Request\SyncGroupRequestV2;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;
use Protocol\Kafka\Protocol\Request\SyncGroupResponseV0;
use Protocol\Kafka\Protocol\Request\SyncGroupResponseV1;
use Protocol\Kafka\Protocol\Request\SyncGroupResponseV2;

/**
 * Byte-exact tests for the four apis of the group membership protocol (keys 11 to 14).
 *
 * Three releases changed them. Kafka 0.10.1 inserted the `RebalanceTimeout` after the `SessionTimeout` of
 * JoinGroup, which is its version 1; Kafka 0.11 added the leading `ThrottleTimeMs` to the ANSWER of all four
 * (KIP-124), which is JoinGroup v2 and SyncGroup, Heartbeat and LeaveGroup v1; and Kafka 2.0 raised every one of
 * them once more without touching a single field (KIP-219), which is JoinGroup v3 and SyncGroup, Heartbeat and
 * LeaveGroup v2. Kafka 2.2 raised JoinGroup once more, to **v4** (KIP-394), again without changing a field: what
 * that version changes is what an EMPTY member id means, see the integration suite. A lower-version frame
 * therefore differs from the current one in the version field of its header only.
 *
 * The member metadata of JoinGroup and the assignments of SyncGroup are opaque byte arrays to these apis - the
 * coordinator never parses them here - so every test in this class uses arbitrary bytes for them, including a NUL
 * byte, and only checks that they survive the round trip untouched. A real `consumer` group is a different matter:
 * a 2.x coordinator does parse the metadata of such a group, see the integration suite.
 *
 * @see docs/protocol/2.8.md, sections "JoinGroup API (key 11, v0 to v5)", "SyncGroup API (key 14, v0 to v3)",
 *      "Heartbeat API (key 12, v0 to v3)" and "LeaveGroup API (key 13, v0 to v3)"
 */
#[CoversClass(JoinGroupRequest::class)]
#[CoversClass(JoinGroupRequestV0::class)]
#[CoversClass(JoinGroupRequestV1::class)]
#[CoversClass(JoinGroupRequestV2::class)]
#[CoversClass(JoinGroupRequestV3::class)]
#[CoversClass(JoinGroupRequestV4::class)]
#[CoversClass(JoinGroupResponse::class)]
#[CoversClass(JoinGroupResponseV0::class)]
#[CoversClass(JoinGroupResponseV1::class)]
#[CoversClass(JoinGroupResponseV2::class)]
#[CoversClass(JoinGroupResponseV3::class)]
#[CoversClass(JoinGroupResponseV4::class)]
#[CoversClass(JoinGroupResponseMemberV0::class)]
#[CoversClass(SyncGroupRequest::class)]
#[CoversClass(SyncGroupRequestV0::class)]
#[CoversClass(SyncGroupRequestV1::class)]
#[CoversClass(SyncGroupRequestV2::class)]
#[CoversClass(SyncGroupResponse::class)]
#[CoversClass(SyncGroupResponseV0::class)]
#[CoversClass(SyncGroupResponseV1::class)]
#[CoversClass(SyncGroupResponseV2::class)]
#[CoversClass(HeartbeatRequest::class)]
#[CoversClass(HeartbeatRequestV0::class)]
#[CoversClass(HeartbeatRequestV1::class)]
#[CoversClass(HeartbeatRequestV2::class)]
#[CoversClass(HeartbeatResponse::class)]
#[CoversClass(HeartbeatResponseV0::class)]
#[CoversClass(HeartbeatResponseV1::class)]
#[CoversClass(HeartbeatResponseV2::class)]
#[CoversClass(LeaveGroupRequest::class)]
#[CoversClass(LeaveGroupRequestV0::class)]
#[CoversClass(LeaveGroupRequestV1::class)]
#[CoversClass(LeaveGroupRequestV2::class)]
#[CoversClass(LeaveGroupRequestMember::class)]
#[CoversClass(LeaveGroupResponse::class)]
#[CoversClass(LeaveGroupResponseV0::class)]
#[CoversClass(LeaveGroupResponseV1::class)]
#[CoversClass(LeaveGroupResponseV2::class)]
#[CoversClass(LeaveGroupResponseMember::class)]
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
     * JoinGroup request v5 of a DYNAMIC member for the group "my-group", correlation id 1, client id "test".
     *
     *   Size             => 00 00 00 3f (63 bytes)
     *   ApiKey           => 00 0b
     *   ApiVersion       => 00 05
     *   CorrelationId    => 00 00 00 01
     *   ClientId         => 00 04 "test"
     *   GroupId          => 00 08 "my-group"
     *   SessionTimeout   => 00 00 75 30 (30000)
     *   RebalanceTimeout => 00 04 93 e0 (300000, the default of max.poll.interval.ms)
     *   MemberId         => 00 00 (empty: this client has none yet)
     *   GroupInstanceId  => ff ff (null: a dynamic member, KIP-345)
     *   ProtocolType     => 00 08 "consumer"
     *   GroupProtocols   => 00 00 00 01
     *     ProtocolName     => 00 05 "range"
     *     ProtocolMetadata => 00 00 00 02 00 ff
     */
    private const string JOIN_REQUEST_HEX = '0000003f'
        . '000b'
        . '0005'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00007530'
        . '000493e0'
        . '0000'
        . 'ffff'
        . '0008' . '636f6e73756d6572'
        . '00000001'
        . '0005' . '72616e6765'
        . '00000002' . '00ff';

    /**
     * The same request of a STATIC member, whose `group_instance_id` "one" stands where the null was
     */
    private const string JOIN_REQUEST_STATIC_HEX = '00000042'
        . '000b'
        . '0005'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00007530'
        . '000493e0'
        . '0000'
        . '0003' . '6f6e65'
        . '0008' . '636f6e73756d6572'
        . '00000001'
        . '0005' . '72616e6765'
        . '00000002' . '00ff';

    /**
     * The same request as a version 4 frame, which has no `group_instance_id` at all
     */
    private const string JOIN_REQUEST_V4_HEX = '0000003d'
        . '000b'
        . '0004'
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
     * The same request as a version 3 frame, which is the same body with the version field 3
     */
    private const string JOIN_REQUEST_V3_HEX = '0000003d'
        . '000b'
        . '0003'
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
     * The same request as a version 2 frame, which is the same body with the version field 2
     */
    private const string JOIN_REQUEST_V2_HEX = '0000003d'
        . '000b'
        . '0002'
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
     * The same request as a version 1 frame, which is the same body with the version field 1
     */
    private const string JOIN_REQUEST_V1_HEX = '0000003d'
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
     * JoinGroup response v0/v1 for the leader of a group of two members.
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
     * The same answer as version 2, which version 3 repeats: four bytes longer, the throttle time opens the body
     */
    private const string JOIN_RESPONSE_V2_HEX = '0000003f'
        . '00000001'
        . '00000000'
        . '0000'
        . '00000002'
        . '0005' . '72616e6765'
        . '0005' . '6f6e652d31'
        . '0005' . '6f6e652d31'
        . '00000002'
        . '0005' . '6f6e652d31' . '00000002' . '00ff'
        . '0005' . '74776f2d32' . '00000000';

    /**
     * The same answer as version 5, whose member entries carry the `group_instance_id` of KIP-345: "one-1" is a
     * static member of the instance "one", "two-2" a dynamic one (ff ff)
     */
    private const string JOIN_RESPONSE_V5_HEX = '00000046'
        . '00000001'
        . '00000000'
        . '0000'
        . '00000002'
        . '0005' . '72616e6765'
        . '0005' . '6f6e652d31'
        . '0005' . '6f6e652d31'
        . '00000002'
        . '0005' . '6f6e652d31' . '0003' . '6f6e65' . '00000002' . '00ff'
        . '0005' . '74776f2d32' . 'ffff' . '00000000';

    /**
     * SyncGroup request v3 of the leader, which assigns three bytes to itself.
     *
     *   Size            => 00 00 00 37 (55 bytes)
     *   ApiKey          => 00 0e
     *   ApiVersion      => 00 03
     *   CorrelationId   => 00 00 00 02
     *   ClientId        => 00 04 "test"
     *   GroupId         => 00 08 "my-group"
     *   GenerationId    => 00 00 00 02
     *   MemberId        => 00 05 "one-1"
     *   GroupInstanceId => ff ff (null: a dynamic member, KIP-345)
     *   GroupAssignment => 00 00 00 01
     *     MemberId         => 00 05 "one-1"
     *     MemberAssignment => 00 00 00 03 01 00 02
     */
    private const string SYNC_REQUEST_HEX = '00000037'
        . '000e'
        . '0003'
        . '00000002'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31'
        . 'ffff'
        . '00000001'
        . '0005' . '6f6e652d31' . '00000003' . '010002';

    /**
     * The same request of a static member, whose instance id "one" stands where the null was
     */
    private const string SYNC_REQUEST_STATIC_HEX = '0000003a'
        . '000e'
        . '0003'
        . '00000002'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31'
        . '0003' . '6f6e65'
        . '00000001'
        . '0005' . '6f6e652d31' . '00000003' . '010002';

    /**
     * The same request as a version 2 frame, which has no `group_instance_id` at all
     */
    private const string SYNC_REQUEST_V2_HEX = '00000035'
        . '000e'
        . '0002'
        . '00000002'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31'
        . '00000001'
        . '0005' . '6f6e652d31' . '00000003' . '010002';

    /**
     * The very same body as a version 1 frame
     */
    private const string SYNC_REQUEST_V1_HEX = '00000035'
        . '000e'
        . '0001'
        . '00000002'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31'
        . '00000001'
        . '0005' . '6f6e652d31' . '00000003' . '010002';

    /**
     * The very same body as a version 0 frame
     */
    private const string SYNC_REQUEST_V0_HEX = '00000035'
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
    private const string SYNC_RESPONSE_V0_HEX = '0000000d' . '00000002' . '0000' . '00000003' . '010002';

    /**
     * The same answer as version 1, which version 2 repeats, with the throttle time in front of the error code
     */
    private const string SYNC_RESPONSE_HEX = '00000011' . '00000002' . '00000000' . '0000' . '00000003' . '010002';

    /**
     * Heartbeat request v3 of the member "one-1" in the generation 2.
     *
     *   Size              => 00 00 00 25 (37 bytes)
     *   ApiKey            => 00 0c
     *   ApiVersion        => 00 03
     *   CorrelationId     => 00 00 00 03
     *   ClientId          => 00 04 "test"
     *   GroupId           => 00 08 "my-group"
     *   GroupGenerationId => 00 00 00 02
     *   MemberId          => 00 05 "one-1"
     *   GroupInstanceId   => ff ff (null: a dynamic member, KIP-345)
     */
    private const string HEARTBEAT_REQUEST_HEX = '00000025'
        . '000c'
        . '0003'
        . '00000003'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31'
        . 'ffff';

    /**
     * The same heartbeat of a static member, whose instance id "one" closes the frame
     */
    private const string HEARTBEAT_REQUEST_STATIC_HEX = '00000028'
        . '000c'
        . '0003'
        . '00000003'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31'
        . '0003' . '6f6e65';

    /**
     * The same request as a version 2 frame, which has no `group_instance_id` at all
     */
    private const string HEARTBEAT_REQUEST_V2_HEX = '00000023'
        . '000c'
        . '0002'
        . '00000003'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31';

    /**
     * The very same body as a version 1 frame
     */
    private const string HEARTBEAT_REQUEST_V1_HEX = '00000023'
        . '000c'
        . '0001'
        . '00000003'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31';

    /**
     * The very same body as a version 0 frame
     */
    private const string HEARTBEAT_REQUEST_V0_HEX = '00000023'
        . '000c'
        . '0000'
        . '00000003'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31';

    /**
     * LeaveGroup request v3 of the member "one-1", which removes itself with a batch of one entry.
     *
     *   Size            => 00 00 00 25 (37 bytes)
     *   ApiKey          => 00 0d
     *   ApiVersion      => 00 03
     *   CorrelationId   => 00 00 00 04
     *   ClientId        => 00 04 "test"
     *   GroupId         => 00 08 "my-group"
     *   Members         => 00 00 00 01
     *     MemberId        => 00 05 "one-1"
     *     GroupInstanceId => ff ff (null: a dynamic member)
     */
    private const string LEAVE_REQUEST_HEX = '00000025'
        . '000d'
        . '0003'
        . '00000004'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000001'
        . '0005' . '6f6e652d31'
        . 'ffff';

    /**
     * The batch that removes two members at once: the static instance "two", named by its instance id alone, and
     * the member "one-1" by its member id
     */
    private const string LEAVE_REQUEST_BATCH_HEX = '0000002c'
        . '000d'
        . '0003'
        . '00000004'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00000002'
        . '0005' . '6f6e652d31' . 'ffff'
        . '0000' . '0003' . '74776f';

    /**
     * The same single member as a version 2 frame, whose `member_id` is the whole body
     */
    private const string LEAVE_REQUEST_V2_HEX = '0000001f'
        . '000d'
        . '0002'
        . '00000004'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '0005' . '6f6e652d31';

    /**
     * The very same body as a version 1 frame
     */
    private const string LEAVE_REQUEST_V1_HEX = '0000001f'
        . '000d'
        . '0001'
        . '00000004'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '0005' . '6f6e652d31';

    /**
     * The very same body as a version 0 frame
     */
    private const string LEAVE_REQUEST_V0_HEX = '0000001f'
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
        self::assertSame(5, $request->getApiVersion(), 'KIP-345 makes the version this client sends 5');
    }

    public function testAStaticMemberWritesItsInstanceIdWhereTheNullOfADynamicOneStands(): void
    {
        $request = new JoinGroupRequest(
            'my-group',
            30000,
            300000,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            'consumer',
            ['range' => self::METADATA],
            'test',
            1,
            'one'
        );

        self::assertSame(self::JOIN_REQUEST_STATIC_HEX, bin2hex((string) $request));
        self::assertSame(
            strlen(self::JOIN_REQUEST_HEX) + 2 * 3,
            strlen(self::JOIN_REQUEST_STATIC_HEX),
            'the three characters of the instance id replace nothing but the two bytes of the null'
        );
    }

    public function testJoinGroupRequestV4CarriesNoGroupInstanceId(): void
    {
        $request = new JoinGroupRequestV4(
            'my-group',
            30000,
            300000,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            'consumer',
            ['range' => self::METADATA],
            'test',
            1,
            'one'
        );

        self::assertSame(self::JOIN_REQUEST_V4_HEX, bin2hex((string) $request));
        self::assertSame(4, $request->getApiVersion());
        self::assertArrayNotHasKey(
            'groupInstanceId',
            JoinGroupRequestV4::getScheme(),
            'the field arrived with version 5 (KIP-345), and an instance id is simply not sent below it'
        );
        self::assertArrayHasKey('groupInstanceId', JoinGroupRequest::getScheme());
    }

    public function testJoinGroupRequestV3SendsTheSameBodyAsVersionFour(): void
    {
        $request = new JoinGroupRequestV3(
            'my-group',
            30000,
            300000,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            'consumer',
            ['range' => self::METADATA],
            'test',
            1
        );

        self::assertSame(self::JOIN_REQUEST_V3_HEX, bin2hex((string) $request));
        self::assertSame(3, $request->getApiVersion());
        self::assertSame(
            substr(self::JOIN_REQUEST_V4_HEX, 16),
            substr(self::JOIN_REQUEST_V3_HEX, 16),
            'KIP-394 changed what an EMPTY member id means, not a single byte of the frame'
        );
    }

    public function testJoinGroupRequestV2SendsTheSameBodyAsVersionThree(): void
    {
        $request = new JoinGroupRequestV2(
            'my-group',
            30000,
            300000,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            'consumer',
            ['range' => self::METADATA],
            'test',
            1
        );

        self::assertSame(self::JOIN_REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(2, $request->getApiVersion());
        self::assertSame(
            substr(self::JOIN_REQUEST_V4_HEX, 16),
            substr(self::JOIN_REQUEST_V2_HEX, 16),
            'KIP-219 raised the api version of JoinGroup without adding a field'
        );
    }

    public function testJoinGroupRequestV1SendsTheSameBodyAsTheVersionsAboveIt(): void
    {
        $request = new JoinGroupRequestV1(
            'my-group',
            30000,
            300000,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            'consumer',
            ['range' => self::METADATA],
            'test',
            1
        );

        self::assertSame(self::JOIN_REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
        self::assertSame(
            substr(self::JOIN_REQUEST_V4_HEX, 16),
            substr(self::JOIN_REQUEST_V1_HEX, 16),
            'JOIN_GROUP_REQUEST_V3 = JOIN_GROUP_REQUEST_V2 = JOIN_GROUP_REQUEST_V1: only the version differs'
        );
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
            'the field arrived with the version 1, and a 0.11.0.3 broker then uses the session timeout instead'
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
        $response = JoinGroupResponseV1::unpack(new StringStream((string) hex2bin(self::JOIN_RESPONSE_HEX)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertSame(2, $response->generationId);
        self::assertSame('range', $response->groupProtocol);
        self::assertSame('one-1', $response->leaderId);
        self::assertSame('one-1', $response->memberId);
        self::assertSame(['one-1', 'two-2'], array_keys($response->members));
        self::assertSame(self::METADATA, $response->members['one-1']->metadata);
        self::assertSame('', $response->members['two-2']->metadata, 'an empty byte array is not null');
        self::assertSame(0, $response->throttleTimeMs, 'the field is not on the wire below version 2');
    }

    public function testTheVersionZeroAndVersionOneAnswersAreTheSameBytes(): void
    {
        $frame = (string) hex2bin(self::JOIN_RESPONSE_HEX);

        $versionZero = JoinGroupResponseV0::unpack(new StringStream($frame));
        $versionOne  = JoinGroupResponseV1::unpack(new StringStream($frame));

        self::assertSame(bin2hex((string) $versionZero), bin2hex((string) $versionOne));
        self::assertSame($versionZero->memberId, $versionOne->memberId);
    }

    public function testJoinGroupResponseOfVersionTwoAndThreeStartsWithTheThrottleTime(): void
    {
        foreach ([JoinGroupResponseV2::class, JoinGroupResponseV3::class, JoinGroupResponseV4::class] as $class) {
            $response = $class::unpack(new StringStream((string) hex2bin(self::JOIN_RESPONSE_V2_HEX)));

            self::assertSame(0, $response->throttleTimeMs);
            self::assertSame(0, $response->errorCode);
            self::assertSame(2, $response->generationId);
            self::assertSame(['one-1', 'two-2'], array_keys($response->members));
            self::assertSame(
                self::JOIN_RESPONSE_V2_HEX,
                bin2hex((string) $response),
                'the answer survives a decode and encode round trip'
            );
        }
    }

    public function testTheVersionFiveAnswerGivesEveryMemberAGroupInstanceId(): void
    {
        $response = JoinGroupResponse::unpack(new StringStream((string) hex2bin(self::JOIN_RESPONSE_V5_HEX)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['one-1', 'two-2'], array_keys($response->members));
        self::assertSame('one', $response->members['one-1']->groupInstanceId, 'a static member names its instance');
        self::assertNull($response->members['two-2']->groupInstanceId, 'a dynamic member sends ff ff');
        self::assertSame(
            self::JOIN_RESPONSE_V5_HEX,
            bin2hex((string) $response),
            'the answer survives a decode and encode round trip'
        );
    }

    public function testTheMemberEntryOfVersionFourHasNoInstanceIdAtAll(): void
    {
        $response = JoinGroupResponseV4::unpack(new StringStream((string) hex2bin(self::JOIN_RESPONSE_V2_HEX)));

        self::assertNull(
            $response->members['one-1']->groupInstanceId,
            'the property stays at its default, the versions below 5 have no such field'
        );
        self::assertArrayNotHasKey('groupInstanceId', JoinGroupResponseMemberV0::getScheme());
        self::assertArrayHasKey('groupInstanceId', JoinGroupResponseMember::getScheme());
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

        $response = JoinGroupResponseV1::unpack(new StringStream((string) hex2bin($frame)));

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

        $response = JoinGroupResponseV0::unpack(new StringStream((string) hex2bin($frame)));

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
        self::assertSame(3, $request->getApiVersion(), 'KIP-345 makes the version this client sends 3');
    }

    public function testASyncOfAStaticMemberCarriesItsInstanceIdBehindTheMemberId(): void
    {
        $request = new SyncGroupRequest('my-group', 2, 'one-1', ['one-1' => self::ASSIGNMENT], 'test', 2, 'one');

        self::assertSame(self::SYNC_REQUEST_STATIC_HEX, bin2hex((string) $request));
    }

    public function testSyncGroupRequestV2CarriesNoGroupInstanceId(): void
    {
        $request = new SyncGroupRequestV2('my-group', 2, 'one-1', ['one-1' => self::ASSIGNMENT], 'test', 2, 'one');

        self::assertSame(self::SYNC_REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(2, $request->getApiVersion());
        self::assertArrayNotHasKey('groupInstanceId', SyncGroupRequestV2::getScheme());
        self::assertArrayHasKey('groupInstanceId', SyncGroupRequest::getScheme());
    }

    public function testSyncGroupRequestV1SendsTheSameBodyAsVersionTwo(): void
    {
        $request = new SyncGroupRequestV1('my-group', 2, 'one-1', ['one-1' => self::ASSIGNMENT], 'test', 2);

        self::assertSame(self::SYNC_REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
        self::assertSame(
            substr(self::SYNC_REQUEST_V2_HEX, 16),
            substr(self::SYNC_REQUEST_V1_HEX, 16),
            'KIP-219 raised the api version of SyncGroup without adding a field'
        );
    }

    public function testSyncGroupRequestV0SendsTheSameBodyAsVersionOne(): void
    {
        $request = new SyncGroupRequestV0('my-group', 2, 'one-1', ['one-1' => self::ASSIGNMENT], 'test', 2);

        self::assertSame(self::SYNC_REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(
            substr(self::SYNC_REQUEST_V2_HEX, 16),
            substr(self::SYNC_REQUEST_V0_HEX, 16),
            'SYNC_GROUP_REQUEST_V2 = SYNC_GROUP_REQUEST_V1 = SYNC_GROUP_REQUEST_V0'
        );
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

        $expected = '00000029'
            . '000e'
            . '0003'
            . '00000002'
            . '0004' . '74657374'
            . '0008' . '6d792d67726f7570'
            . '00000002'
            . '0005' . '74776f2d32'
            . 'ffff'
            . '00000000';

        self::assertSame($expected, bin2hex((string) $request));
    }

    public function testSyncGroupResponseOfVersionZeroIsUnpackedWithoutAThrottleTime(): void
    {
        $response = SyncGroupResponseV0::unpack(new StringStream((string) hex2bin(self::SYNC_RESPONSE_V0_HEX)));

        self::assertSame(2, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertSame(self::ASSIGNMENT, $response->memberAssignment);
        self::assertSame(0, $response->throttleTimeMs);
    }

    public function testSyncGroupResponseOfVersionOneAndTwoStartsWithTheThrottleTime(): void
    {
        foreach ([SyncGroupResponseV1::class, SyncGroupResponseV2::class, SyncGroupResponse::class] as $class) {
            $response = $class::unpack(new StringStream((string) hex2bin(self::SYNC_RESPONSE_HEX)));

            self::assertSame(2, $response->getCorrelationId());
            self::assertSame(0, $response->throttleTimeMs);
            self::assertSame(0, $response->errorCode);
            self::assertSame(self::ASSIGNMENT, $response->memberAssignment);
            self::assertSame(self::SYNC_RESPONSE_HEX, bin2hex((string) $response));
        }
    }

    /**
     * A member the leader did not mention is given an empty assignment by the coordinator, and so is every member
     * of an answer that carries an error code
     */
    public function testSyncGroupResponseWithoutAnAssignmentIsUnpackedAsAnEmptyString(): void
    {
        $response = SyncGroupResponseV0::unpack(
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
        self::assertSame(3, $request->getApiVersion(), 'KIP-345 makes the version this client sends 3');
    }

    public function testAHeartbeatOfAStaticMemberEndsWithItsInstanceId(): void
    {
        $request = new HeartbeatRequest('my-group', 2, 'one-1', 'test', 3, 'one');

        self::assertSame(self::HEARTBEAT_REQUEST_STATIC_HEX, bin2hex((string) $request));
    }

    public function testHeartbeatRequestV2CarriesNoGroupInstanceId(): void
    {
        $request = new HeartbeatRequestV2('my-group', 2, 'one-1', 'test', 3, 'one');

        self::assertSame(self::HEARTBEAT_REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(2, $request->getApiVersion());
        self::assertArrayNotHasKey('groupInstanceId', HeartbeatRequestV2::getScheme());
        self::assertArrayHasKey('groupInstanceId', HeartbeatRequest::getScheme());
    }

    public function testHeartbeatRequestV1SendsTheSameBodyAsVersionTwo(): void
    {
        $request = new HeartbeatRequestV1('my-group', 2, 'one-1', 'test', 3);

        self::assertSame(self::HEARTBEAT_REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
        self::assertSame(
            substr(self::HEARTBEAT_REQUEST_V2_HEX, 16),
            substr(self::HEARTBEAT_REQUEST_V1_HEX, 16),
            'KIP-219 raised the api version of Heartbeat without adding a field'
        );
    }

    public function testHeartbeatRequestV0SendsTheSameBodyAsVersionOne(): void
    {
        $request = new HeartbeatRequestV0('my-group', 2, 'one-1', 'test', 3);

        self::assertSame(self::HEARTBEAT_REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(
            substr(self::HEARTBEAT_REQUEST_V2_HEX, 16),
            substr(self::HEARTBEAT_REQUEST_V0_HEX, 16),
            'HEARTBEAT_REQUEST_V2 = HEARTBEAT_REQUEST_V1 = HEARTBEAT_REQUEST_V0'
        );
    }

    public function testHeartbeatResponseOfVersionZeroIsTheErrorCodeAlone(): void
    {
        $response = HeartbeatResponseV0::unpack(
            new StringStream((string) hex2bin('00000006' . '00000003' . '001b'))
        );

        self::assertSame(3, $response->getCorrelationId());
        self::assertSame(27, $response->errorCode, 'the group is rebalancing, the member has to rejoin');
        self::assertSame(0, $response->throttleTimeMs);
    }

    public function testHeartbeatResponseOfVersionOneAndTwoStartsWithTheThrottleTime(): void
    {
        $frame = '0000000a' . '00000003' . '00000000' . '001b';

        foreach ([HeartbeatResponseV1::class, HeartbeatResponse::class] as $class) {
            $response = $class::unpack(new StringStream((string) hex2bin($frame)));

            self::assertSame(3, $response->getCorrelationId());
            self::assertSame(0, $response->throttleTimeMs);
            self::assertSame(27, $response->errorCode);
            self::assertSame($frame, bin2hex((string) $response));
        }
    }

    public function testLeaveGroupRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new LeaveGroupRequest('my-group', 'one-1', 'test', 4);

        self::assertSame(self::LEAVE_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::LEAVE_GROUP, $request->getApiKey());
        self::assertSame(3, $request->getApiVersion(), 'KIP-345 makes the version this client sends 3');
        self::assertCount(1, $request->getMembers(), 'a member that removes itself is a batch of one');
    }

    public function testTheVersionThreeRequestRemovesSeveralMembersAtOnce(): void
    {
        $request = new LeaveGroupRequest(
            'my-group',
            [
                new LeaveGroupRequestMember('one-1'),
                new LeaveGroupRequestMember(LeaveGroupRequestMember::UNKNOWN_MEMBER_ID, 'two'),
            ],
            'test',
            4
        );

        self::assertSame(self::LEAVE_REQUEST_BATCH_HEX, bin2hex((string) $request));
        self::assertCount(2, $request->getMembers());
        self::assertSame('two', $request->getMembers()[1]->groupInstanceId, 'the second entry names an instance');
    }

    public function testTheVersionTwoRequestCarriesTheFirstMemberIdAndNoBatch(): void
    {
        $request = new LeaveGroupRequestV2('my-group', 'one-1', 'test', 4);

        self::assertSame(self::LEAVE_REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(2, $request->getApiVersion());
        self::assertArrayNotHasKey('members', LeaveGroupRequestV2::getScheme());
        self::assertArrayHasKey('memberId', LeaveGroupRequestV2::getScheme());
        self::assertArrayHasKey('members', LeaveGroupRequest::getScheme());
        self::assertArrayNotHasKey(
            'memberId',
            LeaveGroupRequest::getScheme(),
            'KIP-345 replaced the field, it did not add one'
        );
    }

    public function testLeaveGroupRequestV1SendsTheSameBodyAsVersionTwo(): void
    {
        $request = new LeaveGroupRequestV1('my-group', 'one-1', 'test', 4);

        self::assertSame(self::LEAVE_REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
        self::assertSame(
            substr(self::LEAVE_REQUEST_V2_HEX, 16),
            substr(self::LEAVE_REQUEST_V1_HEX, 16),
            'KIP-219 raised the api version of LeaveGroup without adding a field'
        );
    }

    public function testLeaveGroupRequestV0SendsTheSameBodyAsVersionOne(): void
    {
        $request = new LeaveGroupRequestV0('my-group', 'one-1', 'test', 4);

        self::assertSame(self::LEAVE_REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(
            substr(self::LEAVE_REQUEST_V2_HEX, 16),
            substr(self::LEAVE_REQUEST_V0_HEX, 16),
            'LEAVE_GROUP_REQUEST_V2 = LEAVE_GROUP_REQUEST_V1 = LEAVE_GROUP_REQUEST_V0'
        );
    }

    public function testLeaveGroupResponseOfVersionZeroIsTheErrorCodeAlone(): void
    {
        $response = LeaveGroupResponseV0::unpack(
            new StringStream((string) hex2bin('00000006' . '00000004' . '0019'))
        );

        self::assertSame(4, $response->getCorrelationId());
        self::assertSame(25, $response->errorCode, 'the coordinator does not know this member');
        self::assertSame(0, $response->throttleTimeMs);
    }

    public function testLeaveGroupResponseOfVersionOneAndTwoStartsWithTheThrottleTime(): void
    {
        $frame = '0000000a' . '00000004' . '00000000' . '0019';

        foreach ([LeaveGroupResponseV1::class, LeaveGroupResponseV2::class] as $class) {
            $response = $class::unpack(new StringStream((string) hex2bin($frame)));

            self::assertSame(0, $response->throttleTimeMs);
            self::assertSame(25, $response->errorCode);
            self::assertSame([], $response->members, 'the versions below 3 have no member array at all');
            self::assertSame($frame, bin2hex((string) $response));
        }
    }

    /**
     * The version 3 answer carries the error of every member of the batch, and keeps the top-level code at 0
     */
    public function testLeaveGroupResponseOfVersionThreeCarriesOneEntryPerMember(): void
    {
        $frame = '00000022'
            . '00000004'
            . '00000000'
            . '0000'
            . '00000002'
            . '0005' . '6f6e652d31' . 'ffff' . '0000'
            . '0000' . '0003' . '74776f' . '0019';

        $response = LeaveGroupResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(0, $response->errorCode, 'a member that was refused does not fail the request');
        self::assertCount(2, $response->members);
        self::assertSame('one-1', $response->members[0]->memberId);
        self::assertNull($response->members[0]->groupInstanceId);
        self::assertSame(0, $response->members[0]->errorCode, 'the dynamic member left');
        self::assertSame('', $response->members[1]->memberId, 'the second entry was named by its instance alone');
        self::assertSame('two', $response->members[1]->groupInstanceId);
        self::assertSame(25, $response->members[1]->errorCode, 'and the group does not have that instance');
        self::assertSame($frame, bin2hex((string) $response), 'the answer survives the round trip');
    }
}
