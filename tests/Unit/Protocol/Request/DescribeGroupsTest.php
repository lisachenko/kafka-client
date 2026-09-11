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
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMember;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequest;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequestV1;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponse;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponseV0;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponseV1;

/**
 * Byte-exact tests for the DescribeGroups API of Kafka 0.9 (api key 15), raised to version 1 by KIP-124.
 *
 * The request did not change - `DESCRIBE_GROUPS_REQUEST_V1 = DESCRIBE_GROUPS_REQUEST_V0` - and neither did a group
 * entry of the answer; version 1 only put the `ThrottleTimeMs` in front of the array.
 *
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 to v2)"
 */
#[CoversClass(DescribeGroupsRequest::class)]
#[CoversClass(DescribeGroupsRequestV0::class)]
#[CoversClass(DescribeGroupsRequestV1::class)]
#[CoversClass(DescribeGroupsResponse::class)]
#[CoversClass(DescribeGroupsResponseV0::class)]
#[CoversClass(DescribeGroupsResponseV1::class)]
#[CoversClass(DescribeGroupResponseMetadata::class)]
#[CoversClass(DescribeGroupResponseMember::class)]
final class DescribeGroupsTest extends TestCase
{
    /**
     * DescribeGroups request v2 for two groups.
     *
     *   Size          => 00 00 00 28 (40 bytes)
     *   ApiKey        => 00 0f (15)
     *   ApiVersion    => 00 02
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     *   Groups        => 00 00 00 02
     *     GroupId => 00 08 "my-group"
     *     GroupId => 00 0a "other-grou"
     */
    private const string REQUEST_HEX = '00000028'
        . '000f'
        . '0002'
        . '00000001'
        . '0004' . '74657374'
        . '00000002'
        . '0008' . '6d792d67726f7570'
        . '000a' . '6f746865722d67726f75';

    /**
     * The very same body as a version 1 frame.
     */
    private const string REQUEST_V1_HEX = '00000028'
        . '000f'
        . '0001'
        . '00000001'
        . '0004' . '74657374'
        . '00000002'
        . '0008' . '6d792d67726f7570'
        . '000a' . '6f746865722d67726f75';

    /**
     * The very same body as a version 0 frame.
     */
    private const string REQUEST_V0_HEX = '00000028'
        . '000f'
        . '0000'
        . '00000001'
        . '0004' . '74657374'
        . '00000002'
        . '0008' . '6d792d67726f7570'
        . '000a' . '6f746865722d67726f75';

    /**
     * DescribeGroups response v0 for a stable group with a single member.
     *
     *   Size          => 00 00 00 5d (93 bytes)
     *   CorrelationId => 00 00 00 01
     *   Groups        => 00 00 00 01
     *     ErrorCode        => 00 00
     *     GroupId          => 00 08 "my-group"
     *     State            => 00 06 "Stable"
     *     ProtocolType     => 00 08 "consumer"
     *     Protocol         => 00 05 "range"
     *     Members          => 00 00 00 01
     *       MemberId         => 00 09 "member-42"
     *       ClientId         => 00 04 "test"
     *       ClientHost       => 00 0b "/172.18.0.1"
     *       MemberMetadata   => 00 00 00 04 de ad be ef
     *       MemberAssignment => 00 00 00 02 be ef
     */
    private const string STABLE_RESPONSE_HEX = '0000005d'
        . '00000001'
        . '00000001'
        . '0000'
        . '0008' . '6d792d67726f7570'
        . '0006' . '537461626c65'
        . '0008' . '636f6e73756d6572'
        . '0005' . '72616e6765'
        . '00000001'
        . '0009' . '6d656d6265722d3432'
        . '0004' . '74657374'
        . '000b' . '2f3137322e31382e302e31'
        . '00000004' . 'deadbeef'
        . '00000002' . 'beef';

    /**
     * DescribeGroups response v0 for a group the coordinator does not know: the error code 0 and the state "Dead".
     *
     *   Size          => 00 00 00 22 (34 bytes)
     *   CorrelationId => 00 00 00 01
     *   Groups        => 00 00 00 01
     *     ErrorCode    => 00 00
     *     GroupId      => 00 08 "my-group"
     *     State        => 00 04 "Dead"
     *     ProtocolType => 00 00
     *     Protocol     => 00 00
     *     Members      => 00 00 00 00
     */
    private const string DEAD_RESPONSE_HEX = '00000022'
        . '00000001'
        . '00000001'
        . '0000'
        . '0008' . '6d792d67726f7570'
        . '0004' . '44656164'
        . '0000'
        . '0000'
        . '00000000';

    /**
     * The answer of a broker that is not the coordinator of the group: the error code 16 with an empty description.
     */
    private const string NOT_COORDINATOR_RESPONSE_HEX = '0000001e'
        . '00000001'
        . '00000001'
        . '0010'
        . '0008' . '6d792d67726f7570'
        . '0000'
        . '0000'
        . '0000'
        . '00000000';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new DescribeGroupsRequest(['my-group', 'other-grou'], 'test', 1);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_GROUPS, $request->getApiKey());
        self::assertSame(2, $request->getApiVersion(), 'KIP-219 makes the version this client sends 2');
        self::assertSame(['my-group', 'other-grou'], $request->getGroups());
    }

    public function testTheVersionOneRequestIsTheSameBody(): void
    {
        $request = new DescribeGroupsRequestV1(['my-group', 'other-grou'], 'test', 1);

        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
        self::assertSame(
            substr(self::REQUEST_HEX, 16),
            substr(self::REQUEST_V1_HEX, 16),
            'KIP-219 raised the api version of DescribeGroups without adding a field'
        );
    }

    public function testTheVersionZeroRequestIsTheSameBody(): void
    {
        $request = new DescribeGroupsRequestV0(['my-group', 'other-grou'], 'test', 1);

        self::assertSame(self::REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(substr(self::REQUEST_HEX, 16), substr(self::REQUEST_V0_HEX, 16));
    }

    public function testAnEmptyGroupArrayIsPackedAsAnEmptyArray(): void
    {
        $request = new DescribeGroupsRequest([], '', 0);

        self::assertSame('0000000e' . '000f' . '0002' . '00000000' . '0000' . '00000000', bin2hex((string) $request));
    }

    public function testStableGroupIsUnpackedAccordingToTheSpec(): void
    {
        $response = DescribeGroupsResponseV0::unpack(new StringStream((string) hex2bin(self::STABLE_RESPONSE_HEX)));

        self::assertSame(['my-group'], array_keys($response->groups), 'groups are keyed by their id');
        $group = $response->groups['my-group'];
        self::assertSame(0, $group->errorCode);
        self::assertSame(DescribeGroupResponseMetadata::STATE_STABLE, $group->state);
        self::assertSame('consumer', $group->protocolType);
        self::assertSame('range', $group->protocol);

        self::assertSame(['member-42'], array_keys($group->members), 'members are keyed by their member id');
        $member = $group->members['member-42'];
        self::assertSame('test', $member->clientId);
        self::assertSame('/172.18.0.1', $member->clientHost, 'the host comes with the leading slash of Java');
        self::assertSame('deadbeef', bin2hex($member->memberMetadata), 'the metadata stays an opaque byte array');
        self::assertSame('beef', bin2hex($member->memberAssignment));
    }

    public function testAnUnknownGroupIsReportedAsDeadWithoutAnError(): void
    {
        $response = DescribeGroupsResponseV0::unpack(new StringStream((string) hex2bin(self::DEAD_RESPONSE_HEX)));
        $group    = $response->groups['my-group'];

        self::assertSame(0, $group->errorCode, 'a group that does not exist is not an error');
        self::assertSame(DescribeGroupResponseMetadata::STATE_DEAD, $group->state);
        self::assertSame('', $group->protocolType);
        self::assertSame('', $group->protocol);
        self::assertSame([], $group->members);
    }

    public function testEveryGroupCarriesItsOwnErrorCode(): void
    {
        $frame    = (string) hex2bin(self::NOT_COORDINATOR_RESPONSE_HEX);
        $response = DescribeGroupsResponseV0::unpack(new StringStream($frame));

        self::assertSame(16, $response->groups['my-group']->errorCode);
        self::assertSame('', $response->groups['my-group']->state, 'a broker that is not the coordinator knows nothing');
    }

    public function testTheVersionOneAndVersionTwoAnswersStartWithTheThrottleTime(): void
    {
        $frame = '00000026'
            . '00000001'
            . '00000000'
            . '00000001'
            . '0000'
            . '0008' . '6d792d67726f7570'
            . '0004' . '44656164'
            . '0000'
            . '0000'
            . '00000000';

        foreach ([DescribeGroupsResponseV1::class, DescribeGroupsResponse::class] as $class) {
            $response = $class::unpack(new StringStream((string) hex2bin($frame)));

            self::assertSame(0, $response->throttleTimeMs);
            self::assertSame(['my-group'], array_keys($response->groups));
            self::assertSame(DescribeGroupResponseMetadata::STATE_DEAD, $response->groups['my-group']->state);
            self::assertSame($frame, bin2hex((string) $response));
        }
    }
}
