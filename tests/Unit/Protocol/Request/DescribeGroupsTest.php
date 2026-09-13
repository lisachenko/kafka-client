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
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMemberV0;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadataV0;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequest;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequestV1;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequestV2;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequestV3;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequestV4;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponse;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponseV0;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponseV1;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponseV2;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponseV3;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponseV4;

/**
 * Byte-exact tests for the DescribeGroups API of Kafka 0.9 (api key 15), raised to version 1 by KIP-124.
 *
 * The request did not change - `DESCRIBE_GROUPS_REQUEST_V1 = DESCRIBE_GROUPS_REQUEST_V0` - and neither did a group
 * entry of the answer; version 1 only put the `ThrottleTimeMs` in front of the array.
 *
 * Version 3 (KIP-430, Kafka 2.3) is the first one that changed either half: the request gained the boolean
 * `include_authorized_operations` and every group entry of the answer the 32-bit `authorized_operations`.
 *
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 to v5)"
 */
#[CoversClass(DescribeGroupsRequest::class)]
#[CoversClass(DescribeGroupsRequestV0::class)]
#[CoversClass(DescribeGroupsRequestV1::class)]
#[CoversClass(DescribeGroupsRequestV2::class)]
#[CoversClass(DescribeGroupsRequestV3::class)]
#[CoversClass(DescribeGroupsRequestV4::class)]
#[CoversClass(DescribeGroupsResponse::class)]
#[CoversClass(DescribeGroupsResponseV0::class)]
#[CoversClass(DescribeGroupsResponseV1::class)]
#[CoversClass(DescribeGroupsResponseV2::class)]
#[CoversClass(DescribeGroupsResponseV3::class)]
#[CoversClass(DescribeGroupsResponseV4::class)]
#[CoversClass(DescribeGroupResponseMetadata::class)]
#[CoversClass(DescribeGroupResponseMetadataV0::class)]
#[CoversClass(DescribeGroupResponseMember::class)]
#[CoversClass(DescribeGroupResponseMemberV0::class)]
final class DescribeGroupsTest extends TestCase
{
    /**
     * DescribeGroups request v5 (Kafka 2.4, KIP-482) for two groups, which does not ask for the operations.
     *
     *   Size                        => 00 00 00 26 (38 bytes)
     *   ApiKey                      => 00 0f (15)
     *   ApiVersion                  => 00 05
     *   CorrelationId               => 00 00 00 01
     *   ClientId                    => 00 04 "test" (never compact)
     *   TAG_BUFFER                  => 00 (of the request header v2)
     *   Groups                      => 03 (compact: 2 + 1)
     *     GroupId => 09 "my-group"
     *     GroupId => 0b "other-grou"
     *   IncludeAuthorizedOperations => 00 (false, KIP-430; a boolean is one byte in both encodings)
     *   TAG_BUFFER                  => 00 (of the body)
     */
    private const string REQUEST_HEX = '00000026'
        . '000f'
        . '0005'
        . '00000001'
        . '0004' . '74657374'
        . '00'
        . '03'
        . '09' . '6d792d67726f7570'
        . '0b' . '6f746865722d67726f75'
        . '00'
        . '00';

    /**
     * The same request that does ask for them: the byte before the tag buffer of the body is 01
     */
    private const string REQUEST_WITH_OPERATIONS_HEX = '00000026'
        . '000f'
        . '0005'
        . '00000001'
        . '0004' . '74657374'
        . '00'
        . '03'
        . '09' . '6d792d67726f7570'
        . '0b' . '6f746865722d67726f75'
        . '01'
        . '00';

    /**
     * The same request as a version 3 frame, plainly encoded, with the flag of KIP-430 at its end.
     */
    private const string REQUEST_V3_HEX = '00000029'
        . '000f'
        . '0003'
        . '00000001'
        . '0004' . '74657374'
        . '00000002'
        . '0008' . '6d792d67726f7570'
        . '000a' . '6f746865722d67726f75'
        . '00';

    /**
     * The same request as a version 2 frame, which has no such flag at all.
     */
    private const string REQUEST_V2_HEX = '00000028'
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
        self::assertSame(5, $request->getApiVersion(), 'KIP-482 makes the version this client sends 5');
        self::assertSame(['my-group', 'other-grou'], $request->getGroups());
        self::assertFalse($request->includesAuthorizedOperations(), 'the flag defaults to false');
        self::assertTrue(DescribeGroupsRequest::isFlexible());
    }

    public function testTheVersionThreeRequestIsThePlainEncodingOfTheSameFields(): void
    {
        $request = new DescribeGroupsRequestV3(['my-group', 'other-grou'], 'test', 1);

        self::assertSame(self::REQUEST_V3_HEX, bin2hex((string) $request));
        self::assertSame(3, $request->getApiVersion());
        self::assertFalse(DescribeGroupsRequestV3::isFlexible());
        self::assertSame(
            bin2hex((string) new DescribeGroupsRequestV4(['my-group', 'other-grou'], 'test', 1)),
            str_replace('000f0003', '000f0004', self::REQUEST_V3_HEX),
            'the version 4 request is the version 3 frame: what KIP-345 added there belongs to the answer'
        );
    }

    public function testTheAuthorizedOperationsFlagIsTheLastByteOfTheVersionThreeRequest(): void
    {
        $request = new DescribeGroupsRequest(['my-group', 'other-grou'], 'test', 1, true);

        self::assertSame(self::REQUEST_WITH_OPERATIONS_HEX, bin2hex((string) $request));
        self::assertTrue($request->includesAuthorizedOperations());
    }

    public function testTheVersionTwoRequestHasNoAuthorizedOperationsFlag(): void
    {
        $request = new DescribeGroupsRequestV2(['my-group', 'other-grou'], 'test', 1, true);

        self::assertSame(self::REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(2, $request->getApiVersion());
        self::assertArrayNotHasKey('includeAuthorizedOperations', DescribeGroupsRequestV2::getScheme());
        self::assertArrayHasKey('includeAuthorizedOperations', DescribeGroupsRequest::getScheme());
    }

    public function testTheVersionOneRequestIsTheSameBody(): void
    {
        $request = new DescribeGroupsRequestV1(['my-group', 'other-grou'], 'test', 1);

        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
        self::assertSame(
            substr(self::REQUEST_V2_HEX, 16),
            substr(self::REQUEST_V1_HEX, 16),
            'KIP-219 raised the api version of DescribeGroups without adding a field'
        );
    }

    public function testTheVersionZeroRequestIsTheSameBody(): void
    {
        $request = new DescribeGroupsRequestV0(['my-group', 'other-grou'], 'test', 1);

        self::assertSame(self::REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(substr(self::REQUEST_V2_HEX, 16), substr(self::REQUEST_V0_HEX, 16));
    }

    public function testAnEmptyGroupArrayIsPackedAsAnEmptyArray(): void
    {
        $request = new DescribeGroupsRequest([], '', 0);

        self::assertSame(
            '0000000e' . '000f' . '0005' . '00000000' . '0000' . '00' . '01' . '00' . '00',
            bin2hex((string) $request),
            'an empty compact array is the unsigned varint 1'
        );
        self::assertSame(
            '0000000f' . '000f' . '0003' . '00000000' . '0000' . '00000000' . '00',
            bin2hex((string) new DescribeGroupsRequestV3([], '', 0))
        );
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

        foreach ([DescribeGroupsResponseV1::class, DescribeGroupsResponseV2::class] as $class) {
            $response = $class::unpack(new StringStream((string) hex2bin($frame)));

            self::assertSame(0, $response->throttleTimeMs);
            self::assertSame(['my-group'], array_keys($response->groups));
            self::assertSame(DescribeGroupResponseMetadata::STATE_DEAD, $response->groups['my-group']->state);
            self::assertSame($frame, bin2hex((string) $response));
            self::assertSame(
                DescribeGroupResponseMetadata::OPERATIONS_NOT_REQUESTED,
                $response->groups['my-group']->authorizedOperations,
                'the versions below 3 have no such field, so the property keeps its default'
            );
        }
    }

    /**
     * The version 3 answer of a group the client may read, describe and delete: the bit set 328, which is
     * `AclEntry.supportedOperations(GROUP)` of a broker without an authorizer - READ (3), DELETE (6), DESCRIBE (8)
     */
    public function testTheVersionThreeGroupEntryEndsWithTheAuthorizedOperations(): void
    {
        $frame = '0000002a'
            . '00000001'
            . '00000000'
            . '00000001'
            . '0000'
            . '0008' . '6d792d67726f7570'
            . '0004' . '44656164'
            . '0000'
            . '0000'
            . '00000000'
            . '00000148';

        $response = DescribeGroupsResponseV3::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(328, $response->groups['my-group']->authorizedOperations);
        self::assertSame(
            [3, 6, 8],
            array_values(array_filter(range(0, 12), static fn(int $bit): bool => (328 & (1 << $bit)) !== 0)),
            'the bits are the codes of AclOperation: READ, DELETE and DESCRIBE'
        );
        self::assertSame($frame, bin2hex((string) $response), 'the answer survives the round trip');
    }

    /**
     * A request that does not ask for the operations is answered with `Integer.MIN_VALUE`, and not with an empty
     * bit set: `DescribeGroupsResponse.json` @ 2.8.2 gives the field that default and the broker leaves it there
     */
    public function testAGroupWhoseOperationsWereNotAskedForCarriesIntegerMinValue(): void
    {
        $frame = '0000002a'
            . '00000001'
            . '00000000'
            . '00000001'
            . '0000'
            . '0008' . '6d792d67726f7570'
            . '0004' . '44656164'
            . '0000'
            . '0000'
            . '00000000'
            . '80000000';

        $response = DescribeGroupsResponseV3::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(
            DescribeGroupResponseMetadata::OPERATIONS_NOT_REQUESTED,
            $response->groups['my-group']->authorizedOperations
        );
        self::assertSame(-2147483648, DescribeGroupResponseMetadata::OPERATIONS_NOT_REQUESTED);
    }

    /**
     * Version 4 (KIP-345) gave every member entry the `group_instance_id` of its static member, still plainly
     * encoded; version 5 (KIP-482) is that answer with the compact types and a tag buffer per structure
     */
    public function testTheMemberEntryOfVersionFourCarriesTheInstanceId(): void
    {
        $string = static fn(string $value): string => sprintf('%04x', strlen($value)) . bin2hex($value);
        $body   = '00000001'                                   // correlationId
            . '00000000'                                       // throttleTimeMs
            . '00000001'                                       // groups: 1 item
            . '0000'                                           // errorCode
            . $string('my-group')
            . $string('Stable')
            . $string('consumer')
            . $string('range')
            . '00000001'                                       // members: 1 item
            . $string('member-42')
            . $string('one')                                   // group_instance_id, new in version 4
            . $string('test')
            . $string('/172.18.0.1')
            . '00000000'                                       // memberMetadata: 0 bytes
            . '00000000'                                       // memberAssignment: 0 bytes
            . '00000148';                                      // authorizedOperations = 328
        $frame  = sprintf('%08x', strlen($body) / 2) . $body;

        $response = DescribeGroupsResponseV4::unpack(new StringStream((string) hex2bin($frame)));
        $member   = $response->groups['my-group']->members['member-42'];

        self::assertSame('one', $member->groupInstanceId, 'the instance id of the static member');
        self::assertSame('test', $member->clientId);
        self::assertSame('/172.18.0.1', $member->clientHost);
        self::assertSame(328, $response->groups['my-group']->authorizedOperations);
        self::assertSame($frame, bin2hex((string) $response), 'the answer survives the round trip');
        self::assertFalse(DescribeGroupsResponseV4::isFlexible());
        self::assertArrayNotHasKey(
            'groupInstanceId',
            DescribeGroupResponseMemberV0::getScheme(),
            'the entry of the versions 0 to 3 has no such field'
        );
        self::assertArrayHasKey('groupInstanceId', DescribeGroupResponseMember::getScheme());
    }
}
