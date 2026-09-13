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
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsRequestV0;
use Protocol\Kafka\Protocol\Request\ListGroupsRequestV1;
use Protocol\Kafka\Protocol\Request\ListGroupsRequestV2;
use Protocol\Kafka\Protocol\Request\ListGroupsRequestV3;
use Protocol\Kafka\Protocol\Request\ListGroupsResponse;
use Protocol\Kafka\Protocol\Request\ListGroupsResponseV0;
use Protocol\Kafka\Protocol\Request\ListGroupsResponseV1;
use Protocol\Kafka\Protocol\Request\ListGroupsResponseV2;
use Protocol\Kafka\Protocol\Request\ListGroupsResponseV3;

/**
 * Byte-exact tests for the ListGroups API of Kafka 0.9 (api key 16), raised to version 1 by KIP-124.
 *
 * The request of version 1 is the request of version 0 - the bare header - and only the answer gained the leading
 * `ThrottleTimeMs`, which is why the two versions need a response class each.
 *
 * @see docs/protocol/2.8.md, section "ListGroups API (key 16, v0 to v4)"
 */
#[CoversClass(ListGroupsRequest::class)]
#[CoversClass(ListGroupsRequestV0::class)]
#[CoversClass(ListGroupsRequestV1::class)]
#[CoversClass(ListGroupsRequestV2::class)]
#[CoversClass(ListGroupsResponse::class)]
#[CoversClass(ListGroupsResponseV0::class)]
#[CoversClass(ListGroupsResponseV1::class)]
#[CoversClass(ListGroupsResponseV2::class)]
#[CoversClass(ListGroupResponseProtocol::class)]
final class ListGroupsTest extends TestCase
{
    /**
     * ListGroups request v3 (Kafka 2.4, KIP-482), the flexible frame this client sends.
     *
     *   Size          => 00 00 00 10 (16 bytes)
     *   ApiKey        => 00 10 (16)
     *   ApiVersion    => 00 03
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test" (never compact)
     *   TAG_BUFFER    => 00 (of the request header v2)
     *   TAG_BUFFER    => 00 (of the body, which has no field at all)
     */
    private const string REQUEST_HEX = '00000011'
        . '0010'
        . '0004'
        . '00000001'
        . '0004' . '74657374'
        . '00'
        . '01'
        . '00';

    /**
     * The version 3 request, whose whole body is the empty tag buffer that KIP-482 gave every flexible frame.
     */
    private const string REQUEST_V3_HEX = '00000010'
        . '0010'
        . '0003'
        . '00000001'
        . '0004' . '74657374'
        . '00'
        . '00';

    /**
     * The same request as a version 2 frame, which is the header and nothing else.
     */
    private const string REQUEST_V2_HEX = '0000000e'
        . '0010'
        . '0002'
        . '00000001'
        . '0004' . '74657374';

    /**
     * The very same empty body as a version 1 frame.
     */
    private const string REQUEST_V1_HEX = '0000000e'
        . '0010'
        . '0001'
        . '00000001'
        . '0004' . '74657374';

    /**
     * The very same body as a version 0 frame: `LIST_GROUPS_REQUEST_V1 = LIST_GROUPS_REQUEST_V0`.
     */
    private const string REQUEST_V0_HEX = '0000000e'
        . '0010'
        . '0000'
        . '00000001'
        . '0004' . '74657374';

    /**
     * ListGroups response v0 with two groups.
     *
     *   Size          => 00 00 00 34 (52 bytes)
     *   CorrelationId => 00 00 00 01
     *   ErrorCode     => 00 00
     *   Groups        => 00 00 00 02
     *     GroupId      => 00 08 "my-group"
     *     ProtocolType => 00 08 "consumer"
     *     GroupId      => 00 0a "other-grou"
     *     ProtocolType => 00 08 "consumer"
     */
    private const string RESPONSE_HEX = '00000034'
        . '00000001'
        . '0000'
        . '00000002'
        . '0008' . '6d792d67726f7570'
        . '0008' . '636f6e73756d6572'
        . '000a' . '6f746865722d67726f75'
        . '0008' . '636f6e73756d6572';

    /**
     * The answer of a coordinator that is still loading the offsets: the error code 14 and no groups at all.
     */
    private const string LOADING_RESPONSE_HEX = '0000000a'
        . '00000001'
        . '000e'
        . '00000000';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new ListGroupsRequest('test', 1);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::LIST_GROUPS, $request->getApiKey());
        self::assertSame(4, $request->getApiVersion(), 'KIP-518 makes the version this client sends 4');
        self::assertSame([], $request->getStatesFilter(), 'an empty filter is every group of the coordinator');
        self::assertSame(
            17,
            $request->getMessageSize(),
            'the empty states filter is one byte more than the version 3 body of two empty tagged sections'
        );
    }

    /**
     * KIP-518 (Kafka 2.6): the states the answer is bounded to are a compact array of compact strings
     */
    public function testTheStatesFilterOfVersionFourIsWrittenAsACompactArray(): void
    {
        $request = new ListGroupsRequest('test', 1, ['Empty', 'Stable']);

        self::assertSame(
            '0000001e' . '0010' . '0004' . '00000001'
            . '0004' . '74657374'
            . '00'
            . '03'
            . '06' . '456d707479'
            . '07' . '537461626c65'
            . '00',
            bin2hex((string) $request),
            'two entries are the unsigned varint 3, and each name its length plus one'
        );
        self::assertSame(['Empty', 'Stable'], $request->getStatesFilter());
    }

    /**
     * The version below has no field at all, so its body is the tag buffer of the body and nothing else
     */
    public function testTheVersionThreeRequestHasNoStatesFilter(): void
    {
        $request = new ListGroupsRequestV3('test', 1);

        self::assertSame(self::REQUEST_V3_HEX, bin2hex((string) $request));
        self::assertSame(3, $request->getApiVersion());
        self::assertArrayNotHasKey('statesFilter', ListGroupsRequestV3::getScheme());
        self::assertArrayHasKey('statesFilter', ListGroupsRequest::getScheme());
    }

    public function testTheVersionTwoRequestIsTheHeaderAndNothingElse(): void
    {
        $request = new ListGroupsRequestV2('test', 1);

        self::assertSame(self::REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(2, $request->getApiVersion());
        self::assertSame(14, $request->getMessageSize(), 'header only, the request has no body at all');
    }

    public function testTheVersionOneRequestIsTheSameEmptyBody(): void
    {
        $request = new ListGroupsRequestV1('test', 1);

        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
        self::assertSame(
            substr(self::REQUEST_V2_HEX, 16),
            substr(self::REQUEST_V1_HEX, 16),
            'KIP-219 raised the api version of ListGroups without adding a field'
        );
    }

    public function testTheVersionZeroRequestIsTheSameEmptyBody(): void
    {
        $request = new ListGroupsRequestV0('test', 1);

        self::assertSame(self::REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(
            substr(self::REQUEST_V2_HEX, 16),
            substr(self::REQUEST_V0_HEX, 16),
            'only the version field of the header separates the two'
        );
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = ListGroupsResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertSame(['my-group', 'other-grou'], array_keys($response->groups), 'groups are keyed by their id');
        self::assertSame('my-group', $response->groups['my-group']->groupId);
        self::assertSame('consumer', $response->groups['my-group']->protocolType);
        self::assertSame(0, $response->throttleTimeMs, 'version 0 has no throttle time');
    }

    public function testTheVersionOneAndVersionTwoAnswersStartWithTheThrottleTime(): void
    {
        $frame = '00000038'
            . '00000001'
            . '00000000'
            . '0000'
            . '00000002'
            . '0008' . '6d792d67726f7570'
            . '0008' . '636f6e73756d6572'
            . '000a' . '6f746865722d67726f75'
            . '0008' . '636f6e73756d6572';

        foreach ([ListGroupsResponseV1::class, ListGroupsResponseV2::class] as $class) {
            $response = $class::unpack(new StringStream((string) hex2bin($frame)));

            self::assertSame(0, $response->throttleTimeMs);
            self::assertSame(0, $response->errorCode);
            self::assertSame(['my-group', 'other-grou'], array_keys($response->groups));
            self::assertSame($frame, bin2hex((string) $response));
        }
    }

    /**
     * The version 3 answer is the same fields compactly, with a tag buffer per group entry and one for the body
     */
    public function testTheVersionThreeAnswerIsTheFlexibleEncodingOfTheSameFields(): void
    {
        $frame = '00000035'
            . '00000001'
            . '00'
            . '00000000'
            . '0000'
            . '03'
            . '09' . '6d792d67726f7570' . '09' . '636f6e73756d6572' . '00'
            . '0b' . '6f746865722d67726f75' . '09' . '636f6e73756d6572' . '00'
            . '00';

        $response = ListGroupsResponseV3::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(0, $response->errorCode);
        self::assertSame(['my-group', 'other-grou'], array_keys($response->groups));
        self::assertSame('consumer', $response->groups['my-group']->protocolType);
        self::assertNull($response->groups['my-group']->groupState, 'the versions below 4 report no state');
        self::assertSame($frame, bin2hex((string) $response), 'the answer survives the round trip');
        self::assertTrue(ListGroupsResponseV3::isFlexible());
    }

    /**
     * KIP-518 (Kafka 2.6): every entry of a version 4 answer ends in the state of that group
     */
    public function testTheVersionFourAnswerCarriesTheStateOfEveryGroup(): void
    {
        $frame = '00000042'
            . '00000001'
            . '00'
            . '00000000'
            . '0000'
            . '03'
            . '09' . '6d792d67726f7570' . '09' . '636f6e73756d6572' . '07' . '537461626c65' . '00'
            . '0b' . '6f746865722d67726f75' . '09' . '636f6e73756d6572' . '06' . '456d707479' . '00'
            . '00';

        $response = ListGroupsResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(['my-group', 'other-grou'], array_keys($response->groups));
        self::assertSame(
            DescribeGroupResponseMetadata::STATE_STABLE,
            $response->groups['my-group']->groupState
        );
        self::assertSame(
            DescribeGroupResponseMetadata::STATE_EMPTY,
            $response->groups['other-grou']->groupState,
            'the state is the very name a DescribeGroups answer reports'
        );
        self::assertSame($frame, bin2hex((string) $response), 'the answer survives the round trip');
    }

    public function testAnErrorIsReportedForTheWholeRequestWithoutGroups(): void
    {
        $response = ListGroupsResponseV0::unpack(new StringStream((string) hex2bin(self::LOADING_RESPONSE_HEX)));

        self::assertSame(14, $response->errorCode);
        self::assertSame([], $response->groups);
    }
}
