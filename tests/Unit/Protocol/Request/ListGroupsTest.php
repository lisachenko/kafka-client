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
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsRequestV0;
use Protocol\Kafka\Protocol\Request\ListGroupsResponse;
use Protocol\Kafka\Protocol\Request\ListGroupsResponseV0;

/**
 * Byte-exact tests for the ListGroups API of Kafka 0.9 (api key 16), raised to version 1 by KIP-124.
 *
 * The request of version 1 is the request of version 0 - the bare header - and only the answer gained the leading
 * `ThrottleTimeMs`, which is why the two versions need a response class each.
 *
 * @see docs/protocol/1.1.md, section "ListGroups API (key 16, v0 and v1)"
 */
#[CoversClass(ListGroupsRequest::class)]
#[CoversClass(ListGroupsRequestV0::class)]
#[CoversClass(ListGroupsResponse::class)]
#[CoversClass(ListGroupsResponseV0::class)]
#[CoversClass(ListGroupResponseProtocol::class)]
final class ListGroupsTest extends TestCase
{
    /**
     * ListGroups request v1, which is the request header and nothing else.
     *
     *   Size          => 00 00 00 0e (14 bytes)
     *   ApiKey        => 00 10 (16)
     *   ApiVersion    => 00 01
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     */
    private const string REQUEST_HEX = '0000000e'
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
        self::assertSame(1, $request->getApiVersion());
        self::assertSame(14, $request->getMessageSize(), 'header only, the request has no body');
    }

    public function testTheVersionZeroRequestIsTheSameEmptyBody(): void
    {
        $request = new ListGroupsRequestV0('test', 1);

        self::assertSame(self::REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());
        self::assertSame(
            substr(self::REQUEST_HEX, 16),
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

    public function testTheVersionOneAnswerStartsWithTheThrottleTime(): void
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

        $response = ListGroupsResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(0, $response->errorCode);
        self::assertSame(['my-group', 'other-grou'], array_keys($response->groups));
        self::assertSame($frame, bin2hex((string) $response));
    }

    public function testAnErrorIsReportedForTheWholeRequestWithoutGroups(): void
    {
        $response = ListGroupsResponseV0::unpack(new StringStream((string) hex2bin(self::LOADING_RESPONSE_HEX)));

        self::assertSame(14, $response->errorCode);
        self::assertSame([], $response->groups);
    }
}
