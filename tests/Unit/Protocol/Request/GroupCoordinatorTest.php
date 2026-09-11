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
use Protocol\Kafka\Protocol\Data\GroupCoordinatorResponseMetadata;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequestV0;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequestV1;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequestV2;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponseV0;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponseV1;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponseV2;

/**
 * Byte-exact tests for the GroupCoordinator API, called ConsumerMetadata in Kafka 0.8.2 and FindCoordinator in 0.11.
 *
 * Version 2 (KIP-219, Kafka 2.0) is the version 1 frame with a higher api version and nothing else, so it is the
 * version this client sends and {@see GroupCoordinatorRequestV1} keeps the version 1 number for a lower broker.
 *
 * @see docs/protocol/2.8.md, section "GroupCoordinator API (key 10, v0 to v3)"
 */
#[CoversClass(GroupCoordinatorRequest::class)]
#[CoversClass(GroupCoordinatorRequestV0::class)]
#[CoversClass(GroupCoordinatorRequestV1::class)]
#[CoversClass(GroupCoordinatorRequestV2::class)]
#[CoversClass(GroupCoordinatorResponse::class)]
#[CoversClass(GroupCoordinatorResponseV0::class)]
#[CoversClass(GroupCoordinatorResponseV1::class)]
#[CoversClass(GroupCoordinatorResponseV2::class)]
#[CoversClass(GroupCoordinatorResponseMetadata::class)]
final class GroupCoordinatorTest extends TestCase
{
    /**
     * GroupCoordinator request v0 for the group "my-group", correlation id 1, client id "test".
     *
     *   Size          => 00 00 00 18 (24 bytes)
     *   ApiKey        => 00 0a
     *   ApiVersion    => 00 00
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     *   ConsumerGroup => 00 08 "my-group"
     */
    private const string REQUEST_HEX = '00000018'
        . '000a'
        . '0000'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570';

    /**
     * The same request as version 1, which appends the coordinator type 0 (group).
     *
     *   Size            => 00 00 00 19 (25 bytes)
     *   ApiVersion      => 00 01
     *   CoordinatorKey  => 00 08 "my-group"
     *   CoordinatorType => 00
     */
    private const string REQUEST_V1_HEX = '00000019'
        . '000a'
        . '0001'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00';

    /**
     * The very same lookup as version 2 (KIP-219, Kafka 2.0): only the ApiVersion field changes, 00 01 to 00 02.
     */
    private const string REQUEST_V2_HEX = '00000019'
        . '000a'
        . '0002'
        . '00000001'
        . '0004' . '74657374'
        . '0008' . '6d792d67726f7570'
        . '00';

    /**
     * A version 1 lookup of the transactional id "my-txn", which is the coordinator type 1.
     */
    private const string REQUEST_V2_TRANSACTION_HEX = '00000017'
        . '000a'
        . '0002'
        . '00000002'
        . '0004' . '74657374'
        . '0006' . '6d792d74786e'
        . '01';

    /**
     * GroupCoordinator response v0 naming broker 0 at 127.0.0.1:9092 as the coordinator.
     *
     *   Size            => 00 00 00 19 (25 bytes)
     *   CorrelationId   => 00 00 00 01
     *   ErrorCode       => 00 00
     *   CoordinatorId   => 00 00 00 00
     *   CoordinatorHost => 00 09 "127.0.0.1"
     *   CoordinatorPort => 00 00 23 84 (9092)
     */
    private const string RESPONSE_HEX = '00000019'
        . '00000001'
        . '0000'
        . '00000000'
        . '0009' . '3132372e302e302e31'
        . '00002384';

    /**
     * The same answer as version 1: the throttle time opens it and a null error message follows the error code.
     *
     *   Size            => 00 00 00 1f (31 bytes)
     *   CorrelationId   => 00 00 00 01
     *   ThrottleTimeMs  => 00 00 00 00
     *   ErrorCode       => 00 00
     *   ErrorMessage    => ff ff (null)
     *   CoordinatorId   => 00 00 00 00
     *   CoordinatorHost => 00 09 "127.0.0.1"
     *   CoordinatorPort => 00 00 23 84 (9092)
     */
    private const string RESPONSE_V1_HEX = '0000001f'
        . '00000001'
        . '00000000'
        . '0000'
        . 'ffff'
        . '00000000'
        . '0009' . '3132372e302e302e31'
        . '00002384';

    /**
     * The answer of a broker that is still creating the internal __consumer_offsets topic: error code 15 and the
     * placeholder coordinator -1:"":-1 that kafka/server/KafkaApis.scala writes there.
     */
    /**
     * The same request as a version 3 frame (Kafka 2.4, KIP-482): the header v2 ends in a tag buffer, the group
     * id is a compact string and the body ends in a tag buffer of its own.
     *
     *   Size            => 00 00 00 1a (26 bytes)
     *   ApiKey          => 00 0a
     *   ApiVersion      => 00 03
     *   CorrelationId   => 00 00 00 01
     *   ClientId        => 00 04 "test" (never compact)
     *   TAG_BUFFER      => 00
     *   ConsumerGroup   => 09 "my-group" (compact: 8 + 1)
     *   CoordinatorType => 00
     *   TAG_BUFFER      => 00
     */
    private const string REQUEST_V3_HEX = '0000001a'
        . '000a'
        . '0003'
        . '00000001'
        . '0004' . '74657374'
        . '00'
        . '09' . '6d792d67726f7570'
        . '00'
        . '00';

    private const string NOT_AVAILABLE_RESPONSE_HEX = '00000010'
        . '00000001'
        . '000f'
        . 'ffffffff'
        . '0000'
        . 'ffffffff';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new GroupCoordinatorRequestV0('my-group', GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP, 'test', 1);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::GROUP_COORDINATOR, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
    }

    public function testVersionOneAppendsTheCoordinatorTypeToTheKey(): void
    {
        $request = new GroupCoordinatorRequestV1(
            'my-group',
            GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP,
            'test',
            1
        );

        self::assertSame(self::REQUEST_V1_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
    }

    public function testVersionTwoIsTheVersionOneFrameOneApiVersionHigher(): void
    {
        $request = new GroupCoordinatorRequestV2(
            'my-group',
            GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP,
            'test',
            1
        );

        self::assertSame(self::REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(2, $request->getApiVersion(), 'the highest non-flexible version of the api');
        self::assertSame(
            substr(self::REQUEST_V1_HEX, 16),
            substr(self::REQUEST_V2_HEX, 16),
            'KIP-219 raised the api version without adding a field'
        );
        self::assertFalse(GroupCoordinatorRequestV2::isFlexible());
    }

    /**
     * Version 3 (Kafka 2.4, KIP-482) is the same frame in the flexible encoding: the request header v2 with its
     * tagged-field section, a compact group id and a tag buffer that closes the body
     */
    public function testVersionThreeIsTheFlexibleEncodingOfTheSameFields(): void
    {
        $request = new GroupCoordinatorRequest('my-group', GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP, 'test', 1);

        self::assertSame(self::REQUEST_V3_HEX, bin2hex((string) $request));
        self::assertSame(3, $request->getApiVersion(), 'the version this client sends');
        self::assertTrue(GroupCoordinatorRequest::isFlexible());
    }

    public function testTheGroupTypeIsTheDefaultOfTheRequest(): void
    {
        self::assertSame(
            self::REQUEST_V3_HEX,
            bin2hex((string) new GroupCoordinatorRequest('my-group', clientId: 'test', correlationId: 1))
        );
    }

    public function testATransactionalIdIsLookedUpWithTheCoordinatorTypeOne(): void
    {
        $request = new GroupCoordinatorRequestV2(
            'my-txn',
            GroupCoordinatorRequest::COORDINATOR_TYPE_TRANSACTION,
            'test',
            2
        );

        self::assertSame(self::REQUEST_V2_TRANSACTION_HEX, bin2hex((string) $request));
    }

    public function testTheCoordinatorTypeIsNotOnTheWireOfVersionZero(): void
    {
        $request = new GroupCoordinatorRequestV0(
            'my-group',
            GroupCoordinatorRequest::COORDINATOR_TYPE_TRANSACTION,
            'test',
            1
        );

        self::assertSame(
            self::REQUEST_HEX,
            bin2hex((string) $request),
            'a version 0 frame carries the key alone, whatever type the caller asked for'
        );
    }

    public function testEmptyGroupNameIsPackedAsAnEmptyStringNotAsNull(): void
    {
        $request = new GroupCoordinatorRequestV0('', GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP, '', 0);

        self::assertSame('0000000c' . '000a' . '0000' . '00000000' . '0000' . '0000', bin2hex((string) $request));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = GroupCoordinatorResponseV0::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertSame(0, $response->coordinator->nodeId);
        self::assertSame('127.0.0.1', $response->coordinator->host);
        self::assertSame(9092, $response->coordinator->port);
        self::assertSame(0, $response->throttleTimeMs, 'version 0 has no throttle time and leaves it at zero');
        self::assertNull($response->errorMessage, 'version 0 has no error message either');
    }

    public function testVersionOneReadsTheThrottleTimeAndTheErrorMessageAroundTheErrorCode(): void
    {
        foreach ([GroupCoordinatorResponseV1::class, GroupCoordinatorResponseV2::class] as $class) {
            $response = $class::unpack(new StringStream((string) hex2bin(self::RESPONSE_V1_HEX)));

            self::assertSame(1, $response->getCorrelationId());
            self::assertSame(0, $response->throttleTimeMs);
            self::assertSame(0, $response->errorCode);
            self::assertNull($response->errorMessage, 'the null marker of a 0.11 or 1.1 broker is still readable');
            self::assertSame(0, $response->coordinator->nodeId);
            self::assertSame(9092, $response->coordinator->port);
        }
    }

    public function testTheErrorMessageOfASuccessfulLookupIsTheStringNoneOnATwoPointXBroker(): void
    {
        // A 2.8.2 broker builds every answer with `Errors.message()`, and `Errors.NONE.message()` is the NAME of
        // the constant, so a lookup that succeeded carries "NONE" where a 0.11 or 1.1 broker sent the null marker
        $frame = '00000023'
            . '00000001'
            . '00000000'
            . '0000'
            . '0004' . bin2hex('NONE')
            . '00000000'
            . '0009' . '3132372e302e302e31'
            . '00002384';

        $response = GroupCoordinatorResponseV2::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(0, $response->errorCode);
        self::assertSame('NONE', $response->errorMessage);
        self::assertSame(0, $response->coordinator->nodeId);
        self::assertSame($frame, bin2hex((string) $response));
    }

    /**
     * The version 3 answer carries the same fields compactly, and the three fields of the coordinator are NOT a
     * structure of their own: the frame ends in a single tag buffer behind the port
     */
    public function testTheVersionThreeAnswerIsCompactAndCarriesOneTagBufferForTheWholeBody(): void
    {
        $frame = '00000023'
            . '00000001'
            . '00'
            . '00000000'
            . '0000'
            . '05' . bin2hex('NONE')
            . '00000000'
            . '0a' . '3132372e302e302e31'
            . '00002384'
            . '00';

        $response = GroupCoordinatorResponse::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertSame('NONE', $response->errorMessage);
        self::assertSame(0, $response->coordinator->nodeId);
        self::assertSame('127.0.0.1', $response->coordinator->host);
        self::assertSame(9092, $response->coordinator->port);
        self::assertSame($frame, bin2hex((string) $response), 'the answer survives the round trip');
        self::assertTrue(GroupCoordinatorResponse::isFlexible());
    }

    public function testCoordinatorNotAvailableIsReportedWithASignedErrorCode(): void
    {
        $frame    = (string) hex2bin(self::NOT_AVAILABLE_RESPONSE_HEX);
        $response = GroupCoordinatorResponseV0::unpack(new StringStream($frame));

        self::assertSame(15, $response->errorCode);
        self::assertSame(-1, $response->coordinator->nodeId);
        self::assertSame('', $response->coordinator->host);
        self::assertSame(-1, $response->coordinator->port, 'the port is an INT32 and is read as a signed value');
    }
}
