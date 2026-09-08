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
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;

/**
 * Byte-exact tests for the GroupCoordinator API, called ConsumerMetadata in Kafka 0.8.2 (api key 10, v0).
 *
 * @see docs/protocol/0.8.2.md, section "GroupCoordinator API (key 10, v0)"
 */
#[CoversClass(GroupCoordinatorRequest::class)]
#[CoversClass(GroupCoordinatorResponse::class)]
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
     * The answer of a broker that is still creating the internal __consumer_offsets topic: error code 15 and the
     * placeholder coordinator -1:"":-1 that kafka/server/KafkaApis.scala writes there.
     */
    private const string NOT_AVAILABLE_RESPONSE_HEX = '00000010'
        . '00000001'
        . '000f'
        . 'ffffffff'
        . '0000'
        . 'ffffffff';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new GroupCoordinatorRequest('my-group', 'test', 1);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::GROUP_COORDINATOR, $request->getApiKey());
        self::assertSame(0, $request->getApiVersion());
    }

    public function testEmptyGroupNameIsPackedAsAnEmptyStringNotAsNull(): void
    {
        $request = new GroupCoordinatorRequest('', '', 0);

        self::assertSame('0000000c' . '000a' . '0000' . '00000000' . '0000' . '0000', bin2hex((string) $request));
    }

    public function testResponseIsUnpackedAccordingToTheSpec(): void
    {
        $response = GroupCoordinatorResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode);
        self::assertSame(0, $response->coordinator->nodeId);
        self::assertSame('127.0.0.1', $response->coordinator->host);
        self::assertSame(9092, $response->coordinator->port);
    }

    public function testCoordinatorNotAvailableIsReportedWithASignedErrorCode(): void
    {
        $frame    = (string) hex2bin(self::NOT_AVAILABLE_RESPONSE_HEX);
        $response = GroupCoordinatorResponse::unpack(new StringStream($frame));

        self::assertSame(15, $response->errorCode);
        self::assertSame(-1, $response->coordinator->nodeId);
        self::assertSame('', $response->coordinator->host);
        self::assertSame(-1, $response->coordinator->port, 'the port is an INT32 and is read as a signed value');
    }
}
