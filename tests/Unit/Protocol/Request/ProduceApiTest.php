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
use Protocol\Kafka\Protocol\Data\ProduceRequestPartition;
use Protocol\Kafka\Protocol\Data\ProduceRequestTopic;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopic;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV0;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV0;
use Protocol\Kafka\Tests\Fixture\SpecMessageSet;

/**
 * Byte-exact tests of the Produce API, versions 0 and 1.
 *
 * <pre>
 *   ProduceRequest     => RequiredAcks int16 Timeout int32 [TopicName [Partition int32 MessageSetSize int32
 *                                                                      MessageSet]]
 *   ProduceResponse v0 => [TopicName [Partition int32 ErrorCode int16 Offset int64]]
 *   ProduceResponse v1 => [TopicName [Partition int32 ErrorCode int16 Offset int64]] ThrottleTime int32
 * </pre>
 *
 * The body of the request is the same in both versions, only the answer of version 1 carries the throttle time of
 * a quota violation at its very end.</pre>
 *
 * The message sets are built by {@see SpecMessageSet} directly from the specification, so that the request classes
 * are never checked against bytes they produced themselves.
 *
 * @see docs/protocol/0.9.0.md, sections "Produce API (key 0, v0)" and "MessageSet and Message"
 */
#[CoversClass(ProduceRequest::class)]
#[CoversClass(ProduceRequestV0::class)]
#[CoversClass(ProduceResponse::class)]
#[CoversClass(ProduceResponseV0::class)]
#[CoversClass(ProduceRequestTopic::class)]
#[CoversClass(ProduceRequestPartition::class)]
#[CoversClass(ProduceResponseTopic::class)]
#[CoversClass(ProduceResponsePartition::class)]
final class ProduceApiTest extends TestCase
{
    /**
     * One message set with a single message: no key, value "hello", offset 0.
     *
     *   Offset      => 00 00 00 00 00 00 00 00
     *   MessageSize => 00 00 00 13 (19 bytes)
     *   Crc         => 87 a7 7a b2 (CRC-32 of "MagicByte Attributes Key Value")
     *   MagicByte   => 00, Attributes => 00
     *   Key         => ff ff ff ff (null)
     *   Value       => 00 00 00 05 "hello"
     */
    private const string HELLO_MESSAGE_SET_HEX = '0000000000000000' . '00000013'
        . '87a77ab2' . '00' . '00' . 'ffffffff' . '00000005' . '68656c6c6f';

    /**
     * Header of a produce request for the topic "orders", client id "test", correlation id 5, timeout 1000 ms.
     *
     *   Size          => 00 00 00 4b (75 bytes)
     *   ApiKey        => 00 00 (Produce), ApiVersion => 00 01
     *   CorrelationId => 00 00 00 05, ClientId => 00 04 "test"
     */
    private const string REQUEST_HEADER_HEX = '0000004b' . '0000' . '0001' . '00000005' . '0004' . '74657374';

    /**
     * The same header with the api version 0 in it, the only byte a version 0 request differs in
     */
    private const string REQUEST_HEADER_V0_HEX = '0000004b' . '0000' . '0000' . '00000005' . '0004' . '74657374';

    /**
     * Everything after RequiredAcks: Timeout, one topic "orders" and its partition 0 with the message set above
     */
    private const string REQUEST_BODY_HEX = '000003e8'
        . '00000001' . '0006' . '6f7264657273'
        . '00000001' . '00000000' . '0000001f' . self::HELLO_MESSAGE_SET_HEX;

    public function testMessageSetOfTheFixtureFollowsTheSpecification(): void
    {
        self::assertSame(
            self::HELLO_MESSAGE_SET_HEX,
            bin2hex(SpecMessageSet::of([[null, 'hello']]))
        );
    }

    public function testRequestWithAcksOneWaitsForTheLocalLogOfTheLeader(): void
    {
        $request = $this->createRequest(1);

        self::assertSame(
            self::REQUEST_HEADER_HEX . '0001' . self::REQUEST_BODY_HEX,
            bin2hex((string) $request)
        );
        self::assertTrue($request->expectsResponse());
    }

    public function testRequestWithAcksMinusOneWaitsForAllInSyncReplicas(): void
    {
        $request = $this->createRequest(-1);

        self::assertSame(
            self::REQUEST_HEADER_HEX . 'ffff' . self::REQUEST_BODY_HEX,
            bin2hex((string) $request)
        );
        self::assertTrue($request->expectsResponse());
    }

    public function testRequestWithAcksZeroIsNeverAnsweredByTheBroker(): void
    {
        $request = $this->createRequest(0);

        self::assertSame(
            self::REQUEST_HEADER_HEX . '0000' . self::REQUEST_BODY_HEX,
            bin2hex((string) $request)
        );
        self::assertFalse($request->expectsResponse(), 'acks = 0 is the only request that the broker does not answer');
        self::assertSame(0, $request->getRequiredAcks());
    }

    public function testMessageSetIsCarriedAsAnOpaqueByteArrayOfAnyStringable(): void
    {
        // The message set of T3 is a Stringable, the request only prefixes its bytes with their int32 size
        $messageSet = new class (SpecMessageSet::of([[null, 'hello']])) implements \Stringable {
            public function __construct(private readonly string $buffer) {}

            public function __toString(): string
            {
                return $this->buffer;
            }
        };

        $request = new ProduceRequest(['orders' => [0 => $messageSet]], 1, 1000, 'test', 5);

        self::assertSame(self::REQUEST_HEADER_HEX . '0001' . self::REQUEST_BODY_HEX, bin2hex((string) $request));
    }

    public function testRequestPacksEveryTopicPartitionOfTheBatch(): void
    {
        $request = new ProduceRequest(
            [
                'orders' => [
                    0 => SpecMessageSet::of([[null, 'hello']]),
                    2 => SpecMessageSet::of([['key', 'world']]),
                ],
            ],
            1,
            1000,
            'test',
            5
        );

        //   Size => 00 00 00 75 (117 bytes), then the header, RequiredAcks 1, Timeout 1000, one topic with the
        //   partitions 0 and 2; the second message set carries the key "key" and the value "world"
        self::assertSame(
            '00000075' . '0000' . '0001' . '00000005' . '0004' . '74657374'
            . '0001' . '000003e8'
            . '00000001' . '0006' . '6f7264657273' . '00000002'
            . '00000000' . '0000001f' . self::HELLO_MESSAGE_SET_HEX
            . '00000002' . '00000022'
            . '0000000000000000' . '00000016'
            . '04568840' . '00' . '00' . '00000003' . '6b6579' . '00000005' . '776f726c64',
            bin2hex((string) $request)
        );
    }

    public function testVersion0RequestOnlyLowersTheApiVersionOfTheHeader(): void
    {
        $request = new ProduceRequestV0(
            ['orders' => [0 => SpecMessageSet::of([[null, 'hello']])]],
            1,
            1000,
            'test',
            5
        );

        self::assertSame(self::REQUEST_HEADER_V0_HEX . '0001' . self::REQUEST_BODY_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());
    }

    public function testResponseReportsTheBaseOffsetTheErrorAndTheThrottleTimeOfEveryPartition(): void
    {
        //   Size => 00 00 00 34 (52), CorrelationId => 3, one topic "orders" with two partitions:
        //   partition 0 => no error, base offset 42; partition 1 => error 6 (NotLeaderForPartition), offset -1,
        //   then ThrottleTime => 00 00 00 00 (no quota violation), which only version 1 carries
        $frame = hex2bin(
            '00000034' . '00000003'
            . '00000001' . '0006' . '6f7264657273' . '00000002'
            . '00000000' . '0000' . '000000000000002a'
            . '00000001' . '0006' . 'ffffffffffffffff'
            . '00000000'
        );

        $response = ProduceResponse::unpack(new StringStream($frame));

        self::assertSame(3, $response->getCorrelationId());
        self::assertSame(['orders'], array_keys($response->topics));
        self::assertSame(0, $response->throttleTime);

        $partitions = $response->topics['orders']->partitions;
        self::assertSame([0, 1], array_keys($partitions));
        self::assertSame(0, $partitions[0]->errorCode);
        self::assertSame(42, $partitions[0]->baseOffset);
        self::assertSame(6, $partitions[1]->errorCode, 'NotLeaderForPartition');
        self::assertSame(-1, $partitions[1]->baseOffset);
    }

    public function testThrottleTimeOfAQuotaViolationIsReadFromTheEndOfTheResponse(): void
    {
        //   The same answer with a single partition and ThrottleTime = 250 ms at its end
        $frame = hex2bin(
            '00000026' . '00000003'
            . '00000001' . '0006' . '6f7264657273' . '00000001'
            . '00000000' . '0000' . '000000000000002a'
            . '000000fa'
        );

        $response = ProduceResponse::unpack(new StringStream($frame));

        self::assertSame(250, $response->throttleTime);
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    public function testVersion0ResponseHasNoThrottleTimeAtAll(): void
    {
        //   The same answer as the version 1 one above, four bytes shorter: no ThrottleTime
        $frame = hex2bin(
            '00000022' . '00000003'
            . '00000001' . '0006' . '6f7264657273' . '00000001'
            . '00000000' . '0000' . '000000000000002a'
        );

        $response = ProduceResponseV0::unpack(new StringStream($frame));

        self::assertArrayNotHasKey('throttleTime', ProduceResponseV0::getScheme());
        self::assertSame(42, $response->topics['orders']->partitions[0]->baseOffset);
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    private function createRequest(int $requiredAcks): ProduceRequest
    {
        return new ProduceRequest(
            ['orders' => [0 => SpecMessageSet::of([[null, 'hello']])]],
            $requiredAcks,
            1000,
            'test',
            5
        );
    }
}
