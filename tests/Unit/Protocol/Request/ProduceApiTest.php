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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ProduceRequestPartition;
use Protocol\Kafka\Protocol\Data\ProduceRequestTopic;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartitionV0;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartitionV2;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopic;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopicV0;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopicV2;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV0;
use Protocol\Kafka\Protocol\Request\ProduceRequestV1;
use Protocol\Kafka\Protocol\Request\ProduceRequestV2;
use Protocol\Kafka\Protocol\Request\ProduceRequestV3;
use Protocol\Kafka\Protocol\Request\ProduceRequestV4;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV0;
use Protocol\Kafka\Protocol\Request\ProduceResponseV1;
use Protocol\Kafka\Protocol\Request\ProduceResponseV2;
use Protocol\Kafka\Protocol\Request\ProduceResponseV3;
use Protocol\Kafka\Protocol\Request\ProduceResponseV4;
use Protocol\Kafka\Tests\Fixture\SpecMessageSet;

/**
 * Byte-exact tests of the Produce API, versions 0 to 5.
 *
 * <pre>
 *   ProduceRequest v0-v2 => RequiredAcks int16 Timeout int32 [TopicName [Partition int32 MessageSetSize int32
 *                                                                        MessageSet]]
 *   ProduceRequest v3-v5 => TransactionalId nullable_string RequiredAcks int16 Timeout int32
 *                           [TopicName [Partition int32 RecordSetSize int32 RecordSet]]
 *   ProduceResponse v0   => [TopicName [Partition int32 ErrorCode int16 Offset int64]]
 *   ProduceResponse v1   => [TopicName [Partition int32 ErrorCode int16 Offset int64]] ThrottleTime int32
 *   ProduceResponse v2-4 => [TopicName [Partition int32 ErrorCode int16 Offset int64 LogAppendTime int64]]
 *                           ThrottleTime int32
 *   ProduceResponse v5   => [TopicName [Partition int32 ErrorCode int16 Offset int64 LogAppendTime int64
 *                                       LogStartOffset int64]] ThrottleTime int32
 * </pre>
 *
 * The body of the request is the same in the versions 0 to 2 and gains the nullable `TransactionalId` in version 3,
 * which the versions 4 and 5 send unchanged; the answer of version 1 carries the throttle time of a quota violation
 * at its very end, version 2 puts the `LogAppendTime` the broker stamped the batch with behind the offset of every
 * partition, and version 5 (Kafka 1.0) appends the `LogStartOffset` of the partition to it. The versions 3 and 4
 * left the answer alone - `PRODUCE_RESPONSE_V4` is `PRODUCE_RESPONSE_V3` is `PRODUCE_RESPONSE_V2` @ 1.1.1.
 *
 * The message sets are built by {@see SpecMessageSet} directly from the specification and the record batch is a
 * captured one, so that the request classes are never checked against bytes they produced themselves.
 *
 * @see docs/protocol/2.8.md, sections "Produce API (key 0, v0 to v5)", "MessageSet and Message" and
 *      "RecordBatch (message format v2)"
 */
#[CoversClass(ProduceRequest::class)]
#[CoversClass(ProduceRequestV4::class)]
#[CoversClass(ProduceRequestV3::class)]
#[CoversClass(ProduceRequestV2::class)]
#[CoversClass(ProduceRequestV1::class)]
#[CoversClass(ProduceRequestV0::class)]
#[CoversClass(ProduceResponse::class)]
#[CoversClass(ProduceResponseV4::class)]
#[CoversClass(ProduceResponseV3::class)]
#[CoversClass(ProduceResponseV2::class)]
#[CoversClass(ProduceResponseV1::class)]
#[CoversClass(ProduceResponseV0::class)]
#[CoversClass(ProduceRequestTopic::class)]
#[CoversClass(ProduceRequestPartition::class)]
#[CoversClass(ProduceResponseTopic::class)]
#[CoversClass(ProduceResponseTopicV2::class)]
#[CoversClass(ProduceResponseTopicV0::class)]
#[CoversClass(ProduceResponsePartition::class)]
#[CoversClass(ProduceResponsePartitionV2::class)]
#[CoversClass(ProduceResponsePartitionV0::class)]
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
     * A record batch of the message format v2 with two records, the second of them with two headers.
     *
     * These are the bytes of the vector `messageformat.v2.none.headers`, i.e. a batch that the 0.11.0.3 broker
     * accepted and answered with; a version 3 request carries such a region where the lower versions carry a
     * message set, and it is the only shape that has a place for the headers of a record.
     */
    private const string RECORD_BATCH_HEX = '000000000000000000000090000000000277ad1da10000000000010000017487'
        . '6e800000000174876e800affffffffffffffffffffffffffff000000026c000000010a616c7068610418636f6e74656e742d74'
        . '797065206170706c69636174696f6e2f6a736f6e1074726163652d6964060001024e001402066b65790a627261766f0416656d'
        . '7074792d76616c756500146e756c6c2d76616c756501';

    /**
     * Header of a produce request for the topic "orders", client id "test", correlation id 5, timeout 1000 ms.
     *
     *   Size          => 00 00 00 4b (75 bytes)
     *   ApiKey        => 00 00 (Produce), ApiVersion => 00 02
     *   CorrelationId => 00 00 00 05, ClientId => 00 04 "test"
     */
    private const string REQUEST_HEADER_HEX = '0000004b' . '0000' . '0002' . '00000005' . '0004' . '74657374';

    /**
     * The same header with the api version 3 in it and two bytes more, the `ff ff` of a null `TransactionalId`
     */
    private const string REQUEST_HEADER_V3_HEX = '0000004d' . '0000' . '0003' . '00000005' . '0004' . '74657374';

    /**
     * The same header with the api version 4 in it, the only byte a version 4 request differs from a version 3 one
     */
    private const string REQUEST_HEADER_V4_HEX = '0000004d' . '0000' . '0004' . '00000005' . '0004' . '74657374';

    /**
     * The same header with the api version 5 in it, the version this client sends for the message format v2
     */
    private const string REQUEST_HEADER_V5_HEX = '0000004d' . '0000' . '0005' . '00000005' . '0004' . '74657374';

    /**
     * The same header with the api version 1 in it, the only byte a version 1 request differs in
     */
    private const string REQUEST_HEADER_V1_HEX = '0000004b' . '0000' . '0001' . '00000005' . '0004' . '74657374';

    /**
     * The same header with the api version 0 in it
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

        $request = new ProduceRequestV2(['orders' => [0 => $messageSet]], 1, 1000, 'test', 5);

        self::assertSame(self::REQUEST_HEADER_HEX . '0001' . self::REQUEST_BODY_HEX, bin2hex((string) $request));
    }

    public function testRequestPacksEveryTopicPartitionOfTheBatch(): void
    {
        $request = new ProduceRequestV2(
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
            '00000075' . '0000' . '0002' . '00000005' . '0004' . '74657374'
            . '0001' . '000003e8'
            . '00000001' . '0006' . '6f7264657273' . '00000002'
            . '00000000' . '0000001f' . self::HELLO_MESSAGE_SET_HEX
            . '00000002' . '00000022'
            . '0000000000000000' . '00000016'
            . '04568840' . '00' . '00' . '00000003' . '6b6579' . '00000005' . '776f726c64',
            bin2hex((string) $request)
        );
    }

    public function testVersion1RequestOnlyLowersTheApiVersionOfTheHeader(): void
    {
        $request = new ProduceRequestV1(
            ['orders' => [0 => SpecMessageSet::of([[null, 'hello']])]],
            1,
            1000,
            'test',
            5
        );

        // The body of the request has not changed since version 0, only the api version of the header selects
        // which answer the broker sends back
        self::assertSame(self::REQUEST_HEADER_V1_HEX . '0001' . self::REQUEST_BODY_HEX, bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
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

    public function testVersion3RequestPrefixesTheBodyWithANullTransactionalId(): void
    {
        $request = new ProduceRequestV3(
            ['orders' => [0 => SpecMessageSet::of([[null, 'hello']])]],
            1,
            1000,
            'test',
            5
        );

        // `ff ff` is the length -1 of a NULLABLE_STRING, i.e. "this producer is not transactional"; everything
        // behind it is the body of a version 2 request
        self::assertSame(
            self::REQUEST_HEADER_V3_HEX . 'ffff' . '0001' . self::REQUEST_BODY_HEX,
            bin2hex((string) $request)
        );
        self::assertSame(3, $request->getApiVersion());
        self::assertNull($request->getTransactionalId());
    }

    public function testTheVersionsThreeToFiveSendOneAndTheSameBody(): void
    {
        $frames = [];
        foreach ([3 => ProduceRequestV3::class, 4 => ProduceRequestV4::class, 5 => ProduceRequest::class] as $version => $requestClass) {
            $request          = new $requestClass(
                ['orders' => [0 => SpecMessageSet::of([[null, 'hello']])]],
                1,
                1000,
                'test',
                5
            );
            $frames[$version] = bin2hex((string) $request);
        }

        // `PRODUCE_REQUEST_V5` is `PRODUCE_REQUEST_V4` is `PRODUCE_REQUEST_V3` @ 1.1.1: what the two later
        // versions state is that the client understands the error code 56 (v4) and the LogStartOffset of the
        // answer (v5), not that the request looks different - only the api version of the header does
        $body = 'ffff' . '0001' . self::REQUEST_BODY_HEX;
        self::assertSame(self::REQUEST_HEADER_V3_HEX . $body, $frames[3]);
        self::assertSame(self::REQUEST_HEADER_V4_HEX . $body, $frames[4]);
        self::assertSame(self::REQUEST_HEADER_V5_HEX . $body, $frames[5]);
        self::assertSame(ProduceRequestV3::getScheme(), ProduceRequest::getScheme());
        self::assertSame(5, ProduceRequest::VERSION, 'the client sends version 5 for the message format v2');
    }

    public function testVersion3RequestCarriesTheTransactionalIdOfItsProducer(): void
    {
        $request = new ProduceRequestV3(
            ['orders' => [0 => hex2bin(self::RECORD_BATCH_HEX)]],
            -1,
            1000,
            'test',
            5,
            'tx-1'
        );

        //   Size => 00 00 00 ce (206), ApiVersion => 00 03, TransactionalId => 00 04 "tx-1", RequiredAcks => ff ff
        //   (all in-sync replicas, the only value a transaction is allowed to use), Timeout => 1000 ms, one topic
        //   "orders" whose partition 0 carries the 156 bytes of the record batch
        self::assertSame(
            '000000ce' . '0000' . '0003' . '00000005' . '0004' . '74657374'
            . '0004' . '74782d31' . 'ffff' . '000003e8'
            . '00000001' . '0006' . '6f7264657273'
            . '00000001' . '00000000' . '0000009c' . self::RECORD_BATCH_HEX,
            bin2hex((string) $request)
        );
        self::assertSame('tx-1', $request->getTransactionalId());
    }

    public function testVersion2RequestHasNoPlaceForATransactionalId(): void
    {
        self::assertArrayHasKey('transactionalId', ProduceRequest::getScheme());
        self::assertArrayNotHasKey('transactionalId', ProduceRequestV2::getScheme());
        self::assertArrayNotHasKey('transactionalId', ProduceRequestV1::getScheme());
        self::assertArrayNotHasKey('transactionalId', ProduceRequestV0::getScheme());

        // The transactional id is simply not written, which is what makes a version 2 request byte-identical to a
        // version 0 one apart from its api version
        $request = new ProduceRequestV2(
            ['orders' => [0 => SpecMessageSet::of([[null, 'hello']])]],
            1,
            1000,
            'test',
            5,
            'tx-1'
        );

        self::assertSame(self::REQUEST_HEADER_HEX . '0001' . self::REQUEST_BODY_HEX, bin2hex((string) $request));
    }

    public function testResponseReportsTheBaseOffsetTheErrorAndTheThrottleTimeOfEveryPartition(): void
    {
        //   Size => 00 00 00 44 (68), CorrelationId => 3, one topic "orders" with two partitions:
        //   partition 0 => no error, base offset 42, LogAppendTime -1; partition 1 => error 6
        //   (NotLeaderForPartition), offset -1, LogAppendTime -1, then ThrottleTime => 00 00 00 00 (no quota
        //   violation), which closes the answer of every version above 0
        $frame = hex2bin(
            '00000044' . '00000003'
            . '00000001' . '0006' . '6f7264657273' . '00000002'
            . '00000000' . '0000' . '000000000000002a' . 'ffffffffffffffff'
            . '00000001' . '0006' . 'ffffffffffffffff' . 'ffffffffffffffff'
            . '00000000'
        );

        $response = ProduceResponseV2::unpack(new StringStream($frame));

        self::assertSame(3, $response->getCorrelationId());
        self::assertSame(['orders'], array_keys($response->topics));
        self::assertSame(0, $response->throttleTime);

        $partitions = $response->topics['orders']->partitions;
        self::assertSame([0, 1], array_keys($partitions));
        self::assertSame(0, $partitions[0]->errorCode);
        self::assertSame(42, $partitions[0]->baseOffset);
        self::assertSame(-1, $partitions[0]->logAppendTime, 'the topic keeps the CreateTime of the producer');
        self::assertSame(6, $partitions[1]->errorCode, 'NotLeaderForPartition');
        self::assertSame(-1, $partitions[1]->baseOffset);
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    public function testLogAppendTimeOfVersionTwoIsReadBehindTheOffsetOfEveryPartition(): void
    {
        //   The same answer with a single partition whose LogAppendTime is 1489324800000 (2017-03-12T13:20:00Z), which is
        //   what a topic with message.timestamp.type=LogAppendTime answers
        $frame = hex2bin(
            '0000002e' . '00000003'
            . '00000001' . '0006' . '6f7264657273' . '00000001'
            . '00000000' . '0000' . '000000000000002a' . '0000015ac2acf800'
            . '00000000'
        );

        $response  = ProduceResponseV2::unpack(new StringStream($frame));
        $partition = $response->topics['orders']->partitions[0];

        self::assertSame(1489324800000, $partition->logAppendTime);
        self::assertNotSame(
            ProduceResponsePartition::NO_LOG_APPEND_TIME,
            $partition->logAppendTime,
            'the broker stamped the batch itself'
        );
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    public function testThrottleTimeOfAQuotaViolationIsReadFromTheEndOfTheResponse(): void
    {
        //   The same answer with a single partition and ThrottleTime = 250 ms at its end
        $frame = hex2bin(
            '0000002e' . '00000003'
            . '00000001' . '0006' . '6f7264657273' . '00000001'
            . '00000000' . '0000' . '000000000000002a' . 'ffffffffffffffff'
            . '000000fa'
        );

        $response = ProduceResponseV2::unpack(new StringStream($frame));

        self::assertSame(250, $response->throttleTime);
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    public function testTheAnswerOfTheVersionsThreeAndFourIsTheFrameOfAVersionTwoOne(): void
    {
        // `PRODUCE_RESPONSE_V4` is `PRODUCE_RESPONSE_V3` is `PRODUCE_RESPONSE_V2` in ProduceResponse.java @ 1.1.1,
        // and the broker really answers a version 3 and a version 4 request with that frame - the `LogStartOffset`
        // of the Produce answer is Kafka 1.0 (v5)
        $frame = hex2bin(
            '0000002e' . '00000003'
            . '00000001' . '0006' . '6f7264657273' . '00000001'
            . '00000000' . '0000' . '000000000000002a' . 'ffffffffffffffff'
            . '000000fa'
        );

        $version4 = ProduceResponseV4::unpack(new StringStream($frame));
        $version3 = ProduceResponseV3::unpack(new StringStream($frame));
        $version2 = ProduceResponseV2::unpack(new StringStream($frame));

        self::assertSame(ProduceResponseV4::getScheme(), ProduceResponseV2::getScheme());
        self::assertSame(ProduceResponseV3::getScheme(), ProduceResponseV2::getScheme());
        self::assertSame(250, $version4->throttleTime);
        self::assertSame(42, $version4->topics['orders']->partitions[0]->baseOffset);
        self::assertSame(42, $version3->topics['orders']->partitions[0]->baseOffset);
        self::assertSame(42, $version2->topics['orders']->partitions[0]->baseOffset);
        self::assertSame(
            ProduceResponsePartition::INVALID_OFFSET,
            $version4->topics['orders']->partitions[0]->logStartOffset,
            'a version below 5 does not report a log start offset at all'
        );
        self::assertSame($frame, (string) $version4, 'the response has to survive a round trip');
    }

    public function testVersion5AnswerAppendsTheLogStartOffsetToEveryPartition(): void
    {
        //   The version 2 answer with eight bytes more per partition: the LogStartOffset 17, i.e. the earliest
        //   offset the log of that partition still holds after a DeleteRecords or a retention run
        $frame = hex2bin(
            '00000036' . '00000003'
            . '00000001' . '0006' . '6f7264657273' . '00000001'
            . '00000000' . '0000' . '000000000000002a' . 'ffffffffffffffff' . '0000000000000011'
            . '00000000'
        );

        $response  = ProduceResponse::unpack(new StringStream($frame));
        $partition = $response->topics['orders']->partitions[0];

        self::assertSame(42, $partition->baseOffset);
        self::assertSame(-1, $partition->logAppendTime);
        self::assertSame(17, $partition->logStartOffset);
        self::assertSame(0, $response->throttleTime);
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    public function testAVersion5AnswerOfAnUntouchedLogReportsTheLogStartOffsetZero(): void
    {
        $frame = hex2bin(
            '00000036' . '00000003'
            . '00000001' . '0006' . '6f7264657273' . '00000001'
            . '00000000' . '0000' . '000000000000002a' . 'ffffffffffffffff' . '0000000000000000'
            . '00000000'
        );

        $response = ProduceResponse::unpack(new StringStream($frame));

        self::assertSame(0, $response->topics['orders']->partitions[0]->logStartOffset);
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    public function testVersion1ResponseHasNoLogAppendTimeInItsPartitions(): void
    {
        //   The version 2 answer above without the eight bytes of the LogAppendTime
        $frame = hex2bin(
            '00000026' . '00000003'
            . '00000001' . '0006' . '6f7264657273' . '00000001'
            . '00000000' . '0000' . '000000000000002a'
            . '000000fa'
        );

        $response = ProduceResponseV1::unpack(new StringStream($frame));

        self::assertArrayNotHasKey('logAppendTime', ProduceResponsePartitionV0::getScheme());
        self::assertSame(250, $response->throttleTime);
        self::assertSame(42, $response->topics['orders']->partitions[0]->baseOffset);
        self::assertSame(
            ProduceResponsePartition::NO_LOG_APPEND_TIME,
            $response->topics['orders']->partitions[0]->logAppendTime,
            'a version that does not report an append time leaves the field at -1'
        );
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

    public function testEveryVersionOfTheResponseReadsThePartitionEntryOfItsOwnVersion(): void
    {
        self::assertSame(
            ['partition' => BinarySchema::TYPE_INT32, 'errorCode' => BinarySchema::TYPE_INT16,
                'baseOffset' => BinarySchema::TYPE_INT64, 'logAppendTime' => BinarySchema::TYPE_INT64,
                'logStartOffset' => BinarySchema::TYPE_INT64],
            ProduceResponsePartition::getScheme()
        );
        self::assertSame(
            ['partition' => BinarySchema::TYPE_INT32, 'errorCode' => BinarySchema::TYPE_INT16,
                'baseOffset' => BinarySchema::TYPE_INT64, 'logAppendTime' => BinarySchema::TYPE_INT64],
            ProduceResponsePartitionV2::getScheme()
        );
        self::assertSame(
            ['partition' => BinarySchema::TYPE_INT32, 'errorCode' => BinarySchema::TYPE_INT16,
                'baseOffset' => BinarySchema::TYPE_INT64],
            ProduceResponsePartitionV0::getScheme()
        );
        self::assertSame(
            ['topic' => ProduceResponseTopic::class],
            ProduceResponse::getScheme()['topics'],
            'version 5 reads the partition entries with the LogStartOffset'
        );
        self::assertSame(
            ['topic' => ProduceResponseTopicV2::class],
            ProduceResponseV4::getScheme()['topics'],
            'the versions 2, 3 and 4 read the partition entries with the LogAppendTime alone'
        );
        self::assertSame(
            ['topic' => ProduceResponseTopicV2::class],
            ProduceResponseV3::getScheme()['topics']
        );
        self::assertSame(
            ['topic' => ProduceResponseTopicV2::class],
            ProduceResponseV2::getScheme()['topics']
        );
        self::assertSame(
            ['topic' => ProduceResponseTopicV0::class],
            ProduceResponseV1::getScheme()['topics']
        );
        self::assertSame(
            ['topic' => ProduceResponseTopicV0::class],
            ProduceResponseV0::getScheme()['topics']
        );
    }

    private function createRequest(int $requiredAcks): ProduceRequest
    {
        return new ProduceRequestV2(
            ['orders' => [0 => SpecMessageSet::of([[null, 'hello']])]],
            $requiredAcks,
            1000,
            'test',
            5
        );
    }
}
