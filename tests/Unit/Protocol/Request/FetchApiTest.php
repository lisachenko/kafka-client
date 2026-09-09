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
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\FetchRequestTopic;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Data\FetchResponseTopic;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV0;
use Protocol\Kafka\Protocol\Request\FetchRequestV1;
use Protocol\Kafka\Protocol\Request\FetchRequestV2;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV0;
use Protocol\Kafka\Protocol\Request\FetchResponseV1;
use Protocol\Kafka\Protocol\Request\FetchResponseV2;

/**
 * Byte-exact tests for the Fetch API, versions 0 to 3.
 *
 * <pre>
 *   FetchRequest v0, v1, v2 => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
 *   FetchRequest v3         => ReplicaId MaxWaitTime MinBytes MaxBytes [TopicName [Partition FetchOffset MaxBytes]]
 *   FetchResponse v0        => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
 *   FetchResponse v1 to v3  => ThrottleTimeMs [TopicName [...]]
 * </pre>
 *
 * @see docs/protocol/0.11.0.md, sections "Fetch API (key 1, v0 to v3)" and "MessageSet and Message"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchRequestV2::class)]
#[CoversClass(FetchRequestV1::class)]
#[CoversClass(FetchRequestV0::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchResponseV2::class)]
#[CoversClass(FetchResponseV1::class)]
#[CoversClass(FetchResponseV0::class)]
#[CoversClass(FetchRequestTopic::class)]
#[CoversClass(FetchRequestTopicPartition::class)]
#[CoversClass(FetchResponseTopic::class)]
#[CoversClass(FetchResponsePartition::class)]
final class FetchApiTest extends TestCase
{
    /**
     * Fetch request v3 for one topic and two of its partitions, client id "test", correlation id 1.
     *
     *   Size          => 00 00 00 4d (77 bytes)
     *   ApiKey        => 00 01
     *   ApiVersion    => 00 03
     *   CorrelationId => 00 00 00 01
     *   ClientId      => 00 04 "test"
     *   ReplicaId     => ff ff ff ff (-1, an ordinary consumer)
     *   MaxWaitTime   => 00 00 00 64 (100 ms)
     *   MinBytes      => 00 00 00 01
     *   MaxBytes      => 00 10 00 00 (1 MiB for the whole answer, since v3)
     *   [TopicName]   => 00 00 00 01, 00 05 "topic"
     *     [Partition] => 00 00 00 02
     *       0 => FetchOffset 0,  MaxBytes 1024
     *       1 => FetchOffset 42, MaxBytes 1024
     */
    private const string FETCH_REQUEST_HEX = '0000004d'
        . '0001'
        . '0003'
        . '00000001'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000064'
        . '00000001'
        . '00100000'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . '0000000000000000' . '00000400'
        . '00000001' . '000000000000002a' . '00000400';

    /**
     * The same request without the request-level MaxBytes, which is what the versions 0 to 2 send
     *
     *   Size => 00 00 00 49 (73 bytes), ApiVersion => 00 02
     */
    private const string FETCH_REQUEST_V2_HEX = '00000049'
        . '0001'
        . '0002'
        . '00000001'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000064'
        . '00000001'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . '0000000000000000' . '00000400'
        . '00000001' . '000000000000002a' . '00000400';

    /**
     * Message v0 without a key and with the value "hello", as it lies in the log:
     *
     *   Crc        => 87 a7 7a b2 (crc32 of the 15 bytes that follow)
     *   MagicByte  => 00
     *   Attributes => 00
     *   Key        => ff ff ff ff (null)
     *   Value      => 00 00 00 05 "hello"
     */
    private const string MESSAGE_HELLO_HEX = '87a77ab2' . '00' . '00' . 'ffffffff' . '00000005' . '68656c6c6f';

    /**
     * Message v0 with the key "k" and the value "world"
     */
    private const string MESSAGE_WORLD_HEX = 'a8aa1cff' . '00' . '00' . '00000001' . '6b' . '00000005' . '776f726c64';

    /**
     * MessageSet with two messages: offset 0 with 19 bytes of message, offset 1 with 20 bytes (63 bytes in total)
     */
    private const string MESSAGE_SET_HEX = '0000000000000000' . '00000013' . self::MESSAGE_HELLO_HEX
        . '0000000000000001' . '00000014' . self::MESSAGE_WORLD_HEX;

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new FetchRequest(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1, 1048576);

        self::assertSame(self::FETCH_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(77, $request->getMessageSize());
    }

    public function testTheRequestLevelMaxBytesDefaultsToTheFiftyMegabytesOfTheJavaConsumer(): void
    {
        $request = new FetchRequest(['topic' => [0 => 0]], 100, 1, 1024, -1, 'test', 1);

        self::assertSame(52428800, FetchRequest::DEFAULT_MAX_BYTES);
        // 00 03 20 00 00 = the 50 MiB of `fetch.max.bytes` behind MinBytes
        self::assertStringContainsString('00000001' . '03200000' . '00000001' . '0005746f706963', bin2hex((string) $request));
    }

    public function testTheOrderOfTheRequestedPartitionsIsKept(): void
    {
        // The broker fills the answer of a v3 request in the order of its partitions until MaxBytes are used up,
        // so a consumer that rotates them relies on this order reaching the wire unchanged
        $request = new FetchRequest(['topic' => [1 => 42, 0 => 0]], 100, 1, 1024, -1, 'test', 1, 1048576);

        self::assertStringEndsWith(
            '00000002'
            . '00000001' . '000000000000002a' . '00000400'
            . '00000000' . '0000000000000000' . '00000400',
            bin2hex((string) $request)
        );
    }

    public function testVersion2RequestIsTheVersionOneFrameWithAnotherApiVersion(): void
    {
        $request = new FetchRequestV2(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1);

        // Version 2 is the statement "I understand message format v1" and nothing else: the frame is the one of
        // version 1, without the request-level MaxBytes that version 3 added
        self::assertSame(self::FETCH_REQUEST_V2_HEX, bin2hex((string) $request));
        self::assertSame(2, $request->getApiVersion());
        self::assertArrayNotHasKey('maxBytes', FetchRequestV2::getScheme());
    }

    public function testVersion1RequestIsTheVersionTwoFrameWithAnotherApiVersion(): void
    {
        $request = new FetchRequestV1(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1);

        self::assertSame(substr_replace(self::FETCH_REQUEST_V2_HEX, '0001', 12, 4), bin2hex((string) $request));
        self::assertSame(1, $request->getApiVersion());
    }

    public function testRequestAcceptsStructuredTopicPartitions(): void
    {
        $request = FetchRequest::fromTopicPartitions(
            [
                [new TopicPartition('topic', 0), 0],
                [new TopicPartition('topic', 1), 42],
            ],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576
        );

        self::assertSame(self::FETCH_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testVersion0RequestOnlyLowersTheApiVersionOfTheHeader(): void
    {
        $request = new FetchRequestV0(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1);

        // The very same bytes, with the api version 0 in the header: the body of the request did not change until
        // version 3 added the request-level MaxBytes
        self::assertSame(
            substr_replace(self::FETCH_REQUEST_V2_HEX, '0000', 12, 4),
            bin2hex((string) $request)
        );
        self::assertSame(0, $request->getApiVersion());
    }

    public function testRequestSchemeCarriesNoFieldOfALaterVersion(): void
    {
        $scheme = FetchRequest::getScheme();

        // The request-level MaxBytes of v3 stands between MinBytes and the topics; the IsolationLevel of v4 and the
        // LogStartOffset of v5 belong to Kafka 0.11 and are absent
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'replicaId', 'maxWaitTime', 'minBytes', 'maxBytes', 'topicPartitions'],
            array_keys($scheme)
        );
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'replicaId', 'maxWaitTime', 'minBytes', 'topicPartitions'],
            array_keys(FetchRequestV2::getScheme())
        );
        self::assertSame(['topic' => FetchRequestTopic::class], $scheme['topicPartitions']);
        self::assertSame(
            ['partition' => BinarySchema::TYPE_INT32, 'fetchOffset' => BinarySchema::TYPE_INT64, 'maxBytes' => BinarySchema::TYPE_INT32],
            FetchRequestTopicPartition::getScheme()
        );
    }

    public function testResponseWithAnEmptyMessageSetIsUnpacked(): void
    {
        $response = FetchResponse::unpack(new StringStream(self::responseFrame('')));

        self::assertSame(1, $response->getCorrelationId());
        self::assertSame(0, $response->throttleTimeMs, 'a broker without quotas never throttles');
        self::assertSame(['topic'], array_keys($response->topics));

        $partition = $response->topics['topic']->partitions[0];
        self::assertSame(0, $partition->partition);
        self::assertSame(0, $partition->errorCode);
        self::assertSame(0, $partition->highWaterMarkOffset);
        self::assertSame('', $partition->messageSet);
    }

    public function testResponseWithTwoMessagesKeepsTheRawBytesOfTheMessageSet(): void
    {
        $response = FetchResponse::unpack(new StringStream(self::responseFrame(self::MESSAGE_SET_HEX, 0, 0, 2)));

        $partition = $response->topics['topic']->partitions[0];
        self::assertSame(2, $partition->highWaterMarkOffset);
        self::assertSame(self::MESSAGE_SET_HEX, bin2hex((string) $partition->messageSet));
        self::assertSame(63, strlen((string) $partition->messageSet));
    }

    public function testResponseKeepsThePartialTrailingMessageInTheBuffer(): void
    {
        // The broker cuts the message set at MaxBytes: the second entry breaks off after 10 of its 32 bytes
        $truncatedSet = substr(self::MESSAGE_SET_HEX, 0, 2 * (31 + 10));

        $response  = FetchResponse::unpack(new StringStream(self::responseFrame($truncatedSet, 0, 0, 2)));
        $partition = $response->topics['topic']->partitions[0];

        self::assertSame(41, strlen((string) $partition->messageSet));
        self::assertSame($truncatedSet, bin2hex((string) $partition->messageSet));
    }

    public function testResponseCarriesThePerPartitionErrorCode(): void
    {
        // Error code 1 is OffsetOutOfRange, the partition then comes back without any messages
        $response  = FetchResponse::unpack(new StringStream(self::responseFrame('', 0, 1, 5)));
        $partition = $response->topics['topic']->partitions[0];

        self::assertSame(1, $partition->errorCode);
        self::assertSame(5, $partition->highWaterMarkOffset);
        self::assertSame('', $partition->messageSet);
        self::assertFalse(
            $partition->isSingleMessageTooLarge(0),
            'a partition that failed says nothing about the size of its messages'
        );
    }

    public function testEmptyMessageSetBelowTheHighWaterMarkIsReportedAsAnOversizedMessage(): void
    {
        $response  = FetchResponse::unpack(new StringStream(self::responseFrame('', 0, 0, 7)));
        $partition = $response->topics['topic']->partitions[0];

        self::assertTrue($partition->isSingleMessageTooLarge(0), 'there are 7 messages to read but none fitted');
        self::assertFalse($partition->isSingleMessageTooLarge(7), 'nothing to read at the end of the log');
    }

    public function testMessageSetWithoutASingleCompleteMessageIsReportedAsAnOversizedMessage(): void
    {
        // What a 0.9.0.1 broker really answers when MaxBytes is smaller than the message: its first bytes only
        $firstBytesOnly = substr(self::MESSAGE_SET_HEX, 0, 2 * 20);

        $response  = FetchResponse::unpack(new StringStream(self::responseFrame($firstBytesOnly, 0, 0, 2)));
        $partition = $response->topics['topic']->partitions[0];

        self::assertSame(20, strlen((string) $partition->messageSet));
        self::assertTrue($partition->isSingleMessageTooLarge(0));
    }

    public function testMessageSetWithACompleteMessageAndAPartialOneMakesProgress(): void
    {
        $oneCompleteMessage = substr(self::MESSAGE_SET_HEX, 0, 2 * (31 + 10));

        $response  = FetchResponse::unpack(new StringStream(self::responseFrame($oneCompleteMessage, 0, 0, 2)));
        $partition = $response->topics['topic']->partitions[0];

        self::assertFalse(
            $partition->isSingleMessageTooLarge(0),
            'the first message did fit, so the consumer can advance and ask for the rest'
        );
    }

    public function testMessageSetIsDecodedByTheRecordLayer(): void
    {
        $response  = FetchResponse::unpack(new StringStream(self::responseFrame(self::MESSAGE_SET_HEX, 0, 0, 2)));
        $partition = $response->topics['topic']->partitions[0];

        self::assertSame(self::MESSAGE_SET_HEX, bin2hex((string) $partition->getMessageSet()));
        self::assertSame($partition->getMessageSet(), $partition->getMessageSet(), 'the message set is decoded once');
        self::assertSame([0, 1], array_map(
            static fn(Record $record): ?int => $record->offset,
            $partition->getMessageSet()->getRecords()
        ));
    }

    public function testThrottleTimeOpensTheResponseOfVersionOne(): void
    {
        // 250 ms of throttling, in front of the topics array
        $response = FetchResponse::unpack(new StringStream(self::responseFrame('', 0, 0, 0, 250)));

        self::assertSame(250, $response->throttleTimeMs);
        self::assertSame(['topic'], array_keys($response->topics));
    }

    public function testTheAnswerOfTheVersions1To3IsTheSameFrame(): void
    {
        $frame = self::responseFrame(self::MESSAGE_SET_HEX, 0, 0, 2);

        foreach ([FetchResponse::class, FetchResponseV2::class, FetchResponseV1::class] as $responseClass) {
            $response = $responseClass::unpack(new StringStream($frame));

            self::assertSame(
                ['messageSize', 'correlationId', 'throttleTimeMs', 'topics'],
                array_keys($responseClass::getScheme()),
                "{$responseClass} reads the throttle time between the header and the topics"
            );
            self::assertSame(2, $response->topics['topic']->partitions[0]->highWaterMarkOffset);
            self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
        }
    }

    public function testVersion0ResponseHasNoThrottleTimePrefix(): void
    {
        $frame = self::responseFrameV0(self::MESSAGE_SET_HEX, 0, 0, 2);

        $response = FetchResponseV0::unpack(new StringStream($frame));

        self::assertArrayNotHasKey('throttleTimeMs', FetchResponseV0::getScheme());
        self::assertSame(
            ['messageSize', 'correlationId', 'throttleTimeMs', 'topics'],
            array_keys(FetchResponse::getScheme()),
            'version 1 reads the throttle time between the header and the topics'
        );
        self::assertSame(self::MESSAGE_SET_HEX, bin2hex((string) $response->topics['topic']->partitions[0]->messageSet));
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    /**
     * Builds a Fetch response v1 frame with a single topic "topic" and a single partition
     *
     * @param string $messageSetHex Hexadecimal representation of the message set bytes of that partition
     */
    private static function responseFrame(
        string $messageSetHex,
        int $partition = 0,
        int $errorCode = 0,
        int $highWaterMarkOffset = 0,
        int $throttleTimeMs = 0
    ): string {
        $body = '00000001'                                   // CorrelationId
            . sprintf('%08x', $throttleTimeMs)               // ThrottleTimeMs, version 1 only
            . self::responseTopics($messageSetHex, $partition, $errorCode, $highWaterMarkOffset);

        return (string) hex2bin(sprintf('%08x', intdiv(strlen($body), 2)) . $body);
    }

    /**
     * Builds the same frame without the `ThrottleTimeMs` prefix, i.e. the answer of a version 0 request
     */
    private static function responseFrameV0(
        string $messageSetHex,
        int $partition = 0,
        int $errorCode = 0,
        int $highWaterMarkOffset = 0
    ): string {
        $body = '00000001'                                   // CorrelationId
            . self::responseTopics($messageSetHex, $partition, $errorCode, $highWaterMarkOffset);

        return (string) hex2bin(sprintf('%08x', intdiv(strlen($body), 2)) . $body);
    }

    /**
     * Builds the topics array of a Fetch response, which is the same in both versions
     */
    private static function responseTopics(
        string $messageSetHex,
        int $partition,
        int $errorCode,
        int $highWaterMarkOffset
    ): string {
        return '00000001'                                    // one topic
            . '0005' . '746f706963'                          // TopicName "topic"
            . '00000001'                                     // one partition
            . sprintf('%08x', $partition)
            . sprintf('%04x', $errorCode)
            . sprintf('%016x', $highWaterMarkOffset)
            . sprintf('%08x', intdiv(strlen($messageSetHex), 2))
            . $messageSetHex;
    }
}
