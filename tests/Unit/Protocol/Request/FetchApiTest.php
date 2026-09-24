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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\OffsetMovedToTieredStorageException;
use Protocol\Kafka\Common\Errors\UnknownTopicIdException;
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\FetchRequestForgottenTopic;
use Protocol\Kafka\Protocol\Data\FetchRequestForgottenTopicV7;
use Protocol\Kafka\Protocol\Data\FetchRequestReplicaState;
use Protocol\Kafka\Protocol\Data\FetchRequestTopic;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartitionV0;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartitionV12;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartitionV17;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartitionV5;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartitionV9;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV0;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV12;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV13;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV17;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV5;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV9;
use Protocol\Kafka\Protocol\Data\FetchResponseAbortedTransaction;
use Protocol\Kafka\Protocol\Data\FetchResponseCurrentLeader;
use Protocol\Kafka\Protocol\Data\FetchResponseDivergingEpoch;
use Protocol\Kafka\Protocol\Data\FetchResponseNodeEndpoint;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Data\FetchResponsePartitionV0;
use Protocol\Kafka\Protocol\Data\FetchResponsePartitionV11;
use Protocol\Kafka\Protocol\Data\FetchResponsePartitionV4;
use Protocol\Kafka\Protocol\Data\FetchResponsePartitionV5;
use Protocol\Kafka\Protocol\Data\FetchResponseSnapshotId;
use Protocol\Kafka\Protocol\Data\FetchResponseTopic;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV0;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV11;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV12;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV4;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV0;
use Protocol\Kafka\Protocol\Request\FetchRequestV1;
use Protocol\Kafka\Protocol\Request\FetchRequestV10;
use Protocol\Kafka\Protocol\Request\FetchRequestV11;
use Protocol\Kafka\Protocol\Request\FetchRequestV12;
use Protocol\Kafka\Protocol\Request\FetchRequestV13;
use Protocol\Kafka\Protocol\Request\FetchRequestV14;
use Protocol\Kafka\Protocol\Request\FetchRequestV15;
use Protocol\Kafka\Protocol\Request\FetchRequestV16;
use Protocol\Kafka\Protocol\Request\FetchRequestV17;
use Protocol\Kafka\Protocol\Request\FetchRequestV2;
use Protocol\Kafka\Protocol\Request\FetchRequestV3;
use Protocol\Kafka\Protocol\Request\FetchRequestV4;
use Protocol\Kafka\Protocol\Request\FetchRequestV5;
use Protocol\Kafka\Protocol\Request\FetchRequestV6;
use Protocol\Kafka\Protocol\Request\FetchRequestV7;
use Protocol\Kafka\Protocol\Request\FetchRequestV8;
use Protocol\Kafka\Protocol\Request\FetchRequestV9;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV0;
use Protocol\Kafka\Protocol\Request\FetchResponseV1;
use Protocol\Kafka\Protocol\Request\FetchResponseV10;
use Protocol\Kafka\Protocol\Request\FetchResponseV11;
use Protocol\Kafka\Protocol\Request\FetchResponseV12;
use Protocol\Kafka\Protocol\Request\FetchResponseV13;
use Protocol\Kafka\Protocol\Request\FetchResponseV14;
use Protocol\Kafka\Protocol\Request\FetchResponseV15;
use Protocol\Kafka\Protocol\Request\FetchResponseV16;
use Protocol\Kafka\Protocol\Request\FetchResponseV17;
use Protocol\Kafka\Protocol\Request\FetchResponseV2;
use Protocol\Kafka\Protocol\Request\FetchResponseV3;
use Protocol\Kafka\Protocol\Request\FetchResponseV4;
use Protocol\Kafka\Protocol\Request\FetchResponseV5;
use Protocol\Kafka\Protocol\Request\FetchResponseV6;
use Protocol\Kafka\Protocol\Request\FetchResponseV7;
use Protocol\Kafka\Protocol\Request\FetchResponseV8;
use Protocol\Kafka\Protocol\Request\FetchResponseV9;
use Protocol\Kafka\Protocol\TaggedField;

/**
 * Byte-exact tests for the Fetch API, versions 0 to 7.
 *
 * <pre>
 *   FetchRequest v0, v1, v2 => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
 *   FetchRequest v3         => ReplicaId MaxWaitTime MinBytes MaxBytes [TopicName [Partition FetchOffset MaxBytes]]
 *   FetchRequest v4         => … MaxBytes IsolationLevel [TopicName [Partition FetchOffset MaxBytes]]
 *   FetchRequest v5, v6     => … MaxBytes IsolationLevel [TopicName [Partition FetchOffset LogStartOffset
 *                                                                    MaxBytes]]
 *   FetchRequest v7         => … MaxBytes IsolationLevel SessionId Epoch [TopicName [Partition FetchOffset
 *                               LogStartOffset MaxBytes]] [TopicName [Partition]]
 *   FetchResponse v0        => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
 *   FetchResponse v1 to v3  => ThrottleTimeMs [TopicName [...]]
 *   FetchResponse v4        => ThrottleTimeMs [TopicName [Partition ErrorCode HighwaterMarkOffset
 *                                                         LastStableOffset [AbortedTransactions] …]]
 *   FetchResponse v5, v6    => … HighwaterMarkOffset LastStableOffset LogStartOffset [AbortedTransactions] …
 *   FetchResponse v7        => ThrottleTimeMs ErrorCode SessionId [TopicName [...]]
 * </pre>
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)", "Fetch sessions (v7, KIP-227)",
 *      "The topic ids of the fetch path (v13, KIP-516)" and "MessageSet and Message"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchRequestV12::class)]
#[CoversClass(FetchRequestV13::class)]
#[CoversClass(FetchRequestV14::class)]
#[CoversClass(FetchRequestV15::class)]
#[CoversClass(FetchRequestV16::class)]
#[CoversClass(FetchRequestReplicaState::class)]
#[CoversClass(FetchRequestV6::class)]
#[CoversClass(FetchRequestV5::class)]
#[CoversClass(FetchRequestV4::class)]
#[CoversClass(FetchRequestV3::class)]
#[CoversClass(FetchRequestV2::class)]
#[CoversClass(FetchRequestV1::class)]
#[CoversClass(FetchRequestV0::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchResponseV12::class)]
#[CoversClass(FetchResponseV13::class)]
#[CoversClass(FetchResponseV14::class)]
#[CoversClass(FetchResponseV15::class)]
#[CoversClass(FetchResponseV16::class)]
#[CoversClass(FetchResponseNodeEndpoint::class)]
#[CoversClass(FetchResponseV6::class)]
#[CoversClass(FetchResponseV5::class)]
#[CoversClass(FetchResponseV4::class)]
#[CoversClass(FetchResponseV3::class)]
#[CoversClass(FetchResponseV2::class)]
#[CoversClass(FetchResponseV1::class)]
#[CoversClass(FetchResponseV0::class)]
#[CoversClass(FetchRequestTopic::class)]
#[CoversClass(FetchRequestTopicV0::class)]
#[CoversClass(FetchRequestTopicPartition::class)]
#[CoversClass(FetchRequestTopicPartitionV0::class)]
#[CoversClass(FetchResponseTopic::class)]
#[CoversClass(FetchResponseTopicV4::class)]
#[CoversClass(FetchResponseTopicV0::class)]
#[CoversClass(FetchResponsePartition::class)]
#[CoversClass(FetchResponsePartitionV4::class)]
#[CoversClass(FetchResponsePartitionV0::class)]
#[CoversClass(FetchResponseAbortedTransaction::class)]
#[CoversClass(FetchRequestForgottenTopic::class)]
#[CoversClass(FetchRequestForgottenTopicV7::class)]
#[CoversClass(FetchMetadata::class)]
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
    private const string FETCH_REQUEST_V3_HEX = '0000004d'
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
     * The same request as a version 5 one: the `IsolationLevel` byte of v4 behind `MaxBytes` and the
     * `LogStartOffset` of v5 in every partition entry, between `FetchOffset` and its `MaxBytes`.
     *
     *   Size           => 00 00 00 5e (94 bytes), ApiVersion => 00 05
     *   IsolationLevel => 00 (read_uncommitted)
     *   LogStartOffset => ff ff ff ff ff ff ff ff (-1, a consumer has no log of its own)
     */
    private const string FETCH_REQUEST_V5_HEX = '0000005e'
        . '0001'
        . '0005'
        . '00000001'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000064'
        . '00000001'
        . '00100000'
        . '00'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . '0000000000000000' . 'ffffffffffffffff' . '00000400'
        . '00000001' . '000000000000002a' . 'ffffffffffffffff' . '00000400';

    /**
     * A Fetch request of the version 12 this client sends, which is the first FLEXIBLE version of the api
     * (Kafka 2.7, KIP-482): the header is a request header **v2** - the client id is still a plain
     * `INT16` length plus the bytes, and a tagged-field section closes the header - every string, byte array
     * and array of the body is COMPACT, every structure ends in a tagged-field section, and the body carries
     * the `LastFetchedEpoch` of KIP-595 per partition and the tagged `ClusterId` at its end.
     *
     *   Size    => 00 00 00 76 (118 bytes), ApiVersion => 00 0c
     *   ClientId => 00 04 "test", header tag buffer => 00 (no tagged field in the header)
     *   [Topic]  => 02 (compact array of one entry), Name => 06 "topic" (compact string of five bytes)
     *   [Partition] => 03 (compact array of two entries)
     *     Partition => 00 00 00 00, CurrentLeaderEpoch => ff ff ff ff (-1, KIP-320)
     *     FetchOffset => 00 .. 00, LastFetchedEpoch => ff ff ff ff (-1, KIP-595)
     *     LogStartOffset => ff .. ff (-1), PartitionMaxBytes => 00 00 04 00, tag buffer => 00
     *   [ForgottenTopic] => 01 (the empty compact array), RackId => 01 (the empty compact string)
     *   tag buffer => 00 (no `ClusterId`, the tag 0 of the body)
     */
    private const string FETCH_REQUEST_HEX = '00000076'
        . '0001'
        . '000c'
        . '00000001'
        . '0004' . '74657374'
        . '00'
        . 'ffffffff'
        . '00000064'
        . '00000001'
        . '00100000'
        . '00'
        . '00000000' . 'ffffffff'
        . '02'
        . '06' . '746f706963'
        . '03'
        . '00000000' . 'ffffffff' . '0000000000000000' . 'ffffffff' . 'ffffffffffffffff' . '00000400' . '00'
        . '00000001' . 'ffffffff' . '000000000000002a' . 'ffffffff' . 'ffffffffffffffff' . '00000400' . '00'
        . '00'
        . '01'
        . '01'
        . '00';

    /**
     * The same question as a version 11 frame, the last plain one: the `SessionId` and the `Epoch` of
     * KIP-227 between the `IsolationLevel` and the topics, the `forgotten_topics_data` array behind them, the
     * `CurrentLeaderEpoch` of KIP-320 in front of the fetch offset of every partition, and the `RackId` of
     * KIP-392 at the very end of the frame.
     *
     *   Size           => 00 00 00 74 (116 bytes), ApiVersion => 00 0b
     *   SessionId      => 00 00 00 00 (no session), Epoch => ff ff ff ff (-1, FINAL_EPOCH)
     *   CurrentLeaderEpoch => ff ff ff ff (-1, "I do not know the epoch") on both partitions
     *   [ForgottenTopic] => 00 00 00 00 (nothing to forget)
     *   RackId         => 00 00 (the empty string, "I am in no rack")
     */
    private const string FETCH_REQUEST_V11_HEX = '00000074'
        . '0001'
        . '000b'
        . '00000001'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000064'
        . '00000001'
        . '00100000'
        . '00'
        . '00000000' . 'ffffffff'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . 'ffffffff' . '0000000000000000' . 'ffffffffffffffff' . '00000400'
        . '00000001' . 'ffffffff' . '000000000000002a' . 'ffffffffffffffff' . '00000400'
        . '00000000'
        . '0000';

    /**
     * The same question as a version 10 frame, which has no `RackId` at all: the frame of version 9 with another
     * api version, and the last one a consumer is always served by the leader itself for
     */
    private const string FETCH_REQUEST_V10_HEX = '00000072'
        . '0001'
        . '000a'
        . '00000001'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000064'
        . '00000001'
        . '00100000'
        . '00'
        . '00000000' . 'ffffffff'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . 'ffffffff' . '0000000000000000' . 'ffffffffffffffff' . '00000400'
        . '00000001' . 'ffffffff' . '000000000000002a' . 'ffffffffffffffff' . '00000400'
        . '00000000';

    /**
     * The same question as a version 9 frame: the api version is the only byte that differs from version 10
     */
    private const string FETCH_REQUEST_V9_HEX = '00000072'
        . '0001'
        . '0009'
        . '00000001'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000064'
        . '00000001'
        . '00100000'
        . '00'
        . '00000000' . 'ffffffff'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . 'ffffffff' . '0000000000000000' . 'ffffffffffffffff' . '00000400'
        . '00000001' . 'ffffffff' . '000000000000002a' . 'ffffffffffffffff' . '00000400'
        . '00000000';

    /**
     * The same question as a version 8 frame, whose partition entries carry no `CurrentLeaderEpoch` at all
     *
     *   Size => 00 00 00 6a (106 bytes), ApiVersion => 00 08
     */
    private const string FETCH_REQUEST_V8_HEX = '0000006a'
        . '0001'
        . '0008'
        . '00000001'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000064'
        . '00000001'
        . '00100000'
        . '00'
        . '00000000' . 'ffffffff'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . '0000000000000000' . 'ffffffffffffffff' . '00000400'
        . '00000001' . '000000000000002a' . 'ffffffffffffffff' . '00000400'
        . '00000000';

    /**
     * The very same frame with the api version 7 in its header, which is what a Kafka 1.1.1 broker was sent
     */
    private const string FETCH_REQUEST_V7_HEX = '0000006a'
        . '0001'
        . '0007'
        . '00000001'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000064'
        . '00000001'
        . '00100000'
        . '00'
        . '00000000' . 'ffffffff'
        . '00000001'
        . '0005' . '746f706963'
        . '00000002'
        . '00000000' . '0000000000000000' . 'ffffffffffffffff' . '00000400'
        . '00000001' . '000000000000002a' . 'ffffffffffffffff' . '00000400'
        . '00000000';

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

    /**
     * A record batch of the message format v2 with two records, the second of them with two headers.
     *
     * These are the bytes of the vector `messageformat.v2.none.headers`, the shape a partition of a Fetch v4 or v5
     * answer carries when the log holds the message format v2.
     */
    private const string RECORD_BATCH_HEX = '000000000000000000000090000000000277ad1da10000000000010000017487'
        . '6e800000000174876e800affffffffffffffffffffffffffff000000026c000000010a616c7068610418636f6e74656e742d74'
        . '797065206170706c69636174696f6e2f6a736f6e1074726163652d6964060001024e001402066b65790a627261766f0416656d'
        . '7074792d76616c756500146e756c6c2d76616c756501';

    public function testRequestIsPackedAccordingToTheSpec(): void
    {
        // Version 12, the last one that names its topics by name; the version 13 frame of the same fetch is in
        // {@see self::testVersionThirteenNamesEveryTopicByItsTopicId()}
        $request = new FetchRequestV12(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1, 1048576);

        self::assertSame(self::FETCH_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(
            118,
            $request->getMessageSize(),
            'the compact encoding of version 12 pays for the tag buffers with the bytes it saves on the lengths'
        );
        self::assertSame(FetchRequest::READ_UNCOMMITTED, $request->getIsolationLevel());
        self::assertEquals(
            FetchMetadata::legacy(),
            $request->getMetadata(),
            'a request without metadata is the session-less full fetch of every version below 7'
        );
        self::assertSame([], $request->getForgottenTopicPartitions());
    }

    public function testVersion7SendsTheSessionIdAndTheEpochBetweenTheIsolationLevelAndTheTopics(): void
    {
        $request = new FetchRequestV7(
            ['topic' => [0 => 0]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            new FetchMetadata(123, 4),
            ['topic' => [2, 3], 'other' => [0]]
        );

        //   Size => 00 00 00 74 (116 bytes), ApiVersion => 00 07, SessionId => 00 00 00 7b (123),
        //   Epoch => 00 00 00 04, one topic with the partition 0, then the two forgotten topics
        self::assertSame(
            '00000074' . '0001' . '0007' . '00000001' . '0004' . '74657374'
            . 'ffffffff' . '00000064' . '00000001' . '00100000' . '00'
            . '0000007b' . '00000004'
            . '00000001' . '0005' . '746f706963' . '00000001'
            . '00000000' . '0000000000000000' . 'ffffffffffffffff' . '00000400'
            . '00000002'
            . '0005' . '746f706963' . '00000002' . '00000002' . '00000003'
            . '0005' . '6f74686572' . '00000001' . '00000000',
            bin2hex((string) $request)
        );
        self::assertSame(123, $request->getMetadata()->sessionId);
        self::assertSame(4, $request->getMetadata()->epoch);
        self::assertSame(['topic' => [2, 3], 'other' => [0]], $request->getForgottenTopicPartitions());
    }

    public function testVersion8IsTheVersionSevenFrameWithAnotherApiVersion(): void
    {
        $request = new FetchRequestV8(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1, 1048576);

        // `FetchRequest.json` @ 2.8.2 has no field of version 8 and says "Version 8 is the same as version 7":
        // what version 8 (Kafka 2.0, KIP-219) states is that the client waits out `throttle_time_ms` itself,
        // because the broker answers a throttled request first and mutes the channel afterwards
        self::assertSame(self::FETCH_REQUEST_V7_HEX, substr_replace(self::FETCH_REQUEST_V8_HEX, '0007', 12, 4));
        self::assertSame(self::FETCH_REQUEST_V8_HEX, bin2hex((string) $request));
        self::assertSame(8, $request->getApiVersion());
        self::assertSame(FetchRequestV7::getScheme(), FetchRequestV8::getScheme());
        self::assertSame(FetchResponseV7::getScheme(), FetchResponseV8::getScheme());
        self::assertSame(7, FetchRequestV7::VERSION);
        self::assertSame(7, FetchResponseV7::VERSION);
    }

    public function testVersion9AddsTheCurrentLeaderEpochToEveryPartitionOfTheRequest(): void
    {
        // KIP-320: the epoch sits BETWEEN the partition index and the fetch offset - that is the field order of
        // `FetchRequest.json` @ 2.8.2, which is the wire order, not "behind the fetch offset" as the KIP reads
        $request = new FetchRequestV9(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1, 1048576);

        self::assertSame(self::FETCH_REQUEST_V9_HEX, bin2hex((string) $request));
        self::assertSame(
            self::FETCH_REQUEST_V8_HEX,
            str_replace(['0009', '00000000' . 'ffffffff' . '0000000000000000', '00000001' . 'ffffffff'], ['0008', '00000000' . '0000000000000000', '00000001'], substr_replace(self::FETCH_REQUEST_V9_HEX, '0000006a', 0, 8)),
            'a version 8 frame is the same question without the four epoch bytes per partition'
        );
        self::assertArrayNotHasKey('currentLeaderEpoch', FetchRequestTopicPartitionV5::getScheme());
        self::assertSame(
            ['partition', 'currentLeaderEpoch', 'fetchOffset', 'logStartOffset', 'maxBytes'],
            array_keys(FetchRequestTopicPartitionV9::getScheme())
        );
        self::assertSame(-1, FetchRequestTopicPartition::UNKNOWN_LEADER_EPOCH);
    }

    public function testAPartitionCanBeGivenAsAnOffsetAndEpochPair(): void
    {
        // The frozen shape of the optional epoch: a value of the `$topicPartitions` map is either the plain fetch
        // offset or the pair [offset, currentLeaderEpoch]
        $request = new FetchRequestV11(['topic' => [0 => [0, 7], 1 => 42]], 100, 1, 1024, -1, 'test', 1, 1048576);

        // The epoch of the first partition sits at hex offset 124, right behind its partition index
        self::assertSame(
            substr_replace(self::FETCH_REQUEST_V11_HEX, '00000007', 124, 8),
            bin2hex((string) $request),
            'the epoch of the first partition is 7, the second one keeps the -1 of a plain offset'
        );
        self::assertSame([42, -1], FetchRequest::offsetAndEpoch(42));
        self::assertSame([42, 7], FetchRequest::offsetAndEpoch([42, 7]));
    }

    public function testVersion10IsTheVersionNineFrameWithAnotherApiVersion(): void
    {
        // `FetchRequest.json` @ 2.8.2 has no field of version 10: what it states is that the client understands a
        // zstd-compressed record batch (KIP-110), which a broker refuses to a lower version with the code 76
        $request = new FetchRequestV10(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1, 1048576);

        self::assertSame(self::FETCH_REQUEST_V10_HEX, bin2hex((string) $request));
        self::assertSame(self::FETCH_REQUEST_V9_HEX, substr_replace(self::FETCH_REQUEST_V10_HEX, '0009', 12, 4));
        self::assertSame(10, $request->getApiVersion());
        self::assertSame(FetchRequestV9::getScheme(), FetchRequestV10::getScheme());
        self::assertSame(FetchResponseV9::getScheme(), FetchResponseV10::getScheme());
        self::assertSame(10, FetchRequestV10::VERSION);
        self::assertSame(10, FetchResponseV10::VERSION);
    }

    public function testVersion6RequestIsTheVersionFiveFrameWithAnotherApiVersion(): void
    {
        $request = new FetchRequestV6(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1, 1048576);

        // FETCH_REQUEST_V6 = FETCH_REQUEST_V5 @ 1.1.1: version 6 states that the client understands the error
        // code 56 and nothing else, so only the api version of the header differs
        self::assertSame(
            substr_replace(self::FETCH_REQUEST_V5_HEX, '0006', 12, 4),
            bin2hex((string) $request)
        );
        self::assertSame(6, $request->getApiVersion());
        self::assertSame(FetchRequestV5::getScheme(), FetchRequestV6::getScheme());
    }

    public function testVersion5RequestHasNeitherASessionNorForgottenTopics(): void
    {
        $request = new FetchRequestV5(
            ['topic' => [0 => 0, 1 => 42]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            new FetchMetadata(123, 4),
            ['topic' => [2]]
        );

        // A session that the version can not send is silently not written, exactly as an isolation level is not
        // written below version 4
        self::assertSame(self::FETCH_REQUEST_V5_HEX, bin2hex((string) $request));
        self::assertEquals(FetchMetadata::legacy(), $request->getMetadata());
        self::assertArrayNotHasKey('sessionId', FetchRequestV5::getScheme());
        self::assertArrayNotHasKey('epoch', FetchRequestV5::getScheme());
        self::assertArrayNotHasKey('forgottenTopics', FetchRequestV5::getScheme());
    }

    public function testVersionTwelveIsTheFirstFlexibleVersionOfTheApi(): void
    {
        // KIP-482 (Kafka 2.7): the same question as version 11, written with a request header v2, compact strings
        // and compact arrays, and a tagged-field section behind every structure
        $request = new FetchRequestV12(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1, 1048576);

        self::assertSame(self::FETCH_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(12, $request->getApiVersion());
        self::assertSame(12, FetchRequest::FLEXIBLE_VERSION);
        self::assertSame(12, FetchResponse::FLEXIBLE_VERSION);
        self::assertSame(11, FetchRequestV11::VERSION);
        self::assertSame(11, FetchResponseV11::VERSION);
        self::assertStringEndsWith(
            '01' . '01' . '00',
            bin2hex((string) $request),
            'the empty forgotten topics, the empty rack and the tag buffer of the body, one byte each'
        );
    }

    public function testTheClusterIdOfVersionTwelveIsATaggedFieldOfTheBody(): void
    {
        // `FetchRequest.json` @ 2.8.2 declares the `cluster_id` as the tag 0 with the default null: a request that
        // does not name one writes nothing at all, which is what every request of this client does
        $named = new FetchRequestV12(
            ['topic' => [0 => 0, 1 => 42]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            'cluster-1'
        );

        // The tagged-field section that closes the body: one field, the tag 0, ten bytes of it, and those ten
        // bytes are the compact string "cluster-1" (9 + 1)
        self::assertSame(
            substr_replace(substr(self::FETCH_REQUEST_HEX, 0, -2), '00000082', 0, 8)
            . '01' . '00' . '0a' . '0a' . '636c75737465722d31',
            bin2hex((string) $named),
            'the named cluster is the only difference from the frame that leaves the tag out, twelve bytes longer'
        );
        self::assertInstanceOf(TaggedField::class, FetchRequestV12::getScheme()['clusterId']);
        self::assertSame(0, FetchRequestV12::getScheme()['clusterId']->tag);
        self::assertNull(FetchRequestV12::getScheme()['clusterId']->default);
        self::assertArrayNotHasKey('clusterId', FetchRequestV11::getScheme());
    }

    public function testTheLastFetchedEpochOfKip595TravelsBehindTheFetchOffsetOfAPartition(): void
    {
        // The client-facing shape of the field: the third element of the triple, next to the plain offset and the
        // `[offset, currentLeaderEpoch]` pair of version 9
        $request = new FetchRequestV12(['topic' => [0 => [0, 3, 7]]], 100, 1, 1024, -1, 'test', 1, 1048576);

        self::assertSame(
            '00000055' . '0001' . '000c' . '00000001' . '0004' . '74657374' . '00'
            . 'ffffffff' . '00000064' . '00000001' . '00100000' . '00' . '00000000' . 'ffffffff'
            . '02' . '06' . '746f706963' . '02'
            . '00000000' . '00000003' . '0000000000000000' . '00000007' . 'ffffffffffffffff' . '00000400' . '00'
            . '00' . '01' . '01' . '00',
            bin2hex((string) $request),
            'the current leader epoch 3 in front of the fetch offset, the last fetched epoch 7 behind it'
        );
        self::assertSame(-1, FetchRequest::lastFetchedEpochOf(42));
        self::assertSame(-1, FetchRequest::lastFetchedEpochOf([42, 7]));
        self::assertSame(7, FetchRequest::lastFetchedEpochOf([42, 3, 7]));
        self::assertSame(-1, FetchRequestTopicPartition::UNKNOWN_LAST_FETCHED_EPOCH);
    }

    public function testADivergingEpochIsReadOutOfTheTaggedSectionOfAPartitionEntry(): void
    {
        // The bytes of the vector `fetch.response.v12.diverging-epoch`: the answer of a leader whose log does not
        // match the `last_fetched_epoch` and the fetch offset of the request. The record set is EMPTY, the error
        // code is 0, and the partition ends in a tagged-field section with the tag 0 alone
        $frame = '00000055' . '00000322' . '00'
            . '00000000' . '0000' . '00000000'
            . '02' . '0e' . '74322d32372d766563746f7273' . '02'
            . '00000000' . '0000' . '0000000000000001' . '0000000000000001' . '0000000000000000'
            . '00' . 'ffffffff' . '01'
            . '01' . '00' . '0d' . '00000000' . '0000000000000001' . '00'
            . '00' . '00';

        $response  = FetchResponseV12::unpack(new StringStream((string) hex2bin($frame)));
        $partition = $response->topics['t2-27-vectors']->partitions[0];

        self::assertSame(0, $partition->errorCode);
        self::assertSame('', $partition->messageSet);
        self::assertInstanceOf(FetchResponseDivergingEpoch::class, $partition->divergingEpoch);
        self::assertSame(0, $partition->divergingEpoch->epoch);
        self::assertSame(1, $partition->divergingEpoch->endOffset);
        self::assertNull($partition->currentLeader, 'the tag 1 belongs to the raft replication of a KRaft quorum');
        self::assertNull($partition->snapshotId, 'and so does the tag 2 of KIP-630');
        self::assertSame($frame, bin2hex((string) $response), 'a tagged field that is there travels back out');
        self::assertSame(-1, FetchResponseDivergingEpoch::UNDEFINED);
        self::assertSame(-1, FetchResponseCurrentLeader::UNKNOWN);
        self::assertSame(-1, FetchResponseSnapshotId::UNDEFINED);
        self::assertSame(
            ['epoch' => BinarySchema::TYPE_INT32, 'endOffset' => BinarySchema::TYPE_INT64],
            FetchResponseDivergingEpoch::getScheme()
        );
        self::assertSame(
            ['leaderId' => BinarySchema::TYPE_INT32, 'leaderEpoch' => BinarySchema::TYPE_INT32],
            FetchResponseCurrentLeader::getScheme()
        );
        self::assertSame(
            ['endOffset' => BinarySchema::TYPE_INT64, 'epoch' => BinarySchema::TYPE_INT32],
            FetchResponseSnapshotId::getScheme()
        );
    }

    public function testVersionElevenAppendsTheRackIdOfTheConsumerAndTheReadReplicaOfTheAnswer(): void
    {
        // KIP-392 (Kafka 2.3): the `rack_id` is the LAST field of the request, behind the forgotten topics, and
        // the `preferred_read_replica` sits between the aborted transactions and the record set of every
        // partition of the answer - the field order of `FetchRequest.json` and `FetchResponse.json` @ 2.8.2
        $request = new FetchRequestV11(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1, 1048576);

        self::assertSame(self::FETCH_REQUEST_V11_HEX, bin2hex((string) $request));
        self::assertStringEndsWith('00000000' . '0000', bin2hex((string) $request), 'the empty rack of a consumer');
        self::assertSame(11, $request->getApiVersion());
        self::assertSame('', FetchRequest::NO_RACK);

        $inRack = new FetchRequestV11(
            ['topic' => [0 => 0, 1 => 42]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            'eu-1a'
        );

        self::assertStringEndsWith('0005' . bin2hex('eu-1a'), bin2hex((string) $inRack));
        self::assertSame(
            strlen((string) $request) + 5,
            strlen((string) $inRack),
            'the rack is a plain string at the end of the frame'
        );

        $scheme = FetchRequestV11::getScheme();
        self::assertSame('rackId', array_key_last($scheme));
        self::assertArrayNotHasKey('rackId', FetchRequestV10::getScheme());

        $partition = FetchResponsePartitionV11::getScheme();
        self::assertSame(
            ['partition', 'errorCode', 'highWaterMarkOffset', 'lastStableOffset', 'logStartOffset',
                'abortedTransactions', 'preferredReadReplica', 'messageSet'],
            array_keys($partition)
        );
        self::assertArrayNotHasKey('preferredReadReplica', FetchResponsePartitionV5::getScheme());
        self::assertSame(-1, FetchResponsePartition::NO_PREFERRED_READ_REPLICA);
        self::assertSame(11, FetchRequestV11::VERSION);
        self::assertSame(11, FetchResponseV11::VERSION);
        self::assertSame(12, FetchRequestV12::VERSION);
        self::assertSame(12, FetchResponseV12::VERSION);
        self::assertSame(13, FetchRequestV13::VERSION, 'the topic ids of Kafka 3.1');
        self::assertSame(13, FetchResponseV13::VERSION);
        self::assertSame(14, FetchRequestV14::VERSION);
        self::assertSame(14, FetchResponseV14::VERSION);
        self::assertSame(15, FetchRequestV15::VERSION, 'the replica state of Kafka 3.5');
        self::assertSame(15, FetchResponseV15::VERSION);
        self::assertSame(16, FetchRequestV16::VERSION, 'the leader discovery of Kafka 3.7');
        self::assertSame(16, FetchResponseV16::VERSION);
        self::assertSame(17, FetchRequestV17::VERSION, 'the directory id of Kafka 3.9');
        self::assertSame(17, FetchResponseV17::VERSION);
        self::assertSame(18, FetchRequest::VERSION, 'the client sends the high watermark of Kafka 4.1');
        self::assertSame(18, FetchResponse::VERSION);
    }

    public function testTheIsolationLevelOfVersionFourIsWrittenBehindTheRequestLevelMaxBytes(): void
    {
        $request = new FetchRequestV11(
            ['topic' => [0 => 0, 1 => 42]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_COMMITTED
        );

        // The single byte 01 replaces the 00 of read_uncommitted, and nothing else about the frame changes
        self::assertSame(
            substr_replace(self::FETCH_REQUEST_V11_HEX, '01', 2 * 34, 2),
            bin2hex((string) $request)
        );
        self::assertSame(1, FetchRequest::READ_COMMITTED);
        self::assertSame(0, FetchRequest::READ_UNCOMMITTED);
        self::assertSame(FetchRequest::READ_COMMITTED, $request->getIsolationLevel());
    }

    public function testVersion4RequestCarriesTheIsolationLevelWithoutAPartitionLogStartOffset(): void
    {
        $request = new FetchRequestV4(
            ['topic' => [0 => 0, 1 => 42]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_COMMITTED
        );

        //   Size => 00 00 00 4e (78 bytes), i.e. the version 3 frame plus the single IsolationLevel byte
        self::assertSame(
            '0000004e' . '0001' . '0004' . '00000001' . '0004' . '74657374'
            . 'ffffffff' . '00000064' . '00000001' . '00100000' . '01'
            . '00000001' . '0005' . '746f706963' . '00000002'
            . '00000000' . '0000000000000000' . '00000400'
            . '00000001' . '000000000000002a' . '00000400',
            bin2hex((string) $request)
        );
    }

    public function testVersion3RequestHasNeitherAnIsolationLevelNorALogStartOffset(): void
    {
        $request = new FetchRequestV3(
            ['topic' => [0 => 0, 1 => 42]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_COMMITTED
        );

        // An isolation level that the version can not send is silently not written: a version 3 request always
        // reads uncommitted, because a 0.10 broker knew no transactions at all
        self::assertSame(self::FETCH_REQUEST_V3_HEX, bin2hex((string) $request));
        self::assertSame(FetchRequest::READ_UNCOMMITTED, $request->getIsolationLevel());
        self::assertArrayNotHasKey('isolationLevel', FetchRequestV3::getScheme());
    }

    public function testTheRequestLevelMaxBytesDefaultsToTheFiftyMegabytesOfTheJavaConsumer(): void
    {
        $request = new FetchRequestV11(['topic' => [0 => 0]], 100, 1, 1024, -1, 'test', 1);

        self::assertSame(52428800, FetchRequest::DEFAULT_MAX_BYTES);
        // 00 03 20 00 00 = the 50 MiB of `fetch.max.bytes` behind MinBytes, then the read_uncommitted byte
        self::assertStringContainsString(
            '00000001' . '03200000' . '00' . '00000000' . 'ffffffff' . '00000001' . '0005746f706963',
            bin2hex((string) $request)
        );
    }

    public function testTheOrderOfTheRequestedPartitionsIsKept(): void
    {
        // The broker fills the answer of a v3 request in the order of its partitions until MaxBytes are used up,
        // so a consumer that rotates them relies on this order reaching the wire unchanged
        $request = new FetchRequestV11(['topic' => [1 => 42, 0 => 0]], 100, 1, 1024, -1, 'test', 1, 1048576);

        self::assertStringEndsWith(
            '00000002'
            . '00000001' . 'ffffffff' . '000000000000002a' . 'ffffffffffffffff' . '00000400'
            . '00000000' . 'ffffffff' . '0000000000000000' . 'ffffffffffffffff' . '00000400'
            . '00000000'
            . '0000',
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
        $request = FetchRequestV12::fromTopicPartitions(
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

    public function testVersionThirteenNamesEveryTopicByItsTopicId(): void
    {
        // KIP-516 (Kafka 3.1): `FetchRequest.json` @ 3.1.2 declares the `Topic` of a topic entry and of a
        // forgotten-topic entry as `versions 0-12` and their `TopicId` as `13+`, so the same fetch as
        // {@see self::FETCH_REQUEST_HEX} carries the 16 raw bytes of the id where version 12 carries the compact
        // string `topic` - four bytes more, and no name anywhere in the frame
        $topicId = Uuid::fromString('mFOvIsGGQEaKUqOJUiHGrw');
        $request = new FetchRequestV13(
            ['topic' => [0 => 0, 1 => 42]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            ['topic' => $topicId]
        );

        self::assertSame(13, $request->getApiVersion());
        self::assertSame(['topic' => $topicId], $request->getTopicIds());
        // The version 12 frame with the api version raised, the ten bytes the id costs added to the Size and the
        // compact name `topic` (06 74 6f 70 69 63) replaced by the 16 raw bytes of the id
        $expected = str_replace('06' . '746f706963', bin2hex($topicId), self::FETCH_REQUEST_HEX);
        $expected = substr_replace($expected, '000d', 12, 4);
        $expected = substr_replace($expected, sprintf('%08x', (strlen($expected) - 8) / 2), 0, 8);

        self::assertSame($expected, bin2hex((string) $request));
        self::assertStringNotContainsString(
            bin2hex('topic'),
            bin2hex((string) $request),
            'the name of a topic is nowhere in a version 13 frame'
        );
        self::assertSame(
            strlen((string) hex2bin(self::FETCH_REQUEST_HEX)) + 10,
            strlen((string) $request),
            'a uuid is 16 raw bytes, the compact name of this topic was six'
        );
    }

    public function testVersionFourteenIsTheVersionThirteenFrameWithAnotherApiVersion(): void
    {
        // KIP-405 (Kafka 3.5): `FetchRequest.json` @ 3.5.2 declares no field of version 14 and says "Version 14
        // is the same as version 13 but it also receives a new error called OffsetMovedToTieredStorageException",
        // so the two frames differ in the api version of the header alone - what the version buys is the promise
        // to understand the error code 109 in a partition entry of the answer
        $topicId   = Uuid::fromString('mFOvIsGGQEaKUqOJUiHGrw');
        $arguments = [
            ['topic' => [0 => 0, 1 => 42]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            ['topic' => $topicId],
        ];
        $thirteen = bin2hex((string) new FetchRequestV13(...$arguments));
        $fourteen = bin2hex((string) new FetchRequestV14(...$arguments));

        self::assertSame($thirteen, substr_replace($fourteen, '000d', 12, 4));
        self::assertSame('000e', substr($fourteen, 12, 4));
        self::assertEquals(FetchRequestV13::getScheme(), FetchRequestV14::getScheme());
        self::assertEquals(FetchResponseV13::getScheme(), FetchResponseV14::getScheme());
        self::assertSame(
            109,
            KafkaException::OFFSET_MOVED_TO_TIERED_STORAGE,
            'the error code the version exists for, declared at the foundation of this line'
        );
        self::assertInstanceOf(
            OffsetMovedToTieredStorageException::class,
            KafkaException::fromCode(KafkaException::OFFSET_MOVED_TO_TIERED_STORAGE, ['topic' => 'topic'])
        );
    }

    public function testVersionFifteenReplacesTheReplicaIdWithTheTaggedReplicaState(): void
    {
        // KIP-903 (Kafka 3.5): `FetchRequest.json` @ 3.5.2 declares the top-level `ReplicaId` as
        // `"versions": "0-14"` and puts a `ReplicaState` of a replica id and a replica epoch in its place, as the
        // TAG 1 of the request. A consumer is the default -1 / -1 of that structure, and a tagged field whose
        // value is its default is not written at all: a version 15 consumer fetch is the version 14 frame minus
        // the four bytes of the old field
        $topicId   = Uuid::fromString('mFOvIsGGQEaKUqOJUiHGrw');
        $arguments = [
            ['topic' => [0 => 0, 1 => 42]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            ['topic' => $topicId],
        ];
        $fourteen = bin2hex((string) new FetchRequestV14(...$arguments));
        $fifteen  = bin2hex((string) new FetchRequestV15(...$arguments));

        // The api version, the size and the four bytes of the replica id are the whole difference: the body of a
        // version 15 request starts at `max_wait_ms`
        $expected = substr_replace($fourteen, '000f', 12, 4);
        $expected = str_replace('7465737400' . 'ffffffff' . '00000064', '7465737400' . '00000064', $expected);
        $expected = substr_replace($expected, sprintf('%08x', (strlen($expected) - 8) / 2), 0, 8);

        self::assertSame($expected, $fifteen);
        self::assertSame(
            strlen((string) hex2bin($fourteen)) - 4,
            strlen((string) hex2bin($fifteen)),
            'a consumer writes no replica state at all, so the frame is four bytes shorter'
        );
        self::assertStringEndsWith('0100', $fifteen, 'the empty rack of KIP-392 and an EMPTY tag buffer');
        self::assertSame(-1, new FetchRequestV15(...$arguments)->getReplicaId());
        self::assertNull(new FetchRequestV15(...$arguments)->getReplicaState());
        self::assertNull(
            new FetchRequestV14(...$arguments)->getReplicaState(),
            'no version below 15 has a replica state at all'
        );
    }

    public function testAFetchThatNamesAReplicaWritesTheStructureOfKip903(): void
    {
        $topicId   = Uuid::fromString('mFOvIsGGQEaKUqOJUiHGrw');
        $arguments = [
            ['topic' => [0 => 0]],
            100,
            1,
            1024,
            1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            ['topic' => $topicId],
            7,
        ];
        $request  = new FetchRequest(...$arguments);
        $consumer = new FetchRequest(...array_replace($arguments, [4 => -1, 14 => null]));

        // One tagged field: the count 01, the tag 01, the size 0d, and the 13 bytes of the structure - an int32
        // replica id, an int64 replica epoch and the tag buffer every structure of a flexible version ends in
        self::assertStringEndsWith(
            '01' . '01' . '0d' . '00000001' . '0000000000000007' . '00',
            bin2hex((string) $request)
        );
        self::assertSame(
            strlen((string) $consumer) + 15,
            strlen((string) $request),
            'the tagged field costs two bytes of framing, a length and thirteen of value, where the consumer'
            . ' writes the single byte of an empty section'
        );
        self::assertSame(1, $request->getReplicaId());
        self::assertSame(1, $request->getReplicaState()?->replicaId);
        self::assertSame(7, $request->getReplicaState()?->replicaEpoch);
        self::assertSame(
            ['replicaId' => BinarySchema::TYPE_INT32, 'replicaEpoch' => BinarySchema::TYPE_INT64],
            FetchRequestReplicaState::getScheme()
        );
        self::assertSame(-1, FetchRequestReplicaState::UNKNOWN);
        self::assertSame(15, FetchRequestReplicaState::VERSION);

        // The same request decoded again carries the structure, and re-encodes to the very same bytes
        $decoded = FetchRequest::unpack(new StringStream((string) $request));
        self::assertSame(1, $decoded->getReplicaState()?->replicaId);
        self::assertSame(7, $decoded->getReplicaState()?->replicaEpoch);
        self::assertSame(bin2hex((string) $request), bin2hex((string) $decoded));
    }

    public function testAVersionSixteenRequestIsTheVersionFifteenFrameWithAnotherApiVersion(): void
    {
        // KIP-951 (Kafka 3.7) left the request alone: `FetchRequest.json` @ 3.7.2 declares no field of version 16
        // and its whole comment is "Version 16 is the same as version 15 (KIP-951)"
        $topicId   = Uuid::fromString('mFOvIsGGQEaKUqOJUiHGrw');
        $arguments = [
            ['topic' => [0 => 0, 1 => 42]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            ['topic' => $topicId],
        ];

        $fifteen = bin2hex((string) new FetchRequestV15(...$arguments));
        $sixteen = bin2hex((string) new FetchRequestV16(...$arguments));

        self::assertEquals(FetchRequestV15::getScheme(), FetchRequestV16::getScheme());
        self::assertSame($fifteen, substr_replace($sixteen, '000f', 12, 4), 'only the api version differs');
        self::assertSame('00010010', substr($sixteen, 8, 8), 'the key 1 and the version 16 of the header');
        self::assertStringEndsWith('0100', $sixteen, 'the empty rack of KIP-392 and an EMPTY tag buffer');
    }

    public function testAVersionSeventeenRequestOfAConsumerIsTheVersionSixteenFrameWithAnotherApiVersion(): void
    {
        // KIP-853 (Kafka 3.9) added one TAGGED field to the partition entry - `FetchRequest.json` @ 3.9.2:
        // "Version 17 adds directory id support from KIP-853" - whose default is the zero uuid, and a tagged
        // field whose value is its default is not written at all, so the frame of a consumer does not change
        $topicId   = Uuid::fromString('mFOvIsGGQEaKUqOJUiHGrw');
        $arguments = [
            ['topic' => [0 => 0, 1 => 42]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            ['topic' => $topicId],
        ];

        $sixteen   = bin2hex((string) new FetchRequestV16(...$arguments));
        $seventeen = bin2hex((string) new FetchRequestV17(...$arguments));

        self::assertSame($sixteen, substr_replace($seventeen, '0010', 12, 4), 'only the api version differs');
        self::assertSame('00010011', substr($seventeen, 8, 8), 'the key 1 and the version 17 of the header');
        self::assertStringEndsWith('0100', $seventeen, 'the empty rack of KIP-392 and an EMPTY tag buffer');
        self::assertSame(Uuid::ZERO, new FetchRequestV17(...$arguments)->getReplicaDirectoryId());
        self::assertSame(Uuid::ZERO, new FetchRequestV16(...$arguments)->getReplicaDirectoryId());
        self::assertSame(
            FetchRequestTopicPartitionV17::class,
            FetchRequestTopicV17::partitionClass(),
            'the partition entry of version 17 is the one that declares the tag'
        );
        self::assertSame(FetchRequestTopicPartitionV12::class, FetchRequestTopicV13::partitionClass());
    }

    public function testTheReplicaDirectoryIdOfVersionSeventeenIsTheTagZeroOfEveryPartitionEntry(): void
    {
        // The field a FOLLOWER writes: 18 bytes at the end of the partition entry - the tag 0, the size 16 and
        // the 16 raw bytes of the uuid - and the tag buffer of that entry counts 01 instead of 00
        $topicId     = Uuid::fromString('mFOvIsGGQEaKUqOJUiHGrw');
        $directoryId = Uuid::fromString('UZcrwW8KpJUY7I9lFtsjSA');
        $arguments   = [
            ['topic' => [0 => 0]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            ['topic' => $topicId],
            null,
        ];

        $plain = new FetchRequest(...$arguments);
        $named = new FetchRequest(...[...$arguments, $directoryId]);

        self::assertSame($directoryId, $named->getReplicaDirectoryId());
        self::assertSame(
            strlen((string) $plain) + 18,
            strlen((string) $named),
            'the tag, its size and the uuid are the whole cost of the field'
        );
        self::assertStringContainsString(
            '01' . '00' . '10' . bin2hex($directoryId),
            bin2hex((string) $named),
            'one tagged field of the partition entry: the tag 0, the size 16 and the directory id'
        );
        self::assertSame(
            substr(bin2hex((string) $plain), 8),
            substr(str_replace('0100' . '10' . bin2hex($directoryId), '00', bin2hex((string) $named)), 8),
            'and without the tag the entry is the version 16 entry again'
        );

        // A version below 17 has no place for it at all: the value is carried and never written
        $below = new FetchRequestV16(...[...$arguments, $directoryId]);

        self::assertSame(strlen((string) new FetchRequestV16(...$arguments)), strlen((string) $below));
        self::assertSame(Uuid::ZERO, $below->getReplicaDirectoryId(), 'no version below 17 reports one');
        self::assertInstanceOf(
            TaggedField::class,
            FetchRequestTopicPartition::getScheme()['replicaDirectoryId'],
            'the field is declared as the tag 0 of the entry'
        );
        self::assertArrayNotHasKey('replicaDirectoryId', FetchRequestTopicPartitionV12::getScheme());
    }

    public function testAVersionEighteenRequestOfAConsumerIsTheVersionSeventeenFrameWithAnotherApiVersion(): void
    {
        // KIP-1166 (Kafka 4.1) added a second TAGGED field to the partition entry - `FetchRequest.json` @ 4.1.0:
        // "Version 18 adds high-watermark from KIP-1166" - whose default is Long.MAX_VALUE, "the feature is not
        // supported", and a consumer leaves it there, so its frame does not change again
        $arguments = [
            ['topic' => [0 => 0, 1 => [42, 3, 2]]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            ['topic' => Uuid::fromString('mFOvIsGGQEaKUqOJUiHGrw')],
        ];

        $seventeen = bin2hex((string) new FetchRequestV17(...$arguments));
        $eighteen  = bin2hex((string) new FetchRequest(...$arguments));

        self::assertSame($seventeen, substr_replace($eighteen, '0011', 12, 4), 'only the api version differs');
        self::assertSame('00010012', substr($eighteen, 8, 8), 'the key 1 and the version 18 of the header');
        self::assertSame(
            FetchRequestTopicPartition::class,
            FetchRequestTopic::partitionClass(),
            'the partition entry of version 18 is the one that declares the tag 1'
        );
        self::assertSame(FetchRequestTopicPartitionV17::class, FetchRequestTopicV17::partitionClass());
        self::assertSame(PHP_INT_MAX, FetchRequestTopicPartition::HIGH_WATERMARK_NOT_SUPPORTED);
        self::assertSame(-1, FetchRequestTopicPartition::UNKNOWN_HIGH_WATERMARK);
        self::assertSame(PHP_INT_MAX, FetchRequest::highWatermarkOf(0));
        self::assertSame(PHP_INT_MAX, FetchRequest::highWatermarkOf([42, 3, 2]));
        self::assertSame(1234, FetchRequest::highWatermarkOf([42, 3, 2, 1234]));
    }

    public function testTheHighWatermarkOfVersionEighteenIsTheTagOneOfEveryPartitionEntry(): void
    {
        // The field a FOLLOWER writes: 10 bytes at the end of the partition entry - the tag 1, the size 8 and the
        // int64 - and the tag buffer of that entry counts 01 instead of 00
        $arguments = [
            ['topic' => [0 => [42, 3, 2, 1234]]],
            100,
            1,
            1024,
            -1,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            ['topic' => Uuid::fromString('mFOvIsGGQEaKUqOJUiHGrw')],
        ];
        $consumer = $arguments;
        $consumer[0] = ['topic' => [0 => [42, 3, 2]]];

        $follower = bin2hex((string) new FetchRequest(...$arguments));
        $plain    = bin2hex((string) new FetchRequest(...$consumer));

        self::assertSame(strlen($plain) + 20, strlen($follower), 'the tag, its size and the int64 cost 10 bytes');
        self::assertStringContainsString(
            '01' . '01' . '08' . '00000000000004d2',
            $follower,
            'one tagged field of the partition entry: the tag 1, the size 8 and the high watermark 1234'
        );
        self::assertSame(
            substr($plain, 8),
            substr(str_replace('01' . '01' . '08' . '00000000000004d2', '00', $follower), 8),
            'and without the tag the entry is the consumer entry again'
        );

        // The directory id of KIP-853 goes first: the tags of a section are written in ascending order
        $directoryId = Uuid::fromString('UZcrwW8KpJUY7I9lFtsjSA');
        $both        = bin2hex((string) new FetchRequest(...[...$arguments, null, $directoryId]));
        self::assertStringContainsString(
            '02' . '00' . '10' . bin2hex($directoryId) . '01' . '08' . '00000000000004d2',
            $both,
            'two tagged fields: the tag 0 with the directory id, then the tag 1 with the high watermark'
        );

        // A version below 18 has no place for it at all: the value is carried and never written
        self::assertSame(
            bin2hex((string) new FetchRequestV17(...$consumer)),
            bin2hex((string) new FetchRequestV17(...$arguments))
        );
        self::assertInstanceOf(
            TaggedField::class,
            FetchRequestTopicPartition::getScheme()['highWatermark'],
            'the field is declared as the tag 1 of the entry'
        );
        self::assertArrayNotHasKey('highWatermark', FetchRequestTopicPartitionV17::getScheme());

        // and it survives a round trip through the decoder of the entry
        foreach ([1234 => 1234, FetchRequestTopicPartition::UNKNOWN_HIGH_WATERMARK => -1, PHP_INT_MAX => PHP_INT_MAX] as $value => $expected) {
            $stream = new StringStream();
            BinarySchema::writeObjectToStream(
                new FetchRequestTopicPartition(0, 42, 1048576, -1, 3, 2, Uuid::ZERO, $value),
                $stream,
                true
            );
            $stream = new StringStream($stream->getBuffer());
            /** @var FetchRequestTopicPartition $entry */
            $entry = BinarySchema::readObjectFromStream(FetchRequestTopicPartition::class, $stream, '', true);
            self::assertSame($expected, $entry->highWatermark);
        }
    }

    public function testAVersionEighteenAnswerIsTheVersionSeventeenAnswer(): void
    {
        // `FetchResponse.json` @ 4.1.0: "Version 18 no changes to the response (KIP-1166)"
        $body = '00000001' . '00'
            . '00000000' . '0000' . '00000000'
            . '02' . '9853af22c18640468a52a3895221c6af'
            . '02' . '00000000' . '0000' . '000000000000002a' . '000000000000002a' . '0000000000000000'
            . '00' . 'ffffffff' . '01' . '00'
            . '00'
            . '00';
        $frame = (string) hex2bin(sprintf('%08x', intdiv(strlen($body), 2)) . $body);

        self::assertEquals(FetchResponseV17::getScheme(), FetchResponse::getScheme());
        self::assertSame(
            bin2hex((string) FetchResponseV17::unpack(new StringStream($frame))),
            bin2hex((string) FetchResponse::unpack(new StringStream($frame)))
        );
    }

    public function testAVersionSeventeenAnswerIsTheVersionSixteenAnswer(): void
    {
        // `FetchResponse.json` @ 3.9.2: "Version 17 no changes to the response (KIP-853)"
        $body = '00000001' . '00'
            . '00000000' . '0000' . '00000000'
            . '02' . '9853af22c18640468a52a3895221c6af'
            . '02' . '00000000' . '0000' . '000000000000002a' . '000000000000002a' . '0000000000000000'
            . '00' . 'ffffffff' . '01' . '00'
            . '00'
            . '00';
        $frame = (string) hex2bin(sprintf('%08x', intdiv(strlen($body), 2)) . $body);

        $answer = FetchResponse::unpack(new StringStream($frame));

        self::assertEquals(FetchResponseV16::getScheme(), FetchResponse::getScheme());
        self::assertSame([], $answer->nodeEndpoints);
        self::assertSame(42, $answer->topics[0]->partitions[0]->highWaterMarkOffset);
        self::assertSame($frame, (string) $answer, 'the answer has to survive a round trip');
        self::assertSame(
            bin2hex($frame),
            bin2hex((string) FetchResponseV16::unpack(new StringStream($frame))),
            'and those bytes are the version 16 answer'
        );
    }

    public function testAVersionSixteenAnswerWithoutARefusedPartitionIsTheVersionFifteenAnswer(): void
    {
        // The `node_endpoints` of version 16 is a TAGGED field whose default is the empty array, so an answer
        // that refused nothing writes it not at all and ends in the empty tagged section of the body it has ended
        // in since version 12
        $body = '00000001' . '00'
            . '00000000' . '0000' . '00000000'
            . '02' . '9853af22c18640468a52a3895221c6af'
            . '02' . '00000000' . '0000' . '000000000000002a' . '000000000000002a' . '0000000000000000'
            . '00' . 'ffffffff' . '01' . '00'
            . '00'
            . '00';
        $frame = (string) hex2bin(sprintf('%08x', intdiv(strlen($body), 2)) . $body);

        $answer = FetchResponseV16::unpack(new StringStream($frame));

        self::assertSame([], $answer->nodeEndpoints, 'nothing was refused, so nothing is named');
        self::assertSame(42, $answer->topics[0]->partitions[0]->highWaterMarkOffset);
        self::assertSame($frame, (string) $answer, 'the answer has to survive a round trip');
        self::assertSame(
            bin2hex($frame),
            bin2hex((string) FetchResponseV15::unpack(new StringStream($frame))),
            'and those bytes are the version 15 answer'
        );
    }

    public function testAVersionSixteenAnswerNamesWhereTheLeaderOfARefusedPartitionCanBeReached(): void
    {
        // The shape KIP-951 exists for: a partition refused 74 FencedLeaderEpoch, whose tagged current_leader
        // (tag 1 of the entry, there since version 12) names the node 2 with the epoch 9, and the tagged
        // node_endpoints of the BODY (tag 0, new in version 16) that says where the node 2 can be reached
        $partition = '00000000' . '004a' . 'ffffffffffffffff' . 'ffffffffffffffff' . 'ffffffffffffffff'
            . '00' . 'ffffffff' . '01'
            . '01' . '01' . '09' . '00000002' . '00000009' . '00';
        $body = '00000001' . '00'
            . '00000000' . '0000' . '00000000'
            . '02' . '9853af22c18640468a52a3895221c6af' . '02' . $partition . '00'
            . '01' . '00' . '18' . '02' . '00000002' . '0962726f6b65722d32' . '00002384' . '057261636b' . '00';
        $frame = (string) hex2bin(sprintf('%08x', intdiv(strlen($body), 2)) . $body);

        $answer  = FetchResponse::unpack(new StringStream($frame));
        $refused = $answer->topics[0]->partitions[0];

        self::assertSame(KafkaException::FENCED_LEADER_EPOCH, $refused->errorCode);
        self::assertSame(2, $refused->currentLeader?->leaderId, 'the node the partition is really led by');
        self::assertSame(9, $refused->currentLeader?->leaderEpoch);
        self::assertSame([2], array_keys($answer->nodeEndpoints), 'the array is keyed by the node id');
        self::assertSame(2, $answer->nodeEndpoints[2]->nodeId);
        self::assertSame('broker-2', $answer->nodeEndpoints[2]->host);
        self::assertSame(9092, $answer->nodeEndpoints[2]->port);
        self::assertSame('rack', $answer->nodeEndpoints[2]->rack);
        self::assertSame(16, FetchResponseNodeEndpoint::VERSION);
        self::assertSame($frame, (string) $answer, 'the answer has to survive a round trip');
    }

    public function testTheDebuggingReplicaIdTravelsInTheReplicaStateOfVersionFifteen(): void
    {
        // -2 is the "read like a follower" id of every version below 15; from version 15 on it is a replica state
        // with the epoch -1, and the node serves such a fetch like a consumer's one, because
        // `FetchRequest.isFromFollower` @ 3.9.2 is `replicaId >= 0`
        $topicId = Uuid::fromString('mFOvIsGGQEaKUqOJUiHGrw');
        $request = new FetchRequest(
            ['topic' => [0 => 0]],
            100,
            1,
            1024,
            -2,
            'test',
            1,
            1048576,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            ['topic' => $topicId]
        );

        self::assertStringEndsWith(
            '01' . '01' . '0d' . 'fffffffe' . 'ffffffffffffffff' . '00',
            bin2hex((string) $request)
        );
        self::assertSame(-2, $request->getReplicaId());
        self::assertSame(-1, $request->getReplicaState()?->replicaEpoch);
    }

    public function testAVersionThirteenFetchOfATopicWithoutAnIdIsRefused(): void
    {
        // The whole point of KIP-516: there is no way to name a topic in a version 13 frame but its id, so a
        // client that does not know it refreshes its metadata instead of falling back to the name
        $this->expectException(UnknownTopicIdException::class);

        new FetchRequest(['topic' => [0 => 0]], 100, 1, 1024, -1, 'test', 1);
    }

    public function testTheTopicEntriesOfTheVersions12And13AreTheSameEntryWithAnotherName(): void
    {
        self::assertSame(
            ['topicId' => BinarySchema::TYPE_UUID, 'partitions' => ['partition' => FetchRequestTopicPartition::class]],
            FetchRequestTopic::getScheme()
        );
        self::assertSame(
            ['topic' => BinarySchema::TYPE_STRING,
                'partitions' => ['partition' => FetchRequestTopicPartitionV12::class]],
            FetchRequestTopicV12::getScheme(),
            'version 12 names the topic and picks the partition entry of the versions 12 to 16'
        );
        self::assertSame(
            ['topicId' => BinarySchema::TYPE_UUID,
                'partitions' => ['partition' => FetchRequestTopicPartitionV12::class]],
            FetchRequestTopicV13::getScheme(),
            'and so does the entry of the versions 13 to 16, which version 17 leaves for the tag of KIP-853'
        );
        self::assertSame(
            ['topicId' => BinarySchema::TYPE_UUID, 'partitions' => ['partition' => FetchResponsePartition::class]],
            FetchResponseTopic::getScheme()
        );
        self::assertSame(
            ['topic' => BinarySchema::TYPE_STRING, 'partitions' => ['partition' => FetchResponsePartition::class]],
            FetchResponseTopicV12::getScheme()
        );
        self::assertSame(18, FetchRequestTopic::VERSION, 'the entry of version 18 picks the tag of KIP-1166');
        self::assertSame(17, FetchRequestTopicV17::VERSION, 'the entry of version 17 picks the tag of KIP-853');
        self::assertSame(13, FetchRequestTopicV13::VERSION);
        self::assertSame(12, FetchRequestTopicV12::VERSION);
        self::assertSame(18, FetchRequestTopicPartition::VERSION);
        self::assertSame(17, FetchRequestTopicPartitionV17::VERSION);
        self::assertSame(12, FetchRequestTopicPartitionV12::VERSION);
        self::assertSame(13, FetchResponseTopic::VERSION);
        self::assertSame(12, FetchResponseTopicV12::VERSION);
        self::assertSame(13, FetchRequestForgottenTopic::VERSION);
        self::assertSame(7, FetchRequestForgottenTopicV7::VERSION);
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

    public function testTheLegacyMetadataIsTheSessionLessFullFetchOfEveryVersionBelowSeven(): void
    {
        $legacy = FetchMetadata::legacy();

        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $legacy->sessionId);
        self::assertSame(FetchMetadata::FINAL_EPOCH, $legacy->epoch);
        self::assertSame(0, FetchMetadata::INVALID_SESSION_ID);
        self::assertSame(-1, FetchMetadata::FINAL_EPOCH);
        self::assertTrue($legacy->isFull(), 'a session-less request always carries every partition');
        self::assertSame('(sessionId=INVALID, epoch=FINAL)', (string) $legacy);
    }

    public function testTheInitialMetadataAsksTheBrokerForANewSession(): void
    {
        $initial = FetchMetadata::initial();

        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $initial->sessionId);
        self::assertSame(FetchMetadata::INITIAL_EPOCH, $initial->epoch);
        self::assertSame(0, FetchMetadata::INITIAL_EPOCH);
        self::assertTrue($initial->isFull(), 'the request that creates a session is a full fetch');
        self::assertSame('(sessionId=INVALID, epoch=INITIAL)', (string) $initial);
    }

    public function testTheEpochOfASessionCountsUpAndNeverReachesZeroAgain(): void
    {
        $first  = FetchMetadata::newIncremental(4242);
        $second = $first->nextIncremental();

        self::assertSame(4242, $first->sessionId);
        self::assertSame(1, $first->epoch, 'the first incremental fetch of a session has the epoch 1');
        self::assertFalse($first->isFull());
        self::assertSame(2, $second->epoch);
        self::assertSame(4242, $second->sessionId);
        self::assertSame('(sessionId=4242, epoch=2)', (string) $second);

        // `FetchMetadata.nextEpoch` @ 1.1.1: the successor of FINAL_EPOCH is FINAL_EPOCH and the successor of
        // Integer.MAX_VALUE is 1, because the epoch 0 means "full fetch"
        self::assertSame(FetchMetadata::FINAL_EPOCH, FetchMetadata::nextEpoch(FetchMetadata::FINAL_EPOCH));
        self::assertSame(1, FetchMetadata::nextEpoch(2147483647));
        self::assertSame(1, FetchMetadata::nextEpoch(FetchMetadata::INITIAL_EPOCH));
    }

    public function testClosingAnExistingSessionKeepsItsIdAndGoesBackToTheInitialEpoch(): void
    {
        $closing = FetchMetadata::newIncremental(4242)->nextIncremental()->nextCloseExisting();

        // A full fetch that names a session closes it and creates a new one, which is how a client starts over
        // after the error codes 70 and 71 (`FetchSessionCache.newContext` @ 1.1.1)
        self::assertSame(4242, $closing->sessionId);
        self::assertSame(FetchMetadata::INITIAL_EPOCH, $closing->epoch);
        self::assertTrue($closing->isFull());
    }

    public function testEveryVersionOfTheRequestWritesExactlyTheFieldsItHas(): void
    {
        $scheme = FetchRequest::getScheme();

        // The request-level MaxBytes of v3 stands between MinBytes and the topics, the IsolationLevel of v4 behind
        // it, the SessionId and the Epoch of v7 behind that, the LogStartOffset of v5 inside a partition entry and
        // the forgotten topics of v7 behind the whole topics array. Version 12 puts the tagged-field section of
        // the request header v2 behind the client id and the tagged `ClusterId` of KIP-595 at the very end.
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'headerTaggedFields', 'maxWaitTime', 'minBytes', 'maxBytes', 'isolationLevel', 'sessionId', 'epoch', 'topicPartitions', 'forgottenTopics', 'rackId', 'clusterId', 'replicaState'],
            array_keys($scheme),
            'version 15 (KIP-903) took the replica id out of the body and put the tagged replica state at its end'
        );
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'headerTaggedFields', 'replicaId', 'maxWaitTime', 'minBytes', 'maxBytes', 'isolationLevel', 'sessionId', 'epoch', 'topicPartitions', 'forgottenTopics', 'rackId', 'clusterId'],
            array_keys(FetchRequestV14::getScheme()),
            'every version up to 14 carries the replica id as the first field of the body'
        );
        self::assertSame(
            array_keys(FetchRequestV14::getScheme()),
            array_keys(FetchRequestV13::getScheme()),
            'version 14 (KIP-405) added no field at all'
        );
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'replicaId', 'maxWaitTime', 'minBytes', 'maxBytes', 'isolationLevel', 'sessionId', 'epoch', 'topicPartitions', 'forgottenTopics', 'rackId'],
            array_keys(FetchRequestV11::getScheme()),
            'a version 11 frame has a plain header and no tagged field at all'
        );
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'replicaId', 'maxWaitTime', 'minBytes', 'maxBytes', 'isolationLevel', 'sessionId', 'epoch', 'topicPartitions', 'forgottenTopics'],
            array_keys(FetchRequestV10::getScheme()),
            'the rack id of KIP-392 arrived with version 11'
        );
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'replicaId', 'maxWaitTime', 'minBytes', 'maxBytes', 'isolationLevel', 'topicPartitions'],
            array_keys(FetchRequestV6::getScheme())
        );
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'replicaId', 'maxWaitTime', 'minBytes', 'maxBytes', 'isolationLevel', 'topicPartitions'],
            array_keys(FetchRequestV5::getScheme())
        );
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'replicaId', 'maxWaitTime', 'minBytes', 'maxBytes', 'topicPartitions'],
            array_keys(FetchRequestV3::getScheme())
        );
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'replicaId', 'maxWaitTime', 'minBytes', 'topicPartitions'],
            array_keys(FetchRequestV2::getScheme())
        );
        self::assertSame(BinarySchema::TYPE_INT8, $scheme['isolationLevel']);
        self::assertSame(BinarySchema::TYPE_INT32, $scheme['sessionId']);
        self::assertSame(BinarySchema::TYPE_INT32, $scheme['epoch']);
        self::assertSame([FetchRequestForgottenTopic::class], $scheme['forgottenTopics']);
        self::assertSame(
            ['topicId' => BinarySchema::TYPE_UUID, 'partitions' => [BinarySchema::TYPE_INT32]],
            FetchRequestForgottenTopic::getScheme(),
            'version 13 forgets a partition under the id of its topic (KIP-516)'
        );
        self::assertSame(
            ['topic' => BinarySchema::TYPE_STRING, 'partitions' => [BinarySchema::TYPE_INT32]],
            FetchRequestForgottenTopicV7::getScheme()
        );
        self::assertSame(
            [FetchRequestForgottenTopicV7::class],
            FetchRequestV12::getScheme()['forgottenTopics']
        );
        self::assertSame(
            [FetchRequestTopic::class],
            $scheme['topicPartitions'],
            'a version 13 entry has no name to index the array by'
        );
        self::assertSame(['topic' => FetchRequestTopicV12::class], FetchRequestV12::getScheme()['topicPartitions']);
        self::assertSame(
            ['topic' => FetchRequestTopicV9::class],
            FetchRequestV11::getScheme()['topicPartitions'],
            'below version 12 the partition entries carry no LastFetchedEpoch'
        );
        self::assertSame(
            ['topic' => FetchRequestTopicV5::class],
            FetchRequestV8::getScheme()['topicPartitions'],
            'below version 9 the partition entries carry no CurrentLeaderEpoch'
        );
        self::assertSame(
            ['topic' => FetchRequestTopicV0::class],
            FetchRequestV4::getScheme()['topicPartitions'],
            'below version 5 the partition entries carry no LogStartOffset'
        );
        self::assertSame(
            ['partition' => BinarySchema::TYPE_INT32, 'currentLeaderEpoch' => BinarySchema::TYPE_INT32,
                'fetchOffset' => BinarySchema::TYPE_INT64, 'lastFetchedEpoch' => BinarySchema::TYPE_INT32,
                'logStartOffset' => BinarySchema::TYPE_INT64, 'maxBytes' => BinarySchema::TYPE_INT32],
            FetchRequestTopicPartitionV12::getScheme(),
            'the LastFetchedEpoch of KIP-595 sits between the fetch offset and the log start offset'
        );
        self::assertSame(
            array_keys(FetchRequestTopicPartitionV12::getScheme() + ['replicaDirectoryId' => null]),
            array_keys(FetchRequestTopicPartitionV17::getScheme()),
            'and version 17 appends the tagged replica_directory_id of KIP-853 to that very entry'
        );
        self::assertSame(
            array_keys(FetchRequestTopicPartitionV17::getScheme() + ['highWatermark' => null]),
            array_keys(FetchRequestTopicPartition::getScheme()),
            'and version 18 the tagged high_watermark of KIP-1166 behind it'
        );
        self::assertSame(
            ['partition' => BinarySchema::TYPE_INT32, 'currentLeaderEpoch' => BinarySchema::TYPE_INT32,
                'fetchOffset' => BinarySchema::TYPE_INT64, 'logStartOffset' => BinarySchema::TYPE_INT64,
                'maxBytes' => BinarySchema::TYPE_INT32],
            FetchRequestTopicPartitionV9::getScheme()
        );
        self::assertSame(
            ['partition' => BinarySchema::TYPE_INT32, 'fetchOffset' => BinarySchema::TYPE_INT64,
                'logStartOffset' => BinarySchema::TYPE_INT64, 'maxBytes' => BinarySchema::TYPE_INT32],
            FetchRequestTopicPartitionV5::getScheme(),
            'the versions 5 to 8 carry the LogStartOffset of KIP-107 and no leader epoch'
        );
        self::assertSame(
            ['partition' => BinarySchema::TYPE_INT32, 'fetchOffset' => BinarySchema::TYPE_INT64, 'maxBytes' => BinarySchema::TYPE_INT32],
            FetchRequestTopicPartitionV0::getScheme()
        );
    }

    public function testResponseWithAnEmptyMessageSetIsUnpacked(): void
    {
        $response = FetchResponseV3::unpack(new StringStream(self::responseFrame('')));

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
        $response = FetchResponseV3::unpack(new StringStream(self::responseFrame(self::MESSAGE_SET_HEX, 0, 0, 2)));

        $partition = $response->topics['topic']->partitions[0];
        self::assertSame(2, $partition->highWaterMarkOffset);
        self::assertSame(self::MESSAGE_SET_HEX, bin2hex((string) $partition->messageSet));
        self::assertSame(63, strlen((string) $partition->messageSet));
    }

    public function testResponseKeepsThePartialTrailingMessageInTheBuffer(): void
    {
        // The broker cuts the message set at MaxBytes: the second entry breaks off after 10 of its 32 bytes
        $truncatedSet = substr(self::MESSAGE_SET_HEX, 0, 2 * (31 + 10));

        $response  = FetchResponseV3::unpack(new StringStream(self::responseFrame($truncatedSet, 0, 0, 2)));
        $partition = $response->topics['topic']->partitions[0];

        self::assertSame(41, strlen((string) $partition->messageSet));
        self::assertSame($truncatedSet, bin2hex((string) $partition->messageSet));
    }

    public function testResponseCarriesThePerPartitionErrorCode(): void
    {
        // Error code 1 is OffsetOutOfRange, the partition then comes back without any messages
        $response  = FetchResponseV3::unpack(new StringStream(self::responseFrame('', 0, 1, 5)));
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
        $response  = FetchResponseV3::unpack(new StringStream(self::responseFrame('', 0, 0, 7)));
        $partition = $response->topics['topic']->partitions[0];

        self::assertTrue($partition->isSingleMessageTooLarge(0), 'there are 7 messages to read but none fitted');
        self::assertFalse($partition->isSingleMessageTooLarge(7), 'nothing to read at the end of the log');
    }

    public function testMessageSetWithoutASingleCompleteMessageIsReportedAsAnOversizedMessage(): void
    {
        // What a 0.9.0.1 broker really answers when MaxBytes is smaller than the message: its first bytes only
        $firstBytesOnly = substr(self::MESSAGE_SET_HEX, 0, 2 * 20);

        $response  = FetchResponseV3::unpack(new StringStream(self::responseFrame($firstBytesOnly, 0, 0, 2)));
        $partition = $response->topics['topic']->partitions[0];

        self::assertSame(20, strlen((string) $partition->messageSet));
        self::assertTrue($partition->isSingleMessageTooLarge(0));
    }

    public function testMessageSetWithACompleteMessageAndAPartialOneMakesProgress(): void
    {
        $oneCompleteMessage = substr(self::MESSAGE_SET_HEX, 0, 2 * (31 + 10));

        $response  = FetchResponseV3::unpack(new StringStream(self::responseFrame($oneCompleteMessage, 0, 0, 2)));
        $partition = $response->topics['topic']->partitions[0];

        self::assertFalse(
            $partition->isSingleMessageTooLarge(0),
            'the first message did fit, so the consumer can advance and ask for the rest'
        );
    }

    public function testMessageSetIsDecodedByTheRecordLayer(): void
    {
        $response  = FetchResponseV3::unpack(new StringStream(self::responseFrame(self::MESSAGE_SET_HEX, 0, 0, 2)));
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
        $response = FetchResponseV3::unpack(new StringStream(self::responseFrame('', 0, 0, 0, 250)));

        self::assertSame(250, $response->throttleTimeMs);
        self::assertSame(['topic'], array_keys($response->topics));
    }

    public function testTheAnswerOfTheVersions1To3IsTheSameFrame(): void
    {
        $frame = self::responseFrame(self::MESSAGE_SET_HEX, 0, 0, 2);

        foreach ([FetchResponseV3::class, FetchResponseV2::class, FetchResponseV1::class] as $responseClass) {
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
            array_keys(FetchResponseV1::getScheme()),
            'version 1 reads the throttle time between the header and the topics'
        );
        self::assertSame(self::MESSAGE_SET_HEX, bin2hex((string) $response->topics['topic']->partitions[0]->messageSet));
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    public function testVersion5AnswerCarriesTheLastStableOffsetTheLogStartOffsetAndTheAbortedTransactions(): void
    {
        //   The partition header of version 5: partition 0, no error, HighwaterMarkOffset 12, LastStableOffset 9,
        //   LogStartOffset 4 and one aborted transaction of the producer 1000, which started at the offset 5
        $frame = self::responseFrameV5(
            self::MESSAGE_SET_HEX,
            12,
            9,
            4,
            [[1000, 5]]
        );

        $response  = FetchResponseV5::unpack(new StringStream($frame));
        $partition = $response->topics['topic']->partitions[0];

        self::assertSame(12, $partition->highWaterMarkOffset);
        self::assertSame(9, $partition->lastStableOffset);
        self::assertSame(4, $partition->logStartOffset, 'everything below the offset 4 has been deleted');
        self::assertCount(1, (array) $partition->abortedTransactions);
        self::assertSame(1000, $partition->abortedTransactions[0]->producerId);
        self::assertSame(5, $partition->abortedTransactions[0]->firstOffset);
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    public function testAnEmptyAbortedTransactionsArrayIsNotTheNullOfAReadUncommittedFetch(): void
    {
        // A read_committed fetch of a partition that no transaction ever touched: the array is there and empty
        $empty = FetchResponseV5::unpack(new StringStream(self::responseFrameV5('', 3, 3, 0, [])));
        // A read_uncommitted fetch: the broker does not compute the LSO at all and answers the count -1, `null`
        $null  = FetchResponseV5::unpack(new StringStream(self::responseFrameV5('', 3, -1, 0, null)));

        self::assertSame([], $empty->topics['topic']->partitions[0]->abortedTransactions);
        self::assertSame(3, $empty->topics['topic']->partitions[0]->lastStableOffset);
        self::assertNull($null->topics['topic']->partitions[0]->abortedTransactions);
        self::assertSame(
            FetchResponsePartition::INVALID_LAST_STABLE_OFFSET,
            $null->topics['topic']->partitions[0]->lastStableOffset
        );
        self::assertSame(
            self::responseFrameV5('', 3, -1, 0, null),
            (string) $null,
            'a null array is written back as the count -1, not as an empty one'
        );
    }

    public function testVersion4AnswerHasNoLogStartOffsetBetweenTheLastStableOffsetAndTheTransactions(): void
    {
        $frame = self::responseFrameV4('', 12, 9, [[1000, 5]]);

        $response  = FetchResponseV4::unpack(new StringStream($frame));
        $partition = $response->topics['topic']->partitions[0];

        self::assertSame(9, $partition->lastStableOffset);
        self::assertSame(1000, $partition->abortedTransactions[0]->producerId);
        self::assertSame(
            FetchResponsePartition::INVALID_LOG_START_OFFSET,
            $partition->logStartOffset,
            'a version that does not report a log start offset leaves the field at -1'
        );
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    public function testTheAnswerOfAVersionSixRequestIsTheVersionFiveFrame(): void
    {
        // FETCH_RESPONSE_V6 = FETCH_RESPONSE_V5 @ 1.1.1: version 6 only states that the client understands the
        // error code 56, and the top-level error code and session id of version 7 are not on the wire yet
        $frame = self::responseFrameV5(self::MESSAGE_SET_HEX, 2, 2, 0, []);

        $version6 = FetchResponseV6::unpack(new StringStream($frame));

        self::assertSame(FetchResponseV5::getScheme(), FetchResponseV6::getScheme());
        self::assertSame(2, $version6->topics['topic']->partitions[0]->highWaterMarkOffset);
        self::assertSame(0, $version6->errorCode, 'a version below 7 leaves the session error code at zero');
        self::assertSame(0, $version6->sessionId);
        self::assertSame($frame, (string) $version6, 'the response has to survive a round trip');
    }

    public function testVersion7AnswerCarriesTheSessionErrorCodeAndTheSessionIdBehindTheThrottleTime(): void
    {
        //   ThrottleTimeMs 0, ErrorCode 0, SessionId 0x2a2a2a2a, one topic with one partition
        $frame = self::responseFrameV7(self::MESSAGE_SET_HEX, 2, 0, 707406378);

        $response = FetchResponseV7::unpack(new StringStream($frame));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(0, $response->errorCode);
        self::assertSame(707406378, $response->sessionId);
        self::assertSame(2, $response->topics['topic']->partitions[0]->highWaterMarkOffset);
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    public function testASessionLessVersion7AnswerReportsTheSessionIdZero(): void
    {
        $response = FetchResponseV7::unpack(new StringStream(self::responseFrameV7(self::MESSAGE_SET_HEX, 2)));

        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $response->sessionId);
        self::assertSame(0, $response->errorCode);
    }

    public function testASessionErrorIsAnsweredWithAnEmptyTopicsArray(): void
    {
        //   The two answers of a broken session: 70 FETCH_SESSION_ID_NOT_FOUND for a session id the broker does
        //   not know and 71 INVALID_FETCH_SESSION_EPOCH for an epoch that does not match. `SessionErrorContext`
        //   @ 1.1.1 answers both with an empty topics array and the session id 0.
        $unknownSession = '00000012' . '00000001' . '00000000' . '0046' . '00000000' . '00000000';
        $wrongEpoch     = '00000012' . '00000001' . '00000000' . '0047' . '00000000' . '00000000';

        $notFound = FetchResponseV7::unpack(new StringStream((string) hex2bin($unknownSession)));
        $invalid  = FetchResponseV7::unpack(new StringStream((string) hex2bin($wrongEpoch)));

        self::assertSame(70, $notFound->errorCode);
        self::assertSame(0, $notFound->sessionId);
        self::assertSame([], $notFound->topics);
        self::assertSame(71, $invalid->errorCode);
        self::assertSame([], $invalid->topics);
        self::assertSame($unknownSession, bin2hex((string) $notFound));
        self::assertSame($wrongEpoch, bin2hex((string) $invalid));
    }

    public function testEveryVersionOfTheResponseReadsThePartitionEntryOfItsOwnVersion(): void
    {
        self::assertSame(
            ['messageSize', 'correlationId', 'headerTaggedFields', 'throttleTimeMs', 'errorCode', 'sessionId',
                'topics', 'nodeEndpoints'],
            array_keys(FetchResponse::getScheme()),
            'a version 12 answer comes in a response header v1, whose tagged-field section follows the correlation '
            . 'id, and version 16 declares the tagged node_endpoints of the body (KIP-951)'
        );
        self::assertSame(
            ['messageSize', 'correlationId', 'headerTaggedFields', 'throttleTimeMs', 'errorCode', 'sessionId', 'topics'],
            array_keys(FetchResponseV15::getScheme()),
            'no version below 16 has a field of the body above the topics array'
        );
        self::assertSame(
            array_keys(FetchResponseV15::getScheme()),
            array_keys(FetchResponseV12::getScheme()),
            'version 13 changed the topic entry alone, not a field of the body'
        );
        self::assertSame(
            ['messageSize', 'correlationId', 'throttleTimeMs', 'errorCode', 'sessionId', 'topics'],
            array_keys(FetchResponseV11::getScheme())
        );
        self::assertSame(
            ['messageSize', 'correlationId', 'throttleTimeMs', 'topics'],
            array_keys(FetchResponseV6::getScheme())
        );

        self::assertSame(
            ['partition', 'errorCode', 'highWaterMarkOffset', 'lastStableOffset', 'logStartOffset',
                'abortedTransactions', 'preferredReadReplica', 'messageSet',
                'divergingEpoch', 'currentLeader', 'snapshotId'],
            array_keys(FetchResponsePartition::getScheme()),
            'the three tagged fields of version 12 are declared behind the record set, in the order of their tag'
        );
        self::assertSame(
            ['partition', 'errorCode', 'highWaterMarkOffset', 'lastStableOffset', 'logStartOffset',
                'abortedTransactions', 'preferredReadReplica', 'messageSet'],
            array_keys(FetchResponsePartitionV11::getScheme())
        );
        self::assertSame(
            ['partition', 'errorCode', 'highWaterMarkOffset', 'lastStableOffset', 'logStartOffset',
                'abortedTransactions', 'messageSet'],
            array_keys(FetchResponsePartitionV5::getScheme()),
            'the preferred read replica of KIP-392 arrived with version 11'
        );
        self::assertSame(
            ['partition', 'errorCode', 'highWaterMarkOffset', 'lastStableOffset', 'abortedTransactions',
                'messageSet'],
            array_keys(FetchResponsePartitionV4::getScheme())
        );
        self::assertSame(
            ['partition', 'errorCode', 'highWaterMarkOffset', 'messageSet'],
            array_keys(FetchResponsePartitionV0::getScheme())
        );
        self::assertSame(
            [FetchResponseAbortedTransaction::class, BinarySchema::FLAG_NULLABLE => true],
            FetchResponsePartition::getScheme()['abortedTransactions'],
            'the aborted transactions are a nullable array of structures, not a keyed one'
        );
        self::assertSame(
            [FetchResponseTopic::class],
            FetchResponse::getScheme()['topics'],
            'a version 13 entry is named by its topic id alone, so nothing indexes the array (KIP-516)'
        );
        self::assertSame(['topic' => FetchResponseTopicV12::class], FetchResponseV12::getScheme()['topics']);
        self::assertSame(['topic' => FetchResponseTopicV11::class], FetchResponseV11::getScheme()['topics']);
        self::assertSame(['topic' => FetchResponseTopicV4::class], FetchResponseV4::getScheme()['topics']);
        self::assertSame(['topic' => FetchResponseTopicV0::class], FetchResponseV3::getScheme()['topics']);
        self::assertSame(['topic' => FetchResponseTopicV0::class], FetchResponseV0::getScheme()['topics']);
    }

    public function testTheRecordLayerReadsWhicheverMessageFormatThePartitionCameBackIn(): void
    {
        $legacy = FetchResponseV3::unpack(new StringStream(self::responseFrame(self::MESSAGE_SET_HEX, 0, 0, 2)))
            ->topics['topic']->partitions[0];
        $batch  = FetchResponseV5::unpack(new StringStream(self::responseFrameV5(self::RECORD_BATCH_HEX, 2, 2, 0, [])))
            ->topics['topic']->partitions[0];

        self::assertSame(Message::MAGIC_V0, $legacy->getRecords()->getMagic());
        self::assertSame([0, 1], array_map(
            static fn(Record $record): ?int => $record->offset,
            $legacy->getRecords()->getRecords()
        ));

        self::assertSame(RecordBatch::MAGIC, $batch->getRecords()->getMagic());
        self::assertSame($batch->getRecords(), $batch->getRecords(), 'the region is decoded once');
        $records = $batch->getRecords()->getRecords();
        self::assertCount(2, $records);
        self::assertSame('alpha', $records[0]->value);
        self::assertSame(['content-type', 'trace-id'], array_map(
            static fn(Header $header): string => $header->key,
            $records[0]->headers
        ), 'the headers of a record only exist in the message format v2');
    }

    /**
     * Builds a Fetch response v7 frame with a single topic "topic" and a single partition
     *
     * @param string $recordSetHex Hex of the record set bytes of that partition
     */
    private static function responseFrameV7(
        string $recordSetHex,
        int $highWaterMarkOffset = 0,
        int $sessionErrorCode = 0,
        int $sessionId = 0
    ): string {
        $body = '00000001' . '00000000' . sprintf('%04x', $sessionErrorCode) . sprintf('%08x', $sessionId)
            . '00000001' . '0005' . '746f706963' . '00000001'
            . '00000000' . '0000' . sprintf('%016x', $highWaterMarkOffset)
            . self::int64($highWaterMarkOffset)
            . self::int64(0)
            . self::abortedTransactions([])
            . sprintf('%08x', intdiv(strlen($recordSetHex), 2)) . $recordSetHex;

        return (string) hex2bin(sprintf('%08x', intdiv(strlen($body), 2)) . $body);
    }

    /**
     * Builds a Fetch response v5 frame with a single topic "topic" and a single partition
     *
     * @param string                       $recordSetHex        Hex of the record set bytes of that partition
     * @param list<array{0: int, 1: int}>|null $abortedTransactions Producer id and first offset of every aborted
     *                                                          transaction, `null` for a read_uncommitted answer
     */
    private static function responseFrameV5(
        string $recordSetHex,
        int $highWaterMarkOffset = 0,
        int $lastStableOffset = 0,
        int $logStartOffset = 0,
        ?array $abortedTransactions = null
    ): string {
        $body = '00000001' . '00000000'
            . '00000001' . '0005' . '746f706963' . '00000001'
            . '00000000' . '0000' . sprintf('%016x', $highWaterMarkOffset)
            . self::int64($lastStableOffset)
            . self::int64($logStartOffset)
            . self::abortedTransactions($abortedTransactions)
            . sprintf('%08x', intdiv(strlen($recordSetHex), 2)) . $recordSetHex;

        return (string) hex2bin(sprintf('%08x', intdiv(strlen($body), 2)) . $body);
    }

    /**
     * Builds the same frame without the `LogStartOffset` of version 5, i.e. the answer of a version 4 request
     *
     * @param list<array{0: int, 1: int}>|null $abortedTransactions
     */
    private static function responseFrameV4(
        string $recordSetHex,
        int $highWaterMarkOffset = 0,
        int $lastStableOffset = 0,
        ?array $abortedTransactions = null
    ): string {
        $body = '00000001' . '00000000'
            . '00000001' . '0005' . '746f706963' . '00000001'
            . '00000000' . '0000' . sprintf('%016x', $highWaterMarkOffset)
            . self::int64($lastStableOffset)
            . self::abortedTransactions($abortedTransactions)
            . sprintf('%08x', intdiv(strlen($recordSetHex), 2)) . $recordSetHex;

        return (string) hex2bin(sprintf('%08x', intdiv(strlen($body), 2)) . $body);
    }

    /**
     * Encodes the nullable aborted-transactions array: the element count -1 stands for `null`
     *
     * @param list<array{0: int, 1: int}>|null $abortedTransactions
     */
    private static function abortedTransactions(?array $abortedTransactions): string
    {
        if ($abortedTransactions === null) {
            return 'ffffffff';
        }

        $hex = sprintf('%08x', count($abortedTransactions));
        foreach ($abortedTransactions as [$producerId, $firstOffset]) {
            $hex .= self::int64($producerId) . self::int64($firstOffset);
        }

        return $hex;
    }

    /**
     * Encodes a signed int64 as the eight bytes of the wire format
     */
    private static function int64(int $value): string
    {
        return bin2hex(pack('J', $value));
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
