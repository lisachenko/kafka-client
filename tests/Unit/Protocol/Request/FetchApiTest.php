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
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\FetchRequestForgottenTopic;
use Protocol\Kafka\Protocol\Data\FetchRequestTopic;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartitionV0;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicV0;
use Protocol\Kafka\Protocol\Data\FetchResponseAbortedTransaction;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Data\FetchResponsePartitionV0;
use Protocol\Kafka\Protocol\Data\FetchResponsePartitionV4;
use Protocol\Kafka\Protocol\Data\FetchResponseTopic;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV0;
use Protocol\Kafka\Protocol\Data\FetchResponseTopicV4;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV0;
use Protocol\Kafka\Protocol\Request\FetchRequestV1;
use Protocol\Kafka\Protocol\Request\FetchRequestV2;
use Protocol\Kafka\Protocol\Request\FetchRequestV3;
use Protocol\Kafka\Protocol\Request\FetchRequestV4;
use Protocol\Kafka\Protocol\Request\FetchRequestV5;
use Protocol\Kafka\Protocol\Request\FetchRequestV6;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV0;
use Protocol\Kafka\Protocol\Request\FetchResponseV1;
use Protocol\Kafka\Protocol\Request\FetchResponseV2;
use Protocol\Kafka\Protocol\Request\FetchResponseV3;
use Protocol\Kafka\Protocol\Request\FetchResponseV4;
use Protocol\Kafka\Protocol\Request\FetchResponseV5;
use Protocol\Kafka\Protocol\Request\FetchResponseV6;

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
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v7)", "Fetch sessions (v7, KIP-227)" and
 *      "MessageSet and Message"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchRequestV6::class)]
#[CoversClass(FetchRequestV5::class)]
#[CoversClass(FetchRequestV4::class)]
#[CoversClass(FetchRequestV3::class)]
#[CoversClass(FetchRequestV2::class)]
#[CoversClass(FetchRequestV1::class)]
#[CoversClass(FetchRequestV0::class)]
#[CoversClass(FetchResponse::class)]
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
     * The same request as a version 7 one: the `SessionId` and the `Epoch` of KIP-227 between the
     * `IsolationLevel` and the topics, and the `forgotten_topics_data` array behind them.
     *
     *   Size           => 00 00 00 6a (106 bytes), ApiVersion => 00 07
     *   SessionId      => 00 00 00 00 (no session), Epoch => ff ff ff ff (-1, FINAL_EPOCH)
     *   [ForgottenTopic] => 00 00 00 00 (nothing to forget)
     */
    private const string FETCH_REQUEST_HEX = '0000006a'
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
        $request = new FetchRequest(['topic' => [0 => 0, 1 => 42]], 100, 1, 1024, -1, 'test', 1, 1048576);

        self::assertSame(self::FETCH_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(106, $request->getMessageSize());
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
        $request = new FetchRequest(
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

    public function testTheIsolationLevelOfVersionFourIsWrittenBehindTheRequestLevelMaxBytes(): void
    {
        $request = new FetchRequest(
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
            substr_replace(self::FETCH_REQUEST_HEX, '01', 2 * 34, 2),
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
        $request = new FetchRequest(['topic' => [0 => 0]], 100, 1, 1024, -1, 'test', 1);

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
        $request = new FetchRequest(['topic' => [1 => 42, 0 => 0]], 100, 1, 1024, -1, 'test', 1, 1048576);

        self::assertStringEndsWith(
            '00000002'
            . '00000001' . '000000000000002a' . 'ffffffffffffffff' . '00000400'
            . '00000000' . '0000000000000000' . 'ffffffffffffffff' . '00000400'
            . '00000000',
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
        // the forgotten topics of v7 behind the whole topics array
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'replicaId', 'maxWaitTime', 'minBytes', 'maxBytes', 'isolationLevel', 'sessionId', 'epoch', 'topicPartitions', 'forgottenTopics'],
            array_keys($scheme)
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
            ['topic' => BinarySchema::TYPE_STRING, 'partitions' => [BinarySchema::TYPE_INT32]],
            FetchRequestForgottenTopic::getScheme()
        );
        self::assertSame(['topic' => FetchRequestTopic::class], $scheme['topicPartitions']);
        self::assertSame(
            ['topic' => FetchRequestTopicV0::class],
            FetchRequestV4::getScheme()['topicPartitions'],
            'below version 5 the partition entries carry no LogStartOffset'
        );
        self::assertSame(
            ['partition' => BinarySchema::TYPE_INT32, 'fetchOffset' => BinarySchema::TYPE_INT64,
                'logStartOffset' => BinarySchema::TYPE_INT64, 'maxBytes' => BinarySchema::TYPE_INT32],
            FetchRequestTopicPartition::getScheme()
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

        $response = FetchResponse::unpack(new StringStream($frame));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(0, $response->errorCode);
        self::assertSame(707406378, $response->sessionId);
        self::assertSame(2, $response->topics['topic']->partitions[0]->highWaterMarkOffset);
        self::assertSame($frame, (string) $response, 'the response has to survive a round trip');
    }

    public function testASessionLessVersion7AnswerReportsTheSessionIdZero(): void
    {
        $response = FetchResponse::unpack(new StringStream(self::responseFrameV7(self::MESSAGE_SET_HEX, 2)));

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

        $notFound = FetchResponse::unpack(new StringStream((string) hex2bin($unknownSession)));
        $invalid  = FetchResponse::unpack(new StringStream((string) hex2bin($wrongEpoch)));

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
            ['messageSize', 'correlationId', 'throttleTimeMs', 'errorCode', 'sessionId', 'topics'],
            array_keys(FetchResponse::getScheme())
        );
        self::assertSame(
            ['messageSize', 'correlationId', 'throttleTimeMs', 'topics'],
            array_keys(FetchResponseV6::getScheme())
        );

        self::assertSame(
            ['partition', 'errorCode', 'highWaterMarkOffset', 'lastStableOffset', 'logStartOffset',
                'abortedTransactions', 'messageSet'],
            array_keys(FetchResponsePartition::getScheme())
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
        self::assertSame(['topic' => FetchResponseTopic::class], FetchResponse::getScheme()['topics']);
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
