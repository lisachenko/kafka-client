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

namespace Protocol\Kafka\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV1;
use Protocol\Kafka\Protocol\Request\FetchRequestV2;
use Protocol\Kafka\Protocol\Request\FetchRequestV3;
use Protocol\Kafka\Protocol\Request\FetchRequestV4;
use Protocol\Kafka\Protocol\Request\FetchRequestV5;
use Protocol\Kafka\Protocol\Request\FetchRequestV6;
use Protocol\Kafka\Protocol\Request\FetchRequestV7;
use Protocol\Kafka\Protocol\Request\FetchRequestV8;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV1;
use Protocol\Kafka\Protocol\Request\FetchResponseV2;
use Protocol\Kafka\Protocol\Request\FetchResponseV3;
use Protocol\Kafka\Protocol\Request\FetchResponseV4;
use Protocol\Kafka\Protocol\Request\FetchResponseV5;
use Protocol\Kafka\Protocol\Request\FetchResponseV6;
use Protocol\Kafka\Protocol\Request\FetchResponseV7;
use Protocol\Kafka\Protocol\Request\FetchResponseV8;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV2;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV2;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the Fetch API against a real Kafka 1.1.1 broker.
 *
 * Most of what a version of this api states can not be checked without a broker: version 2 changes nothing on the
 * wire and only tells the broker to stop converting its answer down to message format v0, the request-level
 * `MaxBytes` of version 3 is a rule about how the broker *fills* the answer, the versions 4 and 5 are the ones
 * that answer with the log as it lies, version 6 is the version 5 frame with another error code, and version 7
 * carries the incremental fetch sessions of KIP-227, which have a test class of their own
 * ({@see FetchSessionApiTest}) - this one only checks that a version 7 request **without** a session is served
 * like a version 6 one, which is what {@see \Protocol\Kafka\Client::fetchPartitions()} sends.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v10)" and "Fetch sessions (v7, KIP-227)"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchRequestV6::class)]
#[CoversClass(FetchRequestV5::class)]
#[CoversClass(FetchRequestV2::class)]
#[CoversClass(FetchRequestV1::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchResponseV6::class)]
#[CoversClass(FetchResponseV5::class)]
#[CoversClass(FetchResponseV2::class)]
#[CoversClass(FetchResponseV1::class)]
#[CoversClass(FetchResponsePartition::class)]
final class FetchApiTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t4-fetch';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * How long the broker may hold a fetch request that has nothing to answer, in milliseconds
     */
    private const int FETCH_MAX_WAIT_MS = 500;

    /**
     * CreateTime of the records this test produces, taken from the clock in {@see self::setUp()}
     *
     * It must never be a fixed date of the past: the retention of the broker deletes a log segment by the LARGEST
     * timestamp it holds, so a record stamped with 2017 is swept away while the suite is still running.
     */
    private int $createTime;

    /**
     * Topic of the current test, created and given a leader by {@see self::setUp()}
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic      = self::uniqueTopicName('t4-fetch');
        $this->createTime = self::currentTimestampMs();
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    public function testAVersionTwoAnswerCarriesTheMessageFormatOfTheLogAndAVersionOneAnswerDoesNot(): void
    {
        $this->produce(0, [
            new Record('with a timestamp', 'key', 0, null, $this->createTime),
            new Record('and another one', null, 0, null, $this->createTime + 1),
        ]);

        $stream   = $this->connect();
        $version2 = $this->fetch($stream, FetchRequestV2::class, FetchResponseV2::class, 0, 61);
        $records  = $version2->getRecords()->getRecords();

        self::assertSame(Message::MAGIC_V1, $version2->getRecords()->getMagic());
        self::assertSame(
            [$this->createTime, $this->createTime + 1],
            array_map(static fn(Record $record): ?int => $record->timestamp, $records),
            'from version 2 on the broker answers with the message format the log holds'
        );
        self::assertSame(
            [TimestampType::CREATE_TIME, TimestampType::CREATE_TIME],
            array_map(static fn(Record $record): int => $record->timestampType, $records)
        );

        // The very same partition, asked for with a version 1 request: the broker converts it down to message
        // format v0, which has no timestamps at all
        $version1 = $this->fetch($stream, FetchRequestV1::class, FetchResponseV1::class, 0, 62);

        self::assertSame(Message::MAGIC_V0, $version1->getRecords()->getMagic());
        self::assertSame(
            [null, null],
            array_map(static fn(Record $record): ?int => $record->timestamp, $version1->getRecords()->getRecords())
        );
        self::assertSame(
            ['with a timestamp', 'and another one'],
            array_map(
                static fn(Record $record): ?string => $record->value,
                $version1->getRecords()->getRecords()
            ),
            'the records themselves survive the conversion'
        );
    }

    public function testTheAnswerOfAVersionThreeRequestIsTheAnswerOfAVersionTwoOne(): void
    {
        $this->produce(0, [new Record('same frame', null, 0, null, $this->createTime)]);

        $stream   = $this->connect();
        $version2 = $this->fetch($stream, FetchRequestV2::class, FetchResponseV2::class, 0, 63);
        $version3 = $this->fetch($stream, FetchRequestV3::class, FetchResponseV3::class, 0, 64);

        self::assertSame($version2->getRecords()->toBuffer(), $version3->getRecords()->toBuffer());
        self::assertSame($version2->highWaterMarkOffset, $version3->highWaterMarkOffset);
    }

    public function testTheVersionsFourAndFiveAnswerWithTheRecordBatchTheLogHolds(): void
    {
        $timestamp = self::currentTimestampMs();
        $this->produceRecordBatch(0, [
            new Record('as it lies', 'key', 0, null, $timestamp)->withHeaders(new Header('trace-id', 'abc')),
        ]);
        $this->produceRecordBatch(1, [new Record('without headers', null, 0, null, $timestamp)]);

        $stream   = $this->connect();
        $version4 = $this->fetchWithIsolationLevel($stream, 4, FetchRequest::READ_UNCOMMITTED, 70);
        $version5 = $this->fetchWithIsolationLevel($stream, 5, FetchRequest::READ_UNCOMMITTED, 71);

        self::assertSame(RecordBatch::MAGIC, $version4->getRecords()->getMagic());
        self::assertSame(RecordBatch::MAGIC, $version5->getRecords()->getMagic());
        self::assertSame(
            $version4->getRecords()->toBuffer(),
            $version5->getRecords()->toBuffer(),
            'the record set of a version 4 and of a version 5 answer are the same bytes'
        );

        $record = $version5->getRecords()->getRecords()[0];
        self::assertSame('as it lies', $record->value);
        self::assertSame($timestamp, $record->timestamp);
        self::assertSame(['trace-id'], array_map(
            static fn(Header $header): string => $header->key,
            $record->headers
        ));

        // The very same log without the headers, asked for with a version 3 request: converted down to the
        // message format v1, one message per record, with the timestamp of the record and no headers at all
        $version3 = $this->fetch($stream, FetchRequestV3::class, FetchResponseV3::class, 1, 72);

        self::assertSame(0, $version3->errorCode);
        self::assertSame(Message::MAGIC_V1, $version3->getRecords()->getMagic());
        self::assertSame([], $version3->getRecords()->getRecords()[0]->headers);
        self::assertSame($timestamp, $version3->getRecords()->getRecords()[0]->timestamp);
    }

    /**
     * A fetch below v4 of a partition whose records carry headers is served, and the headers are dropped
     *
     * **This is a behaviour change of Kafka 1.0.** A 0.11.0.3 broker could not build such an answer at all:
     * `MemoryRecordsBuilder.appendWithOffset` @ 0.11.0.3 threw "Magic v1 does not support record headers" out of
     * the down-conversion, and the partition came back with the error code **-1** (UnknownServerError) and an
     * empty record set. KAFKA-5760 replaced that with a down-conversion that simply **leaves the headers out**:
     * `AbstractRecords.convertRecordBatch()` @ 1.1.1 builds a message of the format the request can read, copies
     * key, value and timestamp, and warns `Down-converting records with headers` in the broker log instead of
     * failing.
     *
     * A client of a 1.x broker therefore has to know that a record it reads with a Fetch below v4 may have carried
     * headers it will never see, where a 0.11 broker refused the fetch outright. The batch below holds three
     * records and only the middle one has a header, so the test also pins that **no record is skipped** and that
     * the offsets of the answer are the offsets of the log.
     */
    public function testAPartitionWhoseRecordsCarryHeadersIsDownConvertedWithoutThemBelowVersionFour(): void
    {
        $timestamp = self::currentTimestampMs();
        $this->produceRecordBatch(0, [
            new Record('plain', 'k0', 0, null, $timestamp),
            new Record('with a header', 'k1', 0, null, $timestamp + 1)->withHeaders(new Header('trace-id', 'abc')),
            new Record('also plain', 'k2', 0, null, $timestamp + 2),
        ]);

        // `AbstractRecords.convertRecordBatch` @ 1.1.1: "Ignore headers when down-converting to V0 and V1 since
        // they are not supported". A 1.x broker converts such a batch like any other and simply leaves the headers
        // out, where a 0.11.0.3 broker answered the whole partition with the error code -1 (UnknownServerError)
        // and an empty record set, because the very same call threw "Magic v1 does not support record headers".
        $stream   = $this->connect();
        $version3 = $this->fetch($stream, FetchRequestV3::class, FetchResponseV3::class, 0, 79);
        $version1 = $this->fetch($stream, FetchRequestV1::class, FetchResponseV1::class, 0, 80);

        self::assertSame(0, $version3->errorCode, 'a 1.x broker no longer refuses the conversion');
        self::assertSame(Message::MAGIC_V1, $version3->getRecords()->getMagic());
        self::assertSame(
            ['plain', 'with a header', 'also plain'],
            self::valuesOf($version3),
            'not a single record is skipped'
        );
        self::assertSame(
            [[], [], []],
            array_map(static fn(Record $record): array => $record->headers, $version3->getRecords()->getRecords()),
            'the headers are dropped, silently'
        );
        self::assertSame(
            [$timestamp, $timestamp + 1, $timestamp + 2],
            array_map(static fn(Record $record): ?int => $record->timestamp, $version3->getRecords()->getRecords()),
            'the message format v1 still has a place for the CreateTime of every record'
        );

        self::assertSame(0, $version1->errorCode);
        self::assertSame(Message::MAGIC_V0, $version1->getRecords()->getMagic());
        self::assertSame(['plain', 'with a header', 'also plain'], self::valuesOf($version1));
        self::assertSame(
            [null, null, null],
            array_map(static fn(Record $record): ?int => $record->timestamp, $version1->getRecords()->getRecords()),
            'the message format v0 has no timestamps either'
        );

        // The very same partition carries its headers for a version 4 request
        $version4 = $this->fetchWithIsolationLevel($stream, 4, FetchRequest::READ_UNCOMMITTED, 81);

        self::assertSame(0, $version4->errorCode);
        self::assertSame(['plain', 'with a header', 'also plain'], self::valuesOf($version4));
        self::assertSame('trace-id', $version4->getRecords()->getRecords()[1]->headers[0]->key);
        self::assertSame('abc', $version4->getRecords()->getRecords()[1]->headers[0]->value);
    }

    public function testTheAnswerOfAVersionSixRequestIsTheVersionFiveFrame(): void
    {
        // FETCH_REQUEST_V6 = FETCH_REQUEST_V5 and FETCH_RESPONSE_V6 = FETCH_RESPONSE_V5 @ 1.1.1: version 6 (Kafka
        // 1.0) states that the client understands the error code 56 KAFKA_STORAGE_ERROR and changes no byte
        $this->produce(0, [new Record('version six', null, 0, null, self::currentTimestampMs())]);

        $stream      = $this->connect();
        $versionFive = $this->fetch($stream, FetchRequestV5::class, FetchResponseV5::class, 0, 90);
        $versionSix  = $this->fetch($stream, FetchRequestV6::class, FetchResponseV6::class, 0, 91);

        self::assertSame(0, $versionSix->errorCode);
        self::assertSame(['version six'], self::valuesOf($versionSix));
        self::assertSame($versionFive->highWaterMarkOffset, $versionSix->highWaterMarkOffset);
        self::assertSame($versionFive->lastStableOffset, $versionSix->lastStableOffset);
        self::assertSame($versionFive->logStartOffset, $versionSix->logStartOffset);
        self::assertSame(
            bin2hex((string) $versionFive->messageSet),
            bin2hex((string) $versionSix->messageSet)
        );
    }

    public function testTheAnswerOfAVersionEightRequestIsTheVersionSevenFrame(): void
    {
        // `FetchRequest.json` @ 2.8.2 says "Version 8 is the same as version 7" and `FetchResponse.json` only
        // notes that a throttled answer is now sent before the delay: the two frames are the same bytes, and the
        // version this client sends (8, KIP-219) is the promise that it waits the throttle time out itself
        $this->produce(0, [new Record('version eight', null, 0, null, self::currentTimestampMs())]);

        $stream       = $this->connect();
        $versionSeven = $this->fetch($stream, FetchRequestV7::class, FetchResponseV7::class, 0, 94);
        $versionEight = $this->fetch($stream, FetchRequestV8::class, FetchResponseV8::class, 0, 95);

        self::assertSame(0, $versionEight->errorCode);
        self::assertSame(['version eight'], self::valuesOf($versionEight));
        self::assertSame($versionSeven->highWaterMarkOffset, $versionEight->highWaterMarkOffset);
        self::assertSame($versionSeven->lastStableOffset, $versionEight->lastStableOffset);
        self::assertSame($versionSeven->logStartOffset, $versionEight->logStartOffset);
        self::assertSame(
            bin2hex((string) $versionSeven->messageSet),
            bin2hex((string) $versionEight->messageSet)
        );
        self::assertSame(8, FetchRequestV8::VERSION, 'the version Kafka 2.0 added');
        self::assertSame(10, FetchRequest::VERSION, 'and the client sends the version Kafka 2.1 added');
    }

    public function testAVersionSevenRequestWithoutASessionIsServedLikeAVersionSixOne(): void
    {
        // `session_id = 0` with `epoch = -1` is the LEGACY metadata of the Java client, which is what
        // Client::fetchPartitions() sends: the whole requested set comes back and the broker keeps no state
        $this->produce(0, [
            new Record('one', null, 0, null, self::currentTimestampMs()),
            new Record('two', null, 0, null, self::currentTimestampMs()),
        ]);

        $stream = $this->connect();
        new FetchRequest(
            [$this->topic => [0 => 0]],
            self::FETCH_MAX_WAIT_MS,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            92
        )->writeTo($stream);

        $response = FetchResponse::unpack($stream);

        self::assertSame(92, $response->getCorrelationId());
        self::assertSame(0, $response->errorCode, 'a session-less fetch has no session error');
        self::assertSame(
            FetchMetadata::INVALID_SESSION_ID,
            $response->sessionId,
            'the broker answers the session id 0 when it was not asked to open a session'
        );
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(['one', 'two'], self::valuesOf($response->topics[$this->topic]->partitions[0]));
    }

    public function testTheLastStableOffsetAndTheAbortedTransactionsAreTheAnswerOfAReadCommittedFetchAlone(): void
    {
        $this->produce(0, [
            new Record('committed', null, 0, null, self::currentTimestampMs()),
            new Record('and another one', null, 0, null, self::currentTimestampMs()),
        ]);

        $stream         = $this->connect();
        $uncommittedV4  = $this->fetchWithIsolationLevel($stream, 4, FetchRequest::READ_UNCOMMITTED, 73);
        $committedV4    = $this->fetchWithIsolationLevel($stream, 4, FetchRequest::READ_COMMITTED, 74);
        $uncommittedV5  = $this->fetchWithIsolationLevel($stream, 5, FetchRequest::READ_UNCOMMITTED, 75);
        $committedV5    = $this->fetchWithIsolationLevel($stream, 5, FetchRequest::READ_COMMITTED, 76);
        $uncommittedV8  = $this->fetchWithIsolationLevel($stream, 8, FetchRequest::READ_UNCOMMITTED, 77);
        $committedV8    = $this->fetchWithIsolationLevel($stream, 8, FetchRequest::READ_COMMITTED, 78);

        // **What changed with Kafka 2.x**: a 0.11.0.3 and a 1.1.1 broker answered `last_stable_offset = -1` to a
        // read_uncommitted fetch - "you did not ask, so I did not compute it" - while a 2.8.2 broker fills the
        // field in for both isolation levels (`Partition.readRecords` always puts the LSO into its LogReadInfo).
        // The aborted-transactions array did NOT change: it is still null unless read_committed asked for it.
        $uncommitted = ['v4' => $uncommittedV4, 'v5' => $uncommittedV5, 'v8' => $uncommittedV8];
        foreach ($uncommitted as $version => $partition) {
            self::assertSame(
                $partition->highWaterMarkOffset,
                $partition->lastStableOffset,
                "a 2.8.2 broker answers the real last stable offset to a read_uncommitted {$version} fetch too"
            );
            self::assertNotSame(
                FetchResponsePartition::INVALID_LAST_STABLE_OFFSET,
                $partition->lastStableOffset,
                "the -1 of the lines below this one is gone ({$version})"
            );
            self::assertNull(
                $partition->abortedTransactions,
                "a read_uncommitted {$version} fetch is answered with a null aborted-transactions array"
            );
        }

        foreach (['v4' => $committedV4, 'v5' => $committedV5, 'v8' => $committedV8] as $version => $partition) {
            self::assertSame(
                $partition->highWaterMarkOffset,
                $partition->lastStableOffset,
                "the LSO of a partition without transactions is its high water mark ({$version})"
            );
            self::assertSame(
                [],
                $partition->abortedTransactions,
                "an empty array is not the null of a read_uncommitted fetch ({$version})"
            );
        }

        self::assertSame(2, $committedV5->highWaterMarkOffset);
        self::assertSame(['committed', 'and another one'], self::valuesOf($committedV5));
    }

    public function testOnlyAVersionFiveAnswerCarriesTheLogStartOffsetOfThePartition(): void
    {
        $this->produce(0, [new Record('somewhere in the log', null, 0, null, self::currentTimestampMs())]);

        $stream   = $this->connect();
        $version5 = $this->fetchWithIsolationLevel($stream, 5, FetchRequest::READ_UNCOMMITTED, 77);
        $version4 = $this->fetchWithIsolationLevel($stream, 4, FetchRequest::READ_UNCOMMITTED, 78);

        self::assertSame(0, $version5->logStartOffset, 'nothing was deleted from the front of a fresh log');
        self::assertSame(
            FetchResponsePartition::INVALID_LOG_START_OFFSET,
            $version4->logStartOffset,
            'a version 4 answer does not carry the field at all'
        );
        self::assertSame(
            self::valuesOf($version5),
            self::valuesOf($version4),
            'the record set of the two answers is the same, only the partition header differs'
        );
    }

    public function testTheRequestLevelMaxBytesIsSpentOnThePartitionsInTheOrderOfTheRequest(): void
    {
        $this->produce(0, [new Record('partition zero', null, 0, null, $this->createTime)]);
        $this->produce(1, [new Record('partition one', null, 0, null, $this->createTime)]);

        // 40 bytes are less than a single message of either partition, so the broker serves the first partition of
        // the request - which gets its message in full - and leaves nothing for the second one
        $stream    = $this->connect();
        $inOrder   = $this->fetchPartitions($stream, [0 => 0, 1 => 0], 65, 40);
        $reversed  = $this->fetchPartitions($stream, [1 => 0, 0 => 0], 66, 40);

        self::assertSame(['partition zero'], self::valuesOf($inOrder[0]));
        self::assertSame([], self::valuesOf($inOrder[1]), 'the budget was used up by the partition in front of it');
        self::assertSame(1, $inOrder[1]->highWaterMarkOffset, 'although that partition has something to read');
        self::assertSame(0, $inOrder[1]->errorCode, 'an empty partition is not an error');

        self::assertSame(['partition one'], self::valuesOf($reversed[1]), 'the order of the request decides');
        self::assertSame([], self::valuesOf($reversed[0]));
    }

    public function testTheFirstPartitionOfAnAnswerIsServedEvenWhenItsMessageIsBiggerThanEveryLimit(): void
    {
        $this->produce(0, [new Record(str_repeat('x', 4096), null, 0, null, $this->createTime)]);

        // Neither the partition limit nor the request limit fits that message; version 3 returns it anyway, so
        // that a consumer can always make progress (KIP-74)
        $stream    = $this->connect();
        $partition = $this->fetchPartitions($stream, [0 => 0], 67, 64, 64)[0];

        self::assertSame(0, $partition->errorCode);
        self::assertSame([str_repeat('x', 4096)], self::valuesOf($partition));
        self::assertGreaterThan(4096, strlen((string) $partition->messageSet), 'the answer exceeds its own MaxBytes');
        self::assertFalse(
            $partition->isSingleMessageTooLarge(0),
            'the stuck partition of the lower versions does not exist in version 3'
        );

        // A budget of zero bytes is no exception to that rule
        self::assertSame([str_repeat('x', 4096)], self::valuesOf($this->fetchPartitions($stream, [0 => 0], 68, 0)[0]));

        // The very same fetch with a version 1 request: the broker cuts the message set off at the MaxBytes of the
        // partition and the answer holds no complete message at all
        $version1 = $this->fetch($stream, FetchRequestV1::class, FetchResponseV1::class, 0, 69, 64);

        self::assertSame([], self::valuesOf($version1));
        self::assertTrue($version1->isSingleMessageTooLarge(0), 'which is what the lower versions answer instead');
    }

    public function testTheClientCarriesTheStateOfEveryPartitionOfAVersionFiveAnswer(): void
    {
        $timestamp = self::currentTimestampMs();
        $this->produceRecordBatch(0, [
            new Record('through the client', 'key', 0, null, $timestamp)
                ->withHeaders(new Header('trace-id', 'abc')),
        ]);

        $uncommitted = $this->client()->fetchPartitions([$this->topic => [0 => 0]], 1000)[$this->topic][0];
        $committed   = $this->client(['isolation.level' => 'read_committed'])
            ->fetchPartitions([$this->topic => [0 => 0]], 1000)[$this->topic][0];

        self::assertSame(0, $uncommitted->errorCode);
        self::assertSame(1, $uncommitted->highWaterMarkOffset);
        self::assertSame(0, $uncommitted->logStartOffset, 'version 5 reports it for both isolation levels');
        self::assertSame(
            1,
            $uncommitted->lastStableOffset,
            'a 2.8.2 broker answers the real LSO to a read_uncommitted fetch as well, where 1.1.1 answered -1'
        );
        self::assertNull($uncommitted->abortedTransactions);

        self::assertSame(1, $committed->lastStableOffset, 'without a transaction the LSO is the high water mark');
        self::assertSame([], $committed->abortedTransactions, 'asked for, and nothing was aborted here');
        self::assertSame(0, $committed->logStartOffset);

        $records = $committed->getRecords();
        self::assertCount(1, $records);
        self::assertSame('through the client', $records[0]->value);
        self::assertSame($timestamp, $records[0]->timestamp);
        self::assertSame(['trace-id'], array_map(
            static fn(Header $header): string => $header->key,
            $records[0]->headers
        ), 'the headers survive the whole path through the client');
        self::assertSame(1, $committed->getNextOffset());
    }

    /**
     * A low-level client of the broker under test
     *
     * @param array<string, mixed> $overrides Options on top of the defaults of this test
     */
    private function client(array $overrides = []): Client
    {
        $configuration = $overrides + [
            ClientConfig::BOOTSTRAP_SERVERS           => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                   => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS   => 30000,
            ClientConfig::REQUEST_TIMEOUT_MS          => 10000,
            ConsumerConfig::FETCH_MAX_WAIT_MS         => self::FETCH_MAX_WAIT_MS,
            ConsumerConfig::FETCH_MIN_BYTES           => 1,
            ConsumerConfig::MAX_PARTITION_FETCH_BYTES => 65536,
        ];

        return new Client(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * Produces the given records into one partition of the topic under test
     *
     * @param list<Record> $records Records to append
     */
    private function produce(int $partition, array $records): void
    {
        $stream = $this->connect();
        // A message set may only travel in a request below version 3, see docs/protocol/2.8.md
        new ProduceRequestV2(
            [$this->topic => [$partition => MessageSet::fromRecords($records)]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            1
        )->writeTo($stream);

        $errorCode = ProduceResponseV2::unpack($stream)->topics[$this->topic]->partitions[$partition]->errorCode;
        if ($errorCode !== 0) {
            throw KafkaException::fromCode($errorCode, ['topic' => $this->topic, 'partitionId' => $partition]);
        }
    }

    /**
     * Fetches one partition of the topic under test with the given pair of request and response classes
     *
     * @param class-string<FetchRequest>  $requestClass  Version of the request to send
     * @param class-string<FetchResponse> $responseClass Class that reads the answer of that version
     */
    private function fetch(
        Stream $stream,
        string $requestClass,
        string $responseClass,
        int $partition,
        int $correlationId,
        int $partitionMaxBytes = 65536
    ): FetchResponsePartition {
        new $requestClass(
            [$this->topic => [$partition => 0]],
            self::FETCH_MAX_WAIT_MS,
            1,
            $partitionMaxBytes,
            -1,
            self::CLIENT_ID,
            $correlationId
        )->writeTo($stream);

        $response = $responseClass::unpack($stream);
        self::assertSame($correlationId, $response->getCorrelationId());

        return $response->topics[$this->topic]->partitions[$partition];
    }

    /**
     * Fetches several partitions with one version 3 request and returns the answer of each of them
     *
     * @param array<int, int> $partitionOffsets Offset of every partition, in the order they are asked for
     *
     * @return array<int, FetchResponsePartition>
     */
    private function fetchPartitions(
        Stream $stream,
        array $partitionOffsets,
        int $correlationId,
        int $maxBytes,
        int $partitionMaxBytes = 65536
    ): array {
        new FetchRequest(
            [$this->topic => $partitionOffsets],
            self::FETCH_MAX_WAIT_MS,
            1,
            $partitionMaxBytes,
            -1,
            self::CLIENT_ID,
            $correlationId,
            $maxBytes
        )->writeTo($stream);

        $response = FetchResponse::unpack($stream);
        self::assertSame($correlationId, $response->getCorrelationId());

        return $response->topics[$this->topic]->partitions;
    }

    /**
     * Returns the values of the records a partition of an answer carried
     *
     * @return list<string|null>
     */
    private static function valuesOf(FetchResponsePartition $partition): array
    {
        return array_map(
            static fn(Record $record): ?string => $record->value,
            $partition->getRecords()->getRecords()
        );
    }

    /**
     * Produces the given records as a record batch of the message format v2, which needs a version 3 request
     *
     * @param list<Record> $records Records to append
     */
    private function produceRecordBatch(int $partition, array $records): void
    {
        $stream = $this->connect();
        new ProduceRequest(
            [$this->topic => [$partition => RecordBatch::fromRecords($records)]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            2
        )->writeTo($stream);

        $errorCode = ProduceResponse::unpack($stream)->topics[$this->topic]->partitions[$partition]->errorCode;
        if ($errorCode !== 0) {
            throw KafkaException::fromCode($errorCode, ['topic' => $this->topic, 'partitionId' => $partition]);
        }
    }

    /**
     * Fetches one partition with a version 4 or 5 request at the given isolation level
     */
    private function fetchWithIsolationLevel(
        Stream $stream,
        int $version,
        int $isolationLevel,
        int $correlationId,
        int $partition = 0
    ): FetchResponsePartition {
        $requestClass = match ($version) {
            8       => FetchRequest::class,
            5       => FetchRequestV5::class,
            default => FetchRequestV4::class,
        };
        $responseClass = match ($version) {
            8       => FetchResponse::class,
            5       => FetchResponseV5::class,
            default => FetchResponseV4::class,
        };

        new $requestClass(
            [$this->topic => [$partition => 0]],
            self::FETCH_MAX_WAIT_MS,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            $correlationId,
            FetchRequest::DEFAULT_MAX_BYTES,
            $isolationLevel
        )->writeTo($stream);

        $response = $responseClass::unpack($stream);
        self::assertSame($correlationId, $response->getCorrelationId());

        return $response->topics[$this->topic]->partitions[$partition];
    }

    /**
     * The current time in milliseconds; a test never stamps a record with a timestamp of the past, because the
     * retention of the broker deletes a segment by the largest timestamp it holds
     */
    private static function currentTimestampMs(): int
    {
        return (int) round(microtime(true) * 1000);
    }
}
