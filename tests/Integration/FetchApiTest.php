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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV1;
use Protocol\Kafka\Protocol\Request\FetchRequestV2;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV1;
use Protocol\Kafka\Protocol\Request\FetchResponseV2;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the versions 2 and 3 of the Fetch API against a real Kafka 0.10.2.2 broker.
 *
 * The two versions are what a client of this line really sends, and neither of them can be checked without a
 * broker: version 2 changes nothing on the wire and only tells the broker to stop converting its answer down to
 * message format v0, and the request-level `MaxBytes` of version 3 is a rule about how the broker *fills* the
 * answer - in the order of the partitions of the request, and always with at least one complete message in the
 * first non-empty one.
 *
 * @see docs/protocol/0.11.0.md, section "Fetch API (key 1, v0 to v3)"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchRequestV2::class)]
#[CoversClass(FetchRequestV1::class)]
#[CoversClass(FetchResponse::class)]
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
     * A fixed CreateTime for the produced records, 2017-03-12T13:20:00Z
     */
    private const int CREATE_TIME = 1489324800000;

    /**
     * Topic of the current test, created and given a leader by {@see self::setUp()}
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t4-fetch');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    public function testAVersionTwoAnswerCarriesTheMessageFormatOfTheLogAndAVersionOneAnswerDoesNot(): void
    {
        $this->produce(0, [
            new Record('with a timestamp', 'key', 0, null, self::CREATE_TIME),
            new Record('and another one', null, 0, null, self::CREATE_TIME + 1),
        ]);

        $stream    = $this->connect();
        $version2  = $this->fetch($stream, FetchRequestV2::class, FetchResponseV2::class, 0, 61);
        $records   = $version2->getMessageSet()->getRecords();

        self::assertSame(Message::MAGIC_V1, $version2->getMessageSet()->getMagic());
        self::assertSame(
            [self::CREATE_TIME, self::CREATE_TIME + 1],
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

        self::assertSame(Message::MAGIC_V0, $version1->getMessageSet()->getMagic());
        self::assertSame(
            [null, null],
            array_map(static fn(Record $record): ?int => $record->timestamp, $version1->getMessageSet()->getRecords())
        );
        self::assertSame(
            ['with a timestamp', 'and another one'],
            array_map(
                static fn(Record $record): ?string => $record->value,
                $version1->getMessageSet()->getRecords()
            ),
            'the records themselves survive the conversion'
        );
    }

    public function testTheAnswerOfAVersionThreeRequestIsTheAnswerOfAVersionTwoOne(): void
    {
        $this->produce(0, [new Record('same frame', null, 0, null, self::CREATE_TIME)]);

        $stream   = $this->connect();
        $version2 = $this->fetch($stream, FetchRequestV2::class, FetchResponseV2::class, 0, 63);
        $version3 = $this->fetch($stream, FetchRequest::class, FetchResponse::class, 0, 64);

        self::assertSame($version2->getMessageSet()->toBuffer(), $version3->getMessageSet()->toBuffer());
        self::assertSame($version2->highWaterMarkOffset, $version3->highWaterMarkOffset);
    }

    public function testTheRequestLevelMaxBytesIsSpentOnThePartitionsInTheOrderOfTheRequest(): void
    {
        $this->produce(0, [new Record('partition zero', null, 0, null, self::CREATE_TIME)]);
        $this->produce(1, [new Record('partition one', null, 0, null, self::CREATE_TIME)]);

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
        $this->produce(0, [new Record(str_repeat('x', 4096), null, 0, null, self::CREATE_TIME)]);

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

    /**
     * Produces the given records into one partition of the topic under test
     *
     * @param list<Record> $records Records to append
     */
    private function produce(int $partition, array $records): void
    {
        $stream = $this->connect();
        new ProduceRequest(
            [$this->topic => [$partition => MessageSet::fromRecords($records)]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            1
        )->writeTo($stream);

        $errorCode = ProduceResponse::unpack($stream)->topics[$this->topic]->partitions[$partition]->errorCode;
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
            $partition->getMessageSet()->getRecords()
        );
    }
}
