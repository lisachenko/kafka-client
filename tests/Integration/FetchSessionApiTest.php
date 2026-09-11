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
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchRequestForgottenTopic;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Drives the **incremental fetch sessions** of Fetch v7 (KIP-227) against a real Kafka 1.1.1 broker.
 *
 * A fetch session is broker state that no unit test can see: the broker remembers the partitions of a session and
 * the fetch parameters of each of them, answers only what changed since the previous answer, and refuses a request
 * whose session id it does not know (70) or whose epoch is not the one it expects (71). Everything this class
 * asserts was measured on the container first and is written down in the protocol document; the client itself
 * still sends session-less full fetches ({@see \Protocol\Kafka\Client::fetchPartitions()}), so this is the only
 * place where the session half of the frame meets a broker.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v11)" and "Fetch sessions (v7, KIP-227)"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchMetadata::class)]
#[CoversClass(FetchRequestForgottenTopic::class)]
final class FetchSessionApiTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t3-fetch-session';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * How long the broker may hold a fetch request that has nothing to answer, in milliseconds
     */
    private const int FETCH_MAX_WAIT_MS = 500;

    /**
     * Topic of the current test, three partitions, created and given a leader by {@see self::setUp()}
     */
    private string $topic;

    /**
     * Connection the session of the current test lives on
     */
    private Stream $stream;

    /**
     * Correlation id of the next request
     */
    private int $correlationId = 1;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t3-fetch-session');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
        $this->stream = $this->connect();
    }

    public function testASessionLessRequestIsAnsweredWithTheSessionIdZero(): void
    {
        $this->produce(0, ['a-one']);

        $answer = $this->fetch([0 => 0], FetchMetadata::legacy());

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $answer->sessionId);
        self::assertSame([0], array_keys($answer->topics[$this->topic]->partitions));
    }

    public function testTheEpochZeroOpensASessionAndTheBrokerAnswersItsId(): void
    {
        $this->produce(0, ['a-one']);
        $this->produce(1, ['b-one']);

        $created = $this->fetch([0 => 0, 1 => 0], FetchMetadata::initial());

        self::assertSame(KafkaException::NO_ERROR, $created->errorCode);
        self::assertNotSame(
            FetchMetadata::INVALID_SESSION_ID,
            $created->sessionId,
            'a full fetch with the epoch 0 asks the broker to open a session'
        );
        self::assertSame(
            [0, 1],
            array_keys($created->topics[$this->topic]->partitions),
            'a full fetch is answered with every partition it asked for'
        );

        // The session id is a random non-zero int32, and every request of that session is answered with it again
        $incremental = $this->fetch([], FetchMetadata::newIncremental($created->sessionId));

        self::assertSame($created->sessionId, $incremental->sessionId);
        self::assertSame(KafkaException::NO_ERROR, $incremental->errorCode);
    }

    public function testAnIncrementalFetchIsAnsweredWithTheChangedPartitionsAlone(): void
    {
        $this->produce(0, ['a-one']);
        $this->produce(1, ['b-one']);

        $session = $this->fetch([0 => 0, 1 => 0], FetchMetadata::initial())->sessionId;

        // The session remembers the fetch offset of every partition, so the client has to move them on; the answer
        // of a fetch that is already at the end of both logs carries no topic at all
        $atTheEnd = $this->fetch([0 => 1, 1 => 1], FetchMetadata::newIncremental($session));

        self::assertSame(KafkaException::NO_ERROR, $atTheEnd->errorCode);
        self::assertSame($session, $atTheEnd->sessionId);
        self::assertSame([], $atTheEnd->topics, 'nothing changed, so nothing is answered');

        // One more record in the partition 1 alone
        $this->produce(1, ['b-two']);
        $changed = $this->fetch([], new FetchMetadata($session, 2));

        self::assertSame(KafkaException::NO_ERROR, $changed->errorCode);
        self::assertSame($session, $changed->sessionId);
        self::assertSame(
            [1],
            array_keys($changed->topics[$this->topic]->partitions),
            'only the partition whose log grew is answered'
        );
        self::assertSame(['b-two'], self::valuesOf($changed, 1));
    }

    public function testAnIncrementalFetchRepeatsAPartitionWhoseFetchOffsetDidNotMove(): void
    {
        // "Only what changed" means "only what changed since the answer you were given": the session keeps the
        // fetch offset of every partition, and as long as the client does not move it the very same records are
        // answered again and again
        $this->produce(0, ['a-one']);

        $session = $this->fetch([0 => 0], FetchMetadata::initial())->sessionId;
        $again   = $this->fetch([], FetchMetadata::newIncremental($session));

        self::assertSame([0], array_keys($again->topics[$this->topic]->partitions));
        self::assertSame(['a-one'], self::valuesOf($again, 0));
    }

    public function testAForgottenPartitionDisappearsFromTheAnswerForGood(): void
    {
        $this->produce(0, ['a-one']);
        $this->produce(1, ['b-one']);

        $session = $this->fetch([0 => 0, 1 => 0], FetchMetadata::initial())->sessionId;
        $this->fetch([0 => 1, 1 => 1], FetchMetadata::newIncremental($session));

        // Both partitions get a record, and the partition 1 is dropped from the session in the same request
        $this->produce(0, ['a-two']);
        $this->produce(1, ['b-two']);
        $forgetting = $this->fetch([], new FetchMetadata($session, 2), [$this->topic => [1]]);

        self::assertSame(KafkaException::NO_ERROR, $forgetting->errorCode);
        self::assertSame($session, $forgetting->sessionId);
        self::assertSame(
            [0],
            array_keys($forgetting->topics[$this->topic]->partitions),
            'the forgotten partition is left out although it has a new record'
        );

        // ... and it stays out of every following answer
        $this->produce(0, ['a-three']);
        $this->produce(1, ['b-three']);
        $later = $this->fetch([], new FetchMetadata($session, 3));

        self::assertSame([0], array_keys($later->topics[$this->topic]->partitions));
    }

    public function testForgettingEveryPartitionClosesTheSession(): void
    {
        $this->produce(0, ['a-one']);

        $session  = $this->fetch([0 => 0], FetchMetadata::initial())->sessionId;
        $emptied  = $this->fetch([], FetchMetadata::newIncremental($session), [$this->topic => [0]]);

        self::assertSame(KafkaException::NO_ERROR, $emptied->errorCode);
        self::assertSame(
            FetchMetadata::INVALID_SESSION_ID,
            $emptied->sessionId,
            'a session without partitions is removed and the answer is session-less'
        );

        $gone = $this->fetch([], new FetchMetadata($session, 2));

        self::assertSame(KafkaException::FETCH_SESSION_ID_NOT_FOUND, $gone->errorCode);
    }

    public function testAnUnknownSessionIdIsTheErrorSeventyWithTheSessionIdZero(): void
    {
        $this->produce(0, ['a-one']);

        $answer = $this->fetch([], new FetchMetadata(2147483646, 1));

        self::assertSame(KafkaException::FETCH_SESSION_ID_NOT_FOUND, $answer->errorCode);
        self::assertSame(70, KafkaException::FETCH_SESSION_ID_NOT_FOUND);
        self::assertSame(
            FetchMetadata::INVALID_SESSION_ID,
            $answer->sessionId,
            'the answer of a session error carries the session id 0, not the one that was asked for'
        );
        self::assertSame([], $answer->topics, 'a session error is answered with an empty topics array');
    }

    public function testAWrongEpochIsTheErrorSeventyOneAndTheSessionSurvivesIt(): void
    {
        $this->produce(0, ['a-one']);

        $session = $this->fetch([0 => 0], FetchMetadata::initial())->sessionId;
        $refused = $this->fetch([], new FetchMetadata($session, 99));

        self::assertSame(KafkaException::INVALID_FETCH_SESSION_EPOCH, $refused->errorCode);
        self::assertSame(71, KafkaException::INVALID_FETCH_SESSION_EPOCH);
        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $refused->sessionId);
        self::assertSame([], $refused->topics);

        // The session itself is untouched: the request with the epoch the broker expects is served normally
        $accepted = $this->fetch([], FetchMetadata::newIncremental($session));

        self::assertSame(KafkaException::NO_ERROR, $accepted->errorCode);
        self::assertSame($session, $accepted->sessionId);
    }

    public function testTheFinalEpochClosesTheSessionAndAnswersSessionLess(): void
    {
        $this->produce(0, ['a-one']);

        $session = $this->fetch([0 => 0], FetchMetadata::initial())->sessionId;
        $closing = $this->fetch([0 => 0], new FetchMetadata($session, FetchMetadata::FINAL_EPOCH));

        self::assertSame(KafkaException::NO_ERROR, $closing->errorCode);
        self::assertSame(
            FetchMetadata::INVALID_SESSION_ID,
            $closing->sessionId,
            'the epoch -1 closes the session and the answer is session-less'
        );
        self::assertSame(['a-one'], self::valuesOf($closing, 0), 'the partitions of the request are still served');

        $gone = $this->fetch([], new FetchMetadata($session, 1));

        self::assertSame(KafkaException::FETCH_SESSION_ID_NOT_FOUND, $gone->errorCode);
    }

    public function testAFullFetchThatNamesASessionReplacesItWithANewOne(): void
    {
        $this->produce(0, ['a-one']);

        $first  = $this->fetch([0 => 0], FetchMetadata::initial())->sessionId;
        $second = $this->fetch([0 => 0], new FetchMetadata($first, FetchMetadata::INITIAL_EPOCH));

        self::assertSame(KafkaException::NO_ERROR, $second->errorCode);
        self::assertNotSame(FetchMetadata::INVALID_SESSION_ID, $second->sessionId);
        self::assertNotSame($first, $second->sessionId, 'the broker hands out a new id, it does not reuse the old');

        $gone = $this->fetch([], new FetchMetadata($first, 1));

        self::assertSame(
            KafkaException::FETCH_SESSION_ID_NOT_FOUND,
            $gone->errorCode,
            'the session the full fetch named was removed'
        );

        // FetchMetadata::nextCloseExisting() is exactly that request, and it is the way back after a 70 or a 71
        self::assertSame(
            FetchMetadata::INITIAL_EPOCH,
            FetchMetadata::newIncremental($first)->nextCloseExisting()->epoch
        );
        self::assertSame($first, FetchMetadata::newIncremental($first)->nextCloseExisting()->sessionId);
    }

    /**
     * Sends one Fetch v7 request on the connection of this test and returns the whole answer
     *
     * @param array<int, int>          $partitionOffsets Fetch offset of every partition to send, may be empty
     * @param array<string, list<int>> $forgotten        Partitions the session should forget
     */
    private function fetch(
        array $partitionOffsets,
        FetchMetadata $metadata,
        array $forgotten = []
    ): FetchResponse {
        $correlationId = $this->correlationId++;
        new FetchRequest(
            $partitionOffsets === [] ? [] : [$this->topic => $partitionOffsets],
            self::FETCH_MAX_WAIT_MS,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            $correlationId,
            FetchRequest::DEFAULT_MAX_BYTES,
            FetchRequest::READ_UNCOMMITTED,
            $metadata,
            $forgotten
        )->writeTo($this->stream);

        $response = FetchResponse::unpack($this->stream);
        self::assertSame($correlationId, $response->getCorrelationId());

        return $response;
    }

    /**
     * Appends the given values to one partition of the topic under test with a Produce v5 request
     *
     * @param list<string> $values Values of the records to append
     */
    private function produce(int $partition, array $values): void
    {
        $records = [];
        foreach ($values as $value) {
            $records[] = new Record($value, null, 0, null, self::currentTimestampMs());
        }

        $stream = $this->connect();
        new ProduceRequest(
            [$this->topic => [$partition => RecordBatch::fromRecords($records)]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            $this->correlationId++
        )->writeTo($stream);

        $errorCode = ProduceResponse::unpack($stream)->topics[$this->topic]->partitions[$partition]->errorCode;
        if ($errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($errorCode, ['topic' => $this->topic, 'partitionId' => $partition]);
        }
    }

    /**
     * Returns the values of the records that one partition of an answer carried
     *
     * @return list<string|null>
     */
    private function valuesOf(FetchResponse $response, int $partition): array
    {
        return array_map(
            static fn(Record $record): ?string => $record->value,
            $response->topics[$this->topic]->partitions[$partition]->getRecords()->getRecords()
        );
    }

    /**
     * The current time in milliseconds, the `CreateTime` a producer stamps a record with
     */
    private static function currentTimestampMs(): int
    {
        return (int) (microtime(true) * 1000);
    }
}
