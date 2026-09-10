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

namespace Protocol\Kafka\Tests\Unit\Consumer;

use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\Internals\FetchRequestData;
use Protocol\Kafka\Consumer\Internals\FetchSessionHandler;
use Protocol\Kafka\Consumer\Internals\FetchSessionHandlerBuilder;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;

/**
 * The state machine of an incremental fetch session (KIP-227) as the consumer drives it.
 *
 * These are the cases of the Java `FetchSessionHandlerTest` @ 1.1.1 - a session-less broker, the incrementals, the
 * removal of a partition, the double build and the recovery from a session error - translated to the shape this
 * client has: a partition is `topic => partition => fetch offset` here, because a Fetch request of this client
 * carries one `MaxBytes` for every partition and a `LogStartOffset` that only a follower fills in, so the Java
 * `PartitionData` triple has exactly one value that a consumer moves.
 *
 * @see docs/protocol/1.1.md, section "Fetch sessions (v7, KIP-227)"
 */
#[CoversClass(FetchSessionHandler::class)]
#[CoversClass(FetchSessionHandlerBuilder::class)]
#[CoversClass(FetchRequestData::class)]
#[CoversClass(FetchMetadata::class)]
final class FetchSessionHandlerTest extends TestCase
{
    private const string TOPIC = 'foo';

    private const string OTHER_TOPIC = 'bar';

    /**
     * Session id the broker of these tests hands out
     */
    private const int SESSION_ID = 123;

    public function testTheFirstRequestOfAHandlerIsTheFullFetchThatAsksForASession(): void
    {
        $handler = new FetchSessionHandler(1);

        $data = $handler->newBuilder()
            ->add(new TopicPartition(self::TOPIC, 0), 0)
            ->add(new TopicPartition(self::TOPIC, 1), 10)
            ->build();

        self::assertTrue($data->isFull());
        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $data->metadata->sessionId);
        self::assertSame(FetchMetadata::INITIAL_EPOCH, $data->metadata->epoch);
        self::assertSame([self::TOPIC => [0 => 0, 1 => 10]], $data->toSend, 'a full fetch states every partition');
        self::assertSame([self::TOPIC => [0 => 0, 1 => 10]], $data->sessionPartitions);
        self::assertSame([], $data->toForget, 'a full fetch replaces the set, it forgets nothing');
        self::assertSame('FullFetchRequest(foo-0, foo-1)', (string) $data);
    }

    public function testABrokerThatAnswersWithTheSessionIdZeroKeepsTheClientOnFullFetches(): void
    {
        // This is what a broker below Kafka 1.1 does, and what a 1.1.1 broker does when its session cache is full
        $handler = new FetchSessionHandler(1);
        $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        self::assertTrue($handler->handleResponse(
            self::response([self::TOPIC => [0 => 0]], FetchMetadata::INVALID_SESSION_ID)
        ));

        $next = $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        self::assertTrue($next->isFull());
        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $next->metadata->sessionId);
        self::assertSame(FetchMetadata::INITIAL_EPOCH, $next->metadata->epoch);
        self::assertSame([self::TOPIC => [0 => 0]], $next->toSend);
    }

    public function testTheAnswerOfAFullFetchOpensTheSessionAndTheNextRequestIsIncremental(): void
    {
        $handler = new FetchSessionHandler(1);
        $handler->newBuilder()
            ->add(new TopicPartition(self::TOPIC, 0), 0)
            ->add(new TopicPartition(self::TOPIC, 1), 10)
            ->build();

        self::assertTrue($handler->handleResponse(self::response([self::TOPIC => [0 => 0, 1 => 0]])));
        self::assertSame(self::SESSION_ID, $handler->getSessionId());

        // One partition keeps its offset, one moves, one is new: only the last two travel in the request
        $data = $handler->newBuilder()
            ->add(new TopicPartition(self::TOPIC, 0), 0)
            ->add(new TopicPartition(self::TOPIC, 1), 20)
            ->add(new TopicPartition(self::OTHER_TOPIC, 0), 20)
            ->build();

        self::assertFalse($data->isFull());
        self::assertSame(self::SESSION_ID, $data->metadata->sessionId);
        self::assertSame(1, $data->metadata->epoch);
        self::assertSame([self::TOPIC => [1 => 20], self::OTHER_TOPIC => [0 => 20]], $data->toSend);
        self::assertSame(
            [self::TOPIC => [0 => 0, 1 => 20], self::OTHER_TOPIC => [0 => 20]],
            $data->sessionPartitions,
            'the session holds every partition, whether the request repeated it or not'
        );
        self::assertSame([], $data->toForget);
        self::assertSame(
            'IncrementalFetchRequest(toSend=(foo-1, bar-0), toForget=(), implied=(foo-0))',
            (string) $data
        );
    }

    public function testAnIncrementalAnswerOfASubsetMovesTheEpochOn(): void
    {
        $handler = self::handlerWithSession([self::TOPIC => [0 => 0, 1 => 0]]);
        $handler->newBuilder()
            ->add(new TopicPartition(self::TOPIC, 0), 5)
            ->add(new TopicPartition(self::TOPIC, 1), 0)
            ->build();

        // Only one of the two partitions had something to say, which is the point of an incremental fetch
        self::assertTrue($handler->handleResponse(self::response([self::TOPIC => [1 => 0]])));
        self::assertSame(2, $handler->getNextMetadata()->epoch);

        // ... and so is an answer with no partition at all
        $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 5)->add(
            new TopicPartition(self::TOPIC, 1),
            0
        )->build();

        self::assertTrue($handler->handleResponse(self::response([])));
        self::assertSame(3, $handler->getNextMetadata()->epoch);
    }

    public function testAPartitionThatLeavesTheSetIsForgotten(): void
    {
        $handler = self::handlerWithSession([
            self::TOPIC       => [0 => 0, 1 => 10],
            self::OTHER_TOPIC => [0 => 20],
        ]);

        $data = $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 1), 10)->build();

        self::assertFalse($data->isFull());
        self::assertSame(self::SESSION_ID, $data->metadata->sessionId);
        self::assertSame(1, $data->metadata->epoch);
        self::assertSame([], $data->toSend, 'the partition that stays did not move, so it is not repeated');
        self::assertSame(
            [self::TOPIC => [0], self::OTHER_TOPIC => [0]],
            $data->toForget,
            'both partitions that are gone travel in forgotten_topics_data'
        );
        self::assertSame([self::TOPIC => [1 => 10]], $data->sessionPartitions);
        self::assertSame(
            'IncrementalFetchRequest(toSend=(), toForget=(foo-0, bar-0), implied=(foo-1))',
            (string) $data
        );
    }

    public function testTheErrorCodeSeventyStartsANewSessionWithoutTheOldId(): void
    {
        $handler = self::handlerWithSession([self::TOPIC => [0 => 0]]);
        $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        // The broker evicted the session: there is nothing left to close, and the answer carries no partition
        self::assertFalse($handler->handleResponse(
            self::response([], FetchMetadata::INVALID_SESSION_ID, KafkaException::FETCH_SESSION_ID_NOT_FOUND)
        ));

        $data = $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        self::assertTrue($data->isFull());
        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $data->metadata->sessionId);
        self::assertSame(FetchMetadata::INITIAL_EPOCH, $data->metadata->epoch);
        self::assertSame([self::TOPIC => [0 => 0]], $data->toSend);
    }

    public function testTheErrorCodeSeventyOneClosesTheSessionThatIsStillThereAndOpensANewOne(): void
    {
        $handler = self::handlerWithSession([self::TOPIC => [0 => 0]]);
        $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        // The epoch was refused; the session itself is intact, so the recovery names it and asks for a new one
        self::assertFalse($handler->handleResponse(
            self::response([], FetchMetadata::INVALID_SESSION_ID, KafkaException::INVALID_FETCH_SESSION_EPOCH)
        ));

        $data = $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        self::assertTrue($data->isFull());
        self::assertSame(
            self::SESSION_ID,
            $data->metadata->sessionId,
            'the id of the answer is 0, the handler keeps its own'
        );
        self::assertSame(FetchMetadata::INITIAL_EPOCH, $data->metadata->epoch);
        self::assertSame([self::TOPIC => [0 => 0]], $data->toSend);
    }

    public function testAnIncrementalAnswerWithTheSessionIdZeroMeansThatTheBrokerClosedIt(): void
    {
        // Forgetting the last partition of a session removes it, and the broker says so with the session id 0
        $handler = self::handlerWithSession([self::TOPIC => [0 => 0]]);
        $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        self::assertTrue($handler->handleResponse(self::response([], FetchMetadata::INVALID_SESSION_ID)));

        $data = $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        self::assertTrue($data->isFull());
        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $data->metadata->sessionId);
    }

    public function testAnAnswerWithAPartitionOutsideTheSessionIsRefused(): void
    {
        $handler = self::handlerWithSession([self::TOPIC => [0 => 0]]);
        $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        self::assertFalse(
            $handler->handleResponse(self::response([self::TOPIC => [0 => 0, 7 => 0]])),
            'a partition that is not in the session can not be matched to a fetch offset'
        );

        $data = $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        self::assertTrue($data->isFull());
        self::assertSame(self::SESSION_ID, $data->metadata->sessionId, 'the session is closed and reopened');
    }

    public function testAFullFetchThatIsNotAnsweredWithItsWholeSetIsRefused(): void
    {
        $handler = new FetchSessionHandler(1);
        $handler->newBuilder()
            ->add(new TopicPartition(self::TOPIC, 0), 0)
            ->add(new TopicPartition(self::TOPIC, 1), 0)
            ->build();

        self::assertFalse($handler->handleResponse(self::response([self::TOPIC => [0 => 0]])));

        $data = $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        self::assertTrue($data->isFull());
        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $data->metadata->sessionId);
    }

    public function testALostRequestMakesTheNextOneCloseTheSessionAndOpenANewOne(): void
    {
        $handler = self::handlerWithSession([self::TOPIC => [0 => 0]]);
        $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        $handler->handleError(new NetworkException(['error' => 'the connection dropped']));

        $data = $handler->newBuilder()->add(new TopicPartition(self::TOPIC, 0), 0)->build();

        self::assertTrue($data->isFull());
        self::assertSame(self::SESSION_ID, $data->metadata->sessionId);
        self::assertSame(FetchMetadata::INITIAL_EPOCH, $data->metadata->epoch);
    }

    public function testABuilderIsGoodForOneRequestOnly(): void
    {
        $builder = new FetchSessionHandler(1)->newBuilder();
        $builder->add(new TopicPartition(self::TOPIC, 0), 0);
        $builder->build();

        $this->expectException(LogicException::class);
        $builder->build();
    }

    public function testAPartitionThatIsAddedTwiceKeepsItsPlaceAndTakesTheNewerOffset(): void
    {
        $data = new FetchSessionHandler(1)->newBuilder()
            ->add(new TopicPartition(self::TOPIC, 0), 0)
            ->add(new TopicPartition(self::TOPIC, 1), 0)
            ->add(new TopicPartition(self::TOPIC, 0), 5)
            ->build();

        self::assertSame([self::TOPIC => [0 => 5, 1 => 0]], $data->toSend);
    }

    public function testTheHandlerReportsTheNodeItBelongsTo(): void
    {
        $handler = new FetchSessionHandler(7);

        self::assertSame(7, $handler->node);
        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $handler->getSessionId());
        self::assertSame([], $handler->getSessionPartitions());
    }

    /**
     * Returns a handler whose session is open and holds the given partitions
     *
     * @param array<string, array<int, int>> $topicPartitions Partitions of the session, topic => partition => offset
     */
    private static function handlerWithSession(array $topicPartitions): FetchSessionHandler
    {
        $handler = new FetchSessionHandler(1);
        $builder = $handler->newBuilder();
        $answer  = [];
        foreach ($topicPartitions as $topic => $partitionOffsets) {
            foreach ($partitionOffsets as $partition => $fetchOffset) {
                $builder->add(new TopicPartition($topic, $partition), $fetchOffset);
                $answer[$topic][$partition] = $fetchOffset;
            }
        }
        $builder->build();
        $handler->handleResponse(self::response($answer));

        return $handler;
    }

    /**
     * Builds a Fetch v7 answer with the given partitions, all of them empty and without an error
     *
     * @param array<string, array<int, int>> $topicPartitions Partitions of the answer, topic => partition => offset
     */
    private static function response(
        array $topicPartitions,
        int $sessionId = self::SESSION_ID,
        int $errorCode = KafkaException::NO_ERROR
    ): FetchResponse {
        $topics = [];
        foreach ($topicPartitions as $topic => $partitions) {
            foreach (array_keys($partitions) as $partition) {
                $topics[$topic][$partition] = [KafkaException::NO_ERROR, 0, ''];
            }
        }

        return FetchResponse::unpack(
            new StringStream(ResponseFrame::fetch(1, $topics, 0, [], $errorCode, $sessionId))
        );
    }
}
