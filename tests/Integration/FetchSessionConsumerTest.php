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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\FetchRequestData;
use Protocol\Kafka\Consumer\Internals\FetchSessionHandler;
use Protocol\Kafka\Consumer\Internals\FetchSessionHandlerBuilder;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\ConsumerGroupMemberProcess;
use Protocol\Kafka\Tests\Fixture\SessionAwareConsumer;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;
use RuntimeException;

/**
 * The **consumer side** of the incremental fetch sessions of KIP-227 against a real Kafka 1.1.1 broker.
 *
 * {@see FetchSessionApiTest} drove the session protocol by hand, one frame at a time; this class drives it the way
 * an application does - {@see KafkaConsumer::poll()} - and asserts the things that only a broker can answer: that
 * a session is opened per broker and kept over the whole poll loop, that the partitions of a rebalance really are
 * forgotten, that the error codes 70 and 71 never reach the caller of poll(), what a *full* session cache does to
 * a client, and that the isolation level of a `read_committed` consumer holds for every request of its session.
 *
 * The session state that these tests look at is the {@see FetchSessionHandler} of the client, which
 * {@see SessionAwareConsumer} exposes; everything else is the consumer of the package.
 *
 * @see docs/protocol/1.1.md, section "Fetch sessions (v7, KIP-227)"
 */
#[CoversClass(FetchSessionHandler::class)]
#[CoversClass(FetchSessionHandlerBuilder::class)]
#[CoversClass(FetchRequestData::class)]
#[CoversClass(FetchMetadata::class)]
#[CoversClass(Client::class)]
#[CoversClass(KafkaConsumer::class)]
final class FetchSessionConsumerTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t8-session';

    /**
     * Client id of the second member of the group, which runs in a process of its own
     */
    private const string MEMBER_CLIENT_ID = 'kafka-client-t8-session-member';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * How long a poll loop keeps asking for the expected records, in seconds
     */
    private const float POLL_TIMEOUT = 30.0;

    /**
     * How long to wait for a rebalance of the group to settle, in seconds
     */
    private const float REBALANCE_TIMEOUT = 30.0;

    /**
     * How long to wait for a fresh topic to be servable, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * Session timeout of the group members here; `group.min.session.timeout.ms` of the container is 1000
     */
    private const int SESSION_TIMEOUT_MS = 6000;

    /**
     * Rebalance timeout of the group members, the `rebalance_timeout` of their JoinGroup
     */
    private const int MAX_POLL_INTERVAL_MS = 10000;

    /**
     * Slots of the fetch session cache of the broker, `max.incremental.fetch.session.cache.slots`
     */
    private const int SESSION_CACHE_SLOTS = 1000;

    /**
     * Error codes of a partition that exists but is not being served by this broker yet
     *
     * @var list<int>
     */
    private const array NOT_SERVABLE_YET = [
        KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
        KafkaException::LEADER_NOT_AVAILABLE,
        KafkaException::NOT_LEADER_FOR_PARTITION,
    ];

    /**
     * Topic of the current test, three partitions, created and given a leader by {@see self::setUp()}
     */
    private string $topic;

    /**
     * Correlation id of the next request this test sends by hand
     */
    private int $correlationId = 1;

    /**
     * Connection the requests that this test sends by hand travel on
     *
     * @var resource|null
     */
    private $rawSocket;

    /**
     * Consumers of the current test, which release their assignment when it ends
     *
     * @var list<KafkaConsumer>
     */
    private array $consumers = [];

    /**
     * Members of a group that run in a child process, stopped when the test ends
     *
     * @var list<ConsumerGroupMemberProcess>
     */
    private array $members = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t8-session');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), self::TOPIC_TIMEOUT, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    protected function tearDown(): void
    {
        foreach ($this->members as $member) {
            $member->stop();
        }
        $this->members = [];
        foreach ($this->consumers as $consumer) {
            $consumer->unsubscribe();
        }
        $this->consumers = [];
        if (is_resource($this->rawSocket)) {
            fclose($this->rawSocket);
        }
        $this->rawSocket = null;

        parent::tearDown();
    }

    public function testAConsumerHoldsOneFetchSessionPerBrokerAndKeepsPollingWithIt(): void
    {
        $this->produce(0, ['a-one']);
        $this->produce(1, ['b-one']);
        $this->produce(2, ['c-one']);

        $consumer = $this->consumer();
        $consumer->assign([$this->topic => [0, 1, 2]]);
        $this->pollUntil($consumer, 3);

        $sessions = $consumer->fetchSessions();

        self::assertCount(
            count(self::clusterBrokers()),
            $sessions,
            'a fetch session belongs to one broker, and this consumer read from every one of them'
        );
        foreach ($sessions as $nodeId => $handler) {
            self::assertSame($nodeId, $handler->node);
            self::assertNotSame(
                FetchMetadata::INVALID_SESSION_ID,
                $handler->getSessionId(),
                'the first poll opened a session with a full fetch'
            );
            self::assertGreaterThanOrEqual(
                1,
                $handler->getNextMetadata()->epoch,
                'and every request that follows it is an incremental fetch of that session'
            );
        }

        $node    = array_key_first($sessions);
        $session = $sessions[$node]->getSessionId();
        $epoch   = $sessions[$node]->getNextMetadata()->epoch;

        // Every following poll is an incremental fetch of that very session
        $consumer->poll(500);
        $consumer->poll(500);

        self::assertSame($session, $consumer->fetchSession($node)?->getSessionId());
        self::assertSame($epoch + 2, $consumer->fetchSession($node)?->getNextMetadata()->epoch);
        self::assertSame(
            [$this->topic => [0 => 1, 1 => 1, 2 => 1]],
            $consumer->fetchSession($node)?->getSessionPartitions(),
            'the session holds every assigned partition at the position the consumer reached'
        );
    }

    public function testThePartitionsARebalanceTakesAwayAreForgottenByTheSession(): void
    {
        $this->produce(0, ['a-one']);
        $this->produce(1, ['b-one']);
        $this->produce(2, ['c-one']);

        $groupId  = self::uniqueGroupName();
        $consumer = $this->consumer([ConsumerConfig::GROUP_ID => $groupId]);
        $consumer->subscribe([$this->topic]);
        $this->pollUntil($consumer, 3);

        $node = array_key_first($consumer->fetchSessions());

        self::assertSame(
            [0, 1, 2],
            array_keys($consumer->fetchSession($node)?->getSessionPartitions()[$this->topic] ?? []),
            'the only member of the group reads every partition, so its session holds all three'
        );
        $session = $consumer->fetchSession($node)?->getSessionId();

        // A second member joins the group and the coordinator hands it some of the partitions
        $member = $this->startMember($groupId);
        $this->awaitMemberStart($member);

        $deadline = microtime(true) + self::REBALANCE_TIMEOUT;
        do {
            $consumer->poll(250);
            $member->readEvents();
            $ownPartitions = array_map(intval(...), array_values($consumer->assignment()[$this->topic] ?? []));
            $isSplit       = $ownPartitions !== [] && count($ownPartitions) < 3
                && $member->getPartitionsOf($this->topic) !== [];
        } while (!$isSplit && microtime(true) < $deadline);

        self::assertSame('', $member->getErrorOutput(), 'the member in the child process failed');
        self::assertTrue($isSplit, 'the group is expected to split the partitions between its two members');

        // The session of the consumer holds exactly what is left of its assignment: the partitions that moved to
        // the other member travelled in the forgotten_topics_data of the first request after the rebalance
        $handler = $consumer->fetchSession($node);

        self::assertSame($session, $handler?->getSessionId(), 'a rebalance does not cost the session itself');
        self::assertSame(
            $ownPartitions,
            array_keys($handler?->getSessionPartitions()[$this->topic] ?? []),
            'the partitions of the other member are gone from the session'
        );
    }

    public function testAPausedPartitionLeavesTheSessionAndComesBackWhenItIsResumed(): void
    {
        $this->produce(0, ['a-one']);
        $this->produce(1, ['b-one']);

        $consumer = $this->consumer();
        $consumer->assign([$this->topic => [0, 1]]);
        $this->pollUntil($consumer, 2);

        $node = array_key_first($consumer->fetchSessions());

        $consumer->pause([$this->topic => [1]]);
        $consumer->poll(500);

        self::assertSame(
            [0],
            array_keys($consumer->fetchSession($node)?->getSessionPartitions()[$this->topic] ?? []),
            'a paused partition is not fetched, so the session forgets it'
        );

        $this->produce(1, ['b-two']);
        $consumer->resume([$this->topic => [1]]);
        $received = $this->pollUntil($consumer, 1);

        self::assertSame(['b-two'], self::valuesOf($received, 1), 'and it is added again when it is resumed');
        self::assertSame(
            [0, 1],
            array_keys($consumer->fetchSession($node)?->getSessionPartitions()[$this->topic] ?? []),
        );
    }

    public function testAFetchSessionOutlivesTheConnectionItWasOpenedOn(): void
    {
        $this->produce(0, ['a-one']);

        $consumer = $this->consumer();
        $consumer->assign([$this->topic => [0]]);
        $this->pollUntil($consumer, 1);

        $node    = array_key_first($consumer->fetchSessions());
        $session = $consumer->fetchSession($node)?->getSessionId();
        $epoch   = $consumer->fetchSession($node)?->getNextMetadata()->epoch;

        // The broker keeps a session in a cache of its own, not on the connection it was created on
        Node::closeConnections();
        $this->produce(0, ['a-two']);
        $received = $this->pollUntil($consumer, 1);

        self::assertSame(['a-two'], self::valuesOf($received, 0));
        self::assertSame($session, $consumer->fetchSession($node)?->getSessionId());
        self::assertSame($epoch + 1, $consumer->fetchSession($node)?->getNextMetadata()->epoch);
    }

    public function testAnEpochThatSomebodyElseUsedIsAnsweredWithSeventyOneAndThePollDoesNotNoticeIt(): void
    {
        $this->produce(0, ['a-one']);

        $consumer = $this->consumer();
        $consumer->assign([$this->topic => [0]]);
        $this->pollUntil($consumer, 1);

        $node    = array_key_first($consumer->fetchSessions());
        $handler = $consumer->fetchSession($node);
        $session = (int) $handler?->getSessionId();
        $epoch   = (int) $handler?->getNextMetadata()->epoch;

        // Somebody else sends the epoch the consumer is about to send - which is what a request whose answer was
        // lost and then arrived after all looks like to the broker
        $stolen = $this->rawFetch(new FetchMetadata($session, $epoch));

        self::assertSame(KafkaException::NO_ERROR, $stolen->errorCode, 'the broker serves it and moves the epoch on');

        $this->produce(0, ['a-two']);
        $received = $this->pollUntil($consumer, 1);

        self::assertSame(['a-two'], self::valuesOf($received, 0), 'poll() delivers the records all the same');
        self::assertNotSame(
            $session,
            $consumer->fetchSession($node)?->getSessionId(),
            'the 71 was answered with a full fetch that closed the old session and opened a new one'
        );
        self::assertSame(1, $consumer->fetchSession($node)?->getNextMetadata()->epoch);
    }

    public function testASessionThatIsGoneIsAnsweredWithSeventyAndThePollDoesNotNoticeItEither(): void
    {
        $this->produce(0, ['a-one']);

        $consumer = $this->consumer();
        $consumer->assign([$this->topic => [0]]);
        $this->pollUntil($consumer, 1);

        $node    = array_key_first($consumer->fetchSessions());
        $session = (int) $consumer->fetchSession($node)?->getSessionId();

        // The session is closed behind the back of the consumer, which is what an eviction looks like to it
        $closing = $this->rawFetch(new FetchMetadata($session, FetchMetadata::FINAL_EPOCH));

        self::assertSame(KafkaException::NO_ERROR, $closing->errorCode);
        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $closing->sessionId);

        $this->produce(0, ['a-two']);
        $received = $this->pollUntil($consumer, 1);

        self::assertSame(['a-two'], self::valuesOf($received, 0), 'poll() delivers the records all the same');
        self::assertNotSame(
            $session,
            $consumer->fetchSession($node)?->getSessionId(),
            'the 70 was answered with a full fetch that opened a new session'
        );
    }

    public function testAFullSessionCacheCostsTheNewcomerItsSessionAndLeavesTheOthersAlone(): void
    {
        // The cache of the broker holds `max.incremental.fetch.session.cache.slots` sessions, 1000 by default, and
        // a session younger than `fetch.session.eviction.ms` (two minutes) can not be evicted at all: a client that
        // asks for a session while the cache is full is answered with the session id 0, i.e. it keeps sending full
        // fetches - the incumbent sessions are untouched. Everything this test creates is closed again at its end,
        // otherwise the cache would stay full for the tests that follow.
        $this->produce(0, ['a-one']);

        $consumer = $this->consumer();
        $consumer->assign([$this->topic => [0]]);
        $this->pollUntil($consumer, 1);

        $node       = array_key_first($consumer->fetchSessions());
        $incumbent  = (int) $consumer->fetchSession($node)?->getSessionId();
        $sessionIds = [];

        try {
            $refused = 0;
            for ($created = 0; $created < self::SESSION_CACHE_SLOTS + 1; $created++) {
                $answer = $this->rawFetch(FetchMetadata::initial(), [0 => 0], [], 0);
                if ($answer->sessionId === FetchMetadata::INVALID_SESSION_ID) {
                    $refused = $created;
                    break;
                }
                $sessionIds[] = $answer->sessionId;
            }

            self::assertGreaterThan(0, $refused, 'the cache of the broker is expected to fill up and refuse a session');
            self::assertLessThanOrEqual(self::SESSION_CACHE_SLOTS, $refused);

            // The session of the consumer is not the one that pays for it
            $this->produce(0, ['a-two']);
            $received = $this->pollUntil($consumer, 1);

            self::assertSame(['a-two'], self::valuesOf($received, 0));
            self::assertSame(
                $incumbent,
                $consumer->fetchSession($node)?->getSessionId(),
                'a fresh session is not evictable, so a full cache does not cost the incumbent anything'
            );

            // A consumer that starts while the cache is full is answered with the session id 0 and keeps sending
            // full fetches, which is exactly what a broker below Kafka 1.1 answers as well
            $newcomer = $this->consumer();
            $newcomer->assign([$this->topic => [0]]);
            $records = $this->pollUntil($newcomer, 2);

            self::assertSame(['a-one', 'a-two'], self::valuesOf($records, 0), 'and it reads the log all the same');
            self::assertSame(
                FetchMetadata::INVALID_SESSION_ID,
                $newcomer->fetchSession($node)?->getSessionId(),
                'the broker had no slot left for it'
            );
            self::assertSame(
                FetchMetadata::INITIAL_EPOCH,
                $newcomer->fetchSession($node)?->getNextMetadata()->epoch,
                'so every one of its requests is a full fetch'
            );
        } finally {
            foreach ($sessionIds as $sessionId) {
                $this->rawFetch(new FetchMetadata($sessionId, FetchMetadata::FINAL_EPOCH), [], [], 0);
            }
        }

        // ... and the cache serves a session again once they are closed
        self::assertNotSame(
            FetchMetadata::INVALID_SESSION_ID,
            $this->rawFetch(FetchMetadata::initial(), [0 => 0], [], 0)->sessionId
        );
    }

    public function testTheSessionOfAReadCommittedConsumerStopsAtTheLastStableOffset(): void
    {
        $transactionalId = self::uniqueTopicName('t8-session-txn');
        $producer        = new KafkaProducer([
            ProducerConfig::TRANSACTIONAL_ID          => $transactionalId,
            ProducerConfig::CLIENT_ID                 => self::CLIENT_ID,
            ProducerConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ProducerConfig::REQUEST_TIMEOUT_MS        => 40000,
            ProducerConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ]);
        $producer->initTransactions();

        $consumer = $this->consumer([
            ConsumerConfig::ISOLATION_LEVEL => ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED,
        ]);
        $consumer->assign([$this->topic => [0]]);

        // The session is opened before there is anything to read
        $consumer->poll(500);
        $node    = array_key_first($consumer->fetchSessions());
        $session = (int) $consumer->fetchSession($node)?->getSessionId();

        self::assertNotSame(FetchMetadata::INVALID_SESSION_ID, $session);

        $producer->beginTransaction();
        $producer->send($this->topic, Record::fromValue('still open'), 0);
        $producer->flush();

        // Every incremental request of that session states the isolation level again, so the answer still stops at
        // the last stable offset: the record of the open transaction is not shown
        $consumer->poll(500);
        $consumer->poll(500);

        self::assertSame([], $consumer->poll(500), 'nothing of an open transaction reaches a read_committed poll');
        self::assertSame($session, $consumer->fetchSession($node)?->getSessionId(), 'and the session is the same');

        $producer->commitTransaction();
        $received = $this->pollUntil($consumer, 1);

        self::assertSame(['still open'], self::valuesOf($received, 0), 'the commit makes the record visible');
        self::assertSame(
            $session,
            $consumer->fetchSession($node)?->getSessionId(),
            'the whole exchange happened in one incremental fetch session'
        );
    }

    /**
     * Polls until the expected number of records arrived and returns them, indexed by partition
     *
     * @return array<int, list<Record>>
     */
    private function pollUntil(KafkaConsumer $consumer, int $expectedRecords): array
    {
        $received = [];
        $total    = 0;
        $deadline = microtime(true) + self::POLL_TIMEOUT;

        do {
            foreach ($consumer->poll(500) as $partitions) {
                foreach ($partitions as $partition => $records) {
                    foreach ($records as $record) {
                        $received[$partition][] = $record;
                        $total++;
                    }
                }
            }
        } while ($total < $expectedRecords && microtime(true) < $deadline);

        self::assertGreaterThanOrEqual(
            $expectedRecords,
            $total,
            sprintf('only %d of the %d expected records arrived within the poll timeout', $total, $expectedRecords)
        );

        return $received;
    }

    /**
     * Builds a consumer for the broker under test and registers it for the clean-up
     *
     * @param array<string, mixed> $configuration Options that override the defaults of this test class
     */
    private function consumer(array $configuration = []): SessionAwareConsumer
    {
        $consumer = new SessionAwareConsumer($configuration + [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::RETRIES                   => 2,
            ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
            ConsumerConfig::GROUP_ID                => self::uniqueGroupName(),
            ConsumerConfig::SESSION_TIMEOUT_MS      => self::SESSION_TIMEOUT_MS,
            ConsumerConfig::MAX_POLL_INTERVAL_MS    => self::MAX_POLL_INTERVAL_MS,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS   => 500,
            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
        ]);

        $this->consumers[] = $consumer;

        return $consumer;
    }

    /**
     * Starts a second member of the given group in a process of its own
     */
    private function startMember(string $groupId): ConsumerGroupMemberProcess
    {
        $member = new ConsumerGroupMemberProcess([
            'bootstrapServer'     => 'tcp://' . self::firstBootstrapServer(),
            'clientId'            => self::MEMBER_CLIENT_ID,
            'topic'               => $this->topic,
            'groupId'             => $groupId,
            'strategy'            => 'range',
            'sessionTimeoutMs'    => self::SESSION_TIMEOUT_MS,
            'maxPollIntervalMs'   => self::MAX_POLL_INTERVAL_MS,
            'heartbeatIntervalMs' => 500,
            'requestTimeoutMs'    => 30000,
            'durationSeconds'     => 2 * self::REBALANCE_TIMEOUT,
        ]);

        $this->members[] = $member;

        return $member;
    }

    /**
     * Waits until the member in the child process reports that it has joined the group
     */
    private function awaitMemberStart(ConsumerGroupMemberProcess $member): void
    {
        $deadline = microtime(true) + self::REBALANCE_TIMEOUT;
        do {
            $member->readEvents();
            if ($member->hasStarted()) {
                return;
            }
            usleep(100000);
        } while (microtime(true) < $deadline);

        self::fail('The member in the child process did not start: ' . $member->getErrorOutput());
    }

    /**
     * Sends one Fetch v7 request by hand and returns the whole answer
     *
     * The frame is written in one go on a connection of its own, so that the requests of the tests that need a
     * thousand of them do not spend a round trip each on the partial writes of the schema layer.
     *
     * @param array<int, int>          $partitionOffsets Fetch offset of every partition to send, may be empty
     * @param array<string, list<int>> $forgotten        Partitions the session should forget
     */
    private function rawFetch(
        FetchMetadata $metadata,
        array $partitionOffsets = [],
        array $forgotten = [],
        int $maxWaitMs = 250
    ): FetchResponse {
        $correlationId = $this->correlationId++;
        $request       = new FetchRequest(
            $partitionOffsets === [] ? [] : [$this->topic => $partitionOffsets],
            $maxWaitMs,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            $correlationId,
            FetchRequest::DEFAULT_MAX_BYTES,
            FetchRequest::READ_UNCOMMITTED,
            $metadata,
            $forgotten
        );

        $socket = $this->rawSocket();
        if (@fwrite($socket, (string) $request) === false) {
            throw new RuntimeException('Can not write the fetch request to the broker');
        }
        $sizeBytes = self::readBytes($socket, 4);
        $size      = (int) unpack('N', $sizeBytes)[1];
        $response  = FetchResponse::unpack(new StringStream($sizeBytes . self::readBytes($socket, $size)));

        self::assertSame($correlationId, $response->getCorrelationId());

        return $response;
    }

    /**
     * Returns the raw connection of this test, opening it on the first use
     *
     * @return resource
     */
    private function rawSocket()
    {
        if (is_resource($this->rawSocket)) {
            return $this->rawSocket;
        }

        $socket = @stream_socket_client('tcp://' . self::firstBootstrapServer(), $number, $error, 5.0);
        if ($socket === false) {
            throw new RuntimeException("Can not connect to the broker: {$error} ({$number})");
        }
        stream_set_timeout($socket, 30);
        $this->rawSocket = $socket;

        return $socket;
    }

    /**
     * Reads exactly the given number of bytes from a socket
     *
     * @param resource $socket Connection to read from
     */
    private static function readBytes($socket, int $length): string
    {
        $buffer = '';
        while (strlen($buffer) < $length) {
            $chunk = fread($socket, $length - strlen($buffer));
            if ($chunk === false || ($chunk === '' && feof($socket))) {
                throw new RuntimeException('The broker closed the connection while an answer was being read');
            }
            $buffer .= $chunk;
        }

        return $buffer;
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
            $records[] = new Record($value, null, 0, null, (int) (microtime(true) * 1000));
        }
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;

        do {
            $stream = $this->connect();
            new ProduceRequest(
                [$this->topic => [$partition => RecordBatch::fromRecords($records)]],
                1,
                self::PRODUCE_TIMEOUT_MS,
                self::CLIENT_ID,
                $this->correlationId++
            )->writeTo($stream);

            $errorCode = ProduceResponse::unpack($stream)->topics[$this->topic]->partitions[$partition]->errorCode;
            if ($errorCode === KafkaException::NO_ERROR) {
                return;
            }
            if (!in_array($errorCode, self::NOT_SERVABLE_YET, true)) {
                throw KafkaException::fromCode($errorCode, ['topic' => $this->topic, 'partitionId' => $partition]);
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The partition {$partition} of {$this->topic} did not become servable");
    }

    /**
     * Returns the values of the records that one partition of a poll loop carried
     *
     * @param array<int, list<Record>> $received Records of a poll loop
     *
     * @return list<string|null>
     */
    private static function valuesOf(array $received, int $partition): array
    {
        return array_map(
            static fn(Record $record): ?string => $record->value,
            $received[$partition] ?? []
        );
    }

    /**
     * Builds a consumer group name that no other test of this branch uses
     */
    private static function uniqueGroupName(): string
    {
        return 't8-session-group-' . bin2hex(random_bytes(6));
    }
}
