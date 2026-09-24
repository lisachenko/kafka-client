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
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnknownTopicIdException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\FetchSessionHandler;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\MetadataRequestTopic;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV12;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV12;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequestV11;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\MetadataResponseV11;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV12;
use Protocol\Kafka\Protocol\Request\ProduceResponseV12;

/**
 * What **Kafka 3.1** adds to the apis the consumer sends: the request side of the topic ids of KIP-516.
 *
 * **Fetch v13** replaces the topic *name* of every topic entry - of the topics array and of the
 * `forgotten_topics_data` alike - and of every topic entry of the answer with the 16 raw bytes of the topic id,
 * so a client can only fetch a topic whose id it knows: `Cluster::topicIdOf()` is the map it learns from every
 * Metadata answer, and a fetch of an id the node does not host is answered **100** `UnknownTopicId` per
 * partition. A fetch **session** is keyed by ids from that version on, and mixing the two ways of naming a topic
 * inside one session is the **106** `FetchSessionTopicIdError` that this release adds.
 *
 * **Metadata v12** adds no field at all: it is the version at which the node finally honours the `topic_id` and
 * the nullable name that version 10 put on the wire, so a request may name a topic by its id alone
 * (`MetadataRequest::byTopicIds()`, `AdminClient::describeTopicsByIds()`).
 *
 * Every topic of this class is named `t2-31-…`, so that it can run next to the other suites on the shared node.
 *
 * @see docs/protocol/4.3.md, sections "The topic ids of the fetch path (v13, KIP-516)", "Metadata by topic id
 *      (v12, KIP-516)", "Fetch API (key 1, v0 to v18)" and "Metadata API (key 3, v0 to v13)"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchRequestV12::class)]
#[CoversClass(FetchResponseV12::class)]
#[CoversClass(MetadataRequest::class)]
#[CoversClass(MetadataResponse::class)]
#[CoversClass(MetadataRequestV11::class)]
#[CoversClass(MetadataResponseV11::class)]
#[CoversClass(MetadataRequestTopic::class)]
#[CoversClass(FetchSessionHandler::class)]
#[CoversClass(Cluster::class)]
#[CoversClass(Client::class)]
#[CoversClass(AdminClient::class)]
final class TopicIdsFetchApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t2-31';

    private const int REQUEST_TIMEOUT_MS = 30000;

    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * Id of a topic no cluster hosts: 16 bytes of 0x7f
     */
    private const string UNKNOWN_TOPIC_ID = "\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f";

    private static ?Cluster $sharedCluster = null;

    private Client $client;

    private AdminClient $admin;

    /**
     * Topic of the current test, one partition, deleted again when the test ends
     */
    private string $topic;

    /**
     * Id the node gave that topic, the 16 raw bytes of its uuid
     */
    private string $topicId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->client = new Client($this->cluster(), $this->configuration());
        $this->admin  = new AdminClient($this->cluster(), $this->configuration());
        $this->topic  = self::uniqueTopicName('t2-31-ids');

        self::assertSame([$this->topic => null], $this->admin->createTopics([new NewTopic($this->topic, 1, 1)]));
        $this->awaitTopic($this->topic);

        $topicId = $this->cluster()->topicIdOf($this->topic);
        self::assertNotNull($topicId, 'a topic of a KRaft node always has an id');
        $this->topicId = $topicId;
    }

    protected function tearDown(): void
    {
        if (isset($this->topic)) {
            try {
                $this->admin->deleteTopics([$this->topic]);
            } catch (KafkaException) {
                // A node that can not delete the topic right now must not fail the test that just passed
            }
        }

        parent::tearDown();
    }

    public function testTheClusterResolvesATopicNameToItsIdAndBack(): void
    {
        self::assertSame(Uuid::SIZE, strlen($this->topicId), 'a uuid is 16 raw bytes');
        self::assertFalse(Uuid::isZero($this->topicId));
        self::assertSame($this->topic, $this->cluster()->topicNameById($this->topicId));
        self::assertSame([$this->topic => $this->topicId], $this->cluster()->topicIdsOf([$this->topic]));
        self::assertNull(
            $this->cluster()->topicNameById(self::UNKNOWN_TOPIC_ID),
            'an id no topic of the cluster carries resolves to no name'
        );
        self::assertNull($this->cluster()->topicNameById(Uuid::ZERO), 'and the zero uuid never resolves');
    }

    public function testAVersionThirteenFetchNamesTheTopicByItsIdAndIsAnsweredTheSameWay(): void
    {
        $this->produce(['t2-31 one', 't2-31 two']);

        $request = new FetchRequest(
            [$this->topic => [0 => 0]],
            250,
            1,
            1048576,
            -1,
            self::CLIENT_ID,
            3101,
            52428800,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            [$this->topic => $this->topicId]
        );

        self::assertSame(17, $request->getApiVersion(), 'the client sends the version of Kafka 3.9 now');
        self::assertSame([$this->topic => $this->topicId], $request->getTopicIds());
        self::assertStringNotContainsString(
            $this->topic,
            (string) $request,
            'the name of the topic is nowhere in a version 13 frame'
        );

        $answer = $this->send($request, FetchResponse::class);

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertCount(1, $answer->topics, 'the entries of a version 13 answer are a list, they have no name');
        $topic = $answer->topics[0];
        self::assertSame($this->topicId, $topic->topicId, 'the answer names the very id the request carried');
        self::assertSame('', $topic->topic, 'and carries no name at all');
        self::assertSame(KafkaException::NO_ERROR, $topic->partitions[0]->errorCode);
        self::assertSame(2, $topic->partitions[0]->highWaterMarkOffset);
        self::assertNotSame('', $topic->partitions[0]->messageSet, 'the records of the log came back');
    }

    public function testAVersionThirteenFetchOfAnIdTheNodeDoesNotHostIsAnsweredPerPartition(): void
    {
        $answer = $this->send(
            new FetchRequest(
                ['t2-31-gone' => [0 => 0]],
                250,
                1,
                1048576,
                -1,
                self::CLIENT_ID,
                3102,
                52428800,
                FetchRequest::READ_UNCOMMITTED,
                null,
                [],
                FetchRequest::NO_RACK,
                null,
                ['t2-31-gone' => self::UNKNOWN_TOPIC_ID]
            ),
            FetchResponse::class
        );

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode, 'the 100 is not a top-level error');
        $partition = $answer->topics[0]->partitions[0];
        self::assertSame(self::UNKNOWN_TOPIC_ID, $answer->topics[0]->topicId, 'the id is echoed back');
        self::assertSame(KafkaException::UNKNOWN_TOPIC_ID, $partition->errorCode);
        self::assertSame(-1, $partition->highWaterMarkOffset, 'and every offset of such a partition is -1');
        self::assertSame(-1, $partition->lastStableOffset);
        self::assertSame(-1, $partition->logStartOffset);
    }

    public function testAVersionThirteenFetchOfATopicWhoseIdIsUnknownIsRefusedBeforeItIsSent(): void
    {
        // There is no way to name a topic in a version 13 frame but its id, so a client that does not know it
        // refreshes its metadata instead of falling back to the name
        $this->expectException(UnknownTopicIdException::class);

        new FetchRequest([$this->topic => [0 => 0]], 250, 1, 1048576, -1, self::CLIENT_ID, 3103);
    }

    public function testMixingIdsAndNamesInOneFetchSessionIsTheErrorCodeOfKafkaThreeOne(): void
    {
        $this->produce(['t2-31 session']);

        // A session opened by ID at version 13 …
        $stream = $this->connect();
        $opened = $this->send(
            new FetchRequest(
                [$this->topic => [0 => 0]],
                250,
                1,
                1048576,
                -1,
                self::CLIENT_ID,
                3104,
                52428800,
                FetchRequest::READ_UNCOMMITTED,
                FetchMetadata::initial(),
                [],
                FetchRequest::NO_RACK,
                null,
                [$this->topic => $this->topicId]
            ),
            FetchResponse::class,
            $stream
        );

        self::assertSame(KafkaException::NO_ERROR, $opened->errorCode);
        self::assertNotSame(FetchMetadata::INVALID_SESSION_ID, $opened->sessionId, 'the node opened a session');

        // … and continued by NAME at version 12
        $refused = $this->send(
            new FetchRequestV12(
                [$this->topic => [0 => 1]],
                250,
                1,
                1048576,
                -1,
                self::CLIENT_ID,
                3105,
                52428800,
                FetchRequest::READ_UNCOMMITTED,
                FetchMetadata::newIncremental($opened->sessionId)
            ),
            FetchResponseV12::class,
            $stream
        );

        self::assertSame(KafkaException::FETCH_SESSION_TOPIC_ID_ERROR, $refused->errorCode);
        self::assertSame(FetchMetadata::INVALID_SESSION_ID, $refused->sessionId);
        self::assertSame([], $refused->topics, 'a session error costs no partition anything');

        // The mirror image: a session opened by NAME at version 12, continued by ID at version 13
        $stream = $this->connect();
        $byName = $this->send(
            new FetchRequestV12(
                [$this->topic => [0 => 0]],
                250,
                1,
                1048576,
                -1,
                self::CLIENT_ID,
                3106,
                52428800,
                FetchRequest::READ_UNCOMMITTED,
                FetchMetadata::initial()
            ),
            FetchResponseV12::class,
            $stream
        );

        self::assertSame(KafkaException::NO_ERROR, $byName->errorCode);

        $refused = $this->send(
            new FetchRequest(
                [$this->topic => [0 => 1]],
                250,
                1,
                1048576,
                -1,
                self::CLIENT_ID,
                3107,
                52428800,
                FetchRequest::READ_UNCOMMITTED,
                FetchMetadata::newIncremental($byName->sessionId),
                [],
                FetchRequest::NO_RACK,
                null,
                [$this->topic => $this->topicId]
            ),
            FetchResponse::class,
            $stream
        );

        self::assertSame(
            KafkaException::FETCH_SESSION_TOPIC_ID_ERROR,
            $refused->errorCode,
            'the node refuses both directions of the mix'
        );
        self::assertSame([], $refused->topics);
    }

    public function testASessionThatNamesItsTopicsByIdThroughoutIsServedIncrementally(): void
    {
        $this->produce(['t2-31 incremental']);

        $stream = $this->connect();
        $opened = $this->send(
            $this->sessionFetch(0, 3108, FetchMetadata::initial()),
            FetchResponse::class,
            $stream
        );

        self::assertSame(KafkaException::NO_ERROR, $opened->errorCode);
        self::assertCount(1, $opened->topics);

        $incremental = $this->send(
            $this->sessionFetch(1, 3109, FetchMetadata::newIncremental($opened->sessionId)),
            FetchResponse::class,
            $stream
        );

        self::assertSame(KafkaException::NO_ERROR, $incremental->errorCode);
        self::assertSame($opened->sessionId, $incremental->sessionId, 'the session lives on');
    }

    public function testTheConsumerFetchPathResolvesTheTopicIdItself(): void
    {
        $this->produce(['t2-31 client one', 't2-31 client two']);

        $partitions = $this->client->fetchPartitions([$this->topic => [0 => 0]], 2000);

        self::assertArrayHasKey($this->topic, $partitions, 'the answer is mapped back to the topic NAME');
        $fetched = $partitions[$this->topic][0];
        self::assertSame(KafkaException::NO_ERROR, $fetched->errorCode);
        self::assertSame(
            ['t2-31 client one', 't2-31 client two'],
            array_map(static fn(Record $record): string => $record->value, $fetched->getRecords())
        );

        // And the same through the session path of the consumer, which keeps the id map of its own
        $withSessions = $this->client->fetchPartitionsWithSessions([$this->topic => [0 => 0]], 2000);

        self::assertArrayHasKey($this->topic, $withSessions);
        self::assertCount(2, $withSessions[$this->topic][0]->getRecords());
        $handlers = $this->client->getFetchSessionHandlers();
        self::assertNotSame([], $handlers, 'the fetch opened a session');
        foreach ($handlers as $handler) {
            self::assertSame(
                [$this->topicId => $this->topic],
                $handler->getSessionTopicNames(),
                'the session remembers the id it named the topic by'
            );
        }
    }

    public function testMetadataVersionTwelveResolvesATopicThatIsNamedByItsIdAlone(): void
    {
        $answer = $this->send(
            MetadataRequest::byTopicIds([$this->topicId], self::CLIENT_ID, 3110),
            MetadataResponse::class
        );

        self::assertArrayHasKey($this->topic, $answer->topics, 'the answer fills the name in');
        self::assertSame(KafkaException::NO_ERROR, $answer->topics[$this->topic]->topicErrorCode);
        self::assertSame($this->topicId, $answer->topics[$this->topic]->topicId);
        self::assertCount(1, $answer->topics[$this->topic]->partitions);

        // The same question asked by name is answered the very same body
        $byName = $this->send(
            new MetadataRequest([$this->topic], false, self::CLIENT_ID, 3110),
            MetadataResponse::class
        );

        self::assertSame(bin2hex((string) $answer), bin2hex((string) $byName));
    }

    public function testMetadataVersionTwelveAnswersAnUnknownTopicIdWithTheHundredAndANullName(): void
    {
        $answer = $this->send(
            MetadataRequest::byTopicIds([self::UNKNOWN_TOPIC_ID], self::CLIENT_ID, 3111),
            MetadataResponse::class
        );

        self::assertNotSame([], $answer->brokers, 'the brokers of the cluster are reported as usual');
        self::assertCount(1, $answer->topics);
        $topic = $answer->topics[0];
        self::assertSame(KafkaException::UNKNOWN_TOPIC_ID, $topic->topicErrorCode);
        self::assertNull($topic->topic, 'the nullable name of version 12 is what this entry exists for');
        self::assertSame(self::UNKNOWN_TOPIC_ID, $topic->topicId);
        self::assertSame([], $topic->partitions);
    }

    public function testAMetadataRequestThatMixesIdsAndNamesLosesTheNamedHalf(): void
    {
        // KafkaApis.handleTopicMetadataRequest @ 3.9.2: useTopicId is set by ONE non-zero id and the topics of
        // the answer are then the names those ids resolved to, so the named entries are dropped without a word
        $other = self::uniqueTopicName('t2-31-other');
        self::assertSame([$other => null], $this->admin->createTopics([new NewTopic($other, 1, 1)]));
        $this->awaitTopic($other);

        try {
            $answer = $this->send(
                new MetadataRequest(
                    [MetadataRequestTopic::byId($this->topicId), new MetadataRequestTopic($other)],
                    false,
                    self::CLIENT_ID,
                    3112
                ),
                MetadataResponse::class
            );

            self::assertSame([$this->topic], array_keys($answer->topics), 'the named topic is not in the answer');
        } finally {
            try {
                $this->admin->deleteTopics([$other]);
            } catch (KafkaException) {
                // best effort, tearDownAfterClass removes it as well
            }
        }
    }

    public function testAVersionBelowTwelveThatIsAskedByIdIsRefusedWithAWholeErrorResponse(): void
    {
        $answer = $this->send(
            new MetadataRequestV11([MetadataRequestTopic::byId($this->topicId)], false, self::CLIENT_ID, 3113),
            MetadataResponseV11::class
        );

        self::assertSame([], $answer->brokers, 'not one broker - this is an error response, not an answer');
        self::assertNull($answer->clusterId);
        self::assertSame(MetadataResponse::NO_CONTROLLER_ID, $answer->controllerId);
        self::assertCount(1, $answer->topics);
        $topic = reset($answer->topics);
        self::assertSame(KafkaException::INVALID_REQUEST, $topic->topicErrorCode);
        self::assertSame('', $topic->topic, 'the name of a version 11 answer is never null');
        self::assertSame($this->topicId, $topic->topicId);
    }

    public function testTheAdminClientDescribesTopicsByTheirIds(): void
    {
        $described = $this->admin->describeTopicsByIds([$this->topicId, self::UNKNOWN_TOPIC_ID]);

        $known   = Uuid::toString($this->topicId);
        $unknown = Uuid::toString(self::UNKNOWN_TOPIC_ID);

        self::assertArrayHasKey($known, $described);
        self::assertSame($this->topic, $described[$known]->topic);
        self::assertSame(KafkaException::NO_ERROR, $described[$known]->topicErrorCode);
        self::assertCount(1, $described[$known]->partitions);

        self::assertArrayHasKey($unknown, $described);
        self::assertNull($described[$unknown]->topic);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_ID, $described[$unknown]->topicErrorCode);
        self::assertSame([], $this->admin->describeTopicsByIds([]), 'an empty list asks nothing');
    }

    /**
     * Builds the version 13 fetch of the session tests: the topic named by its id, at the given fetch offset
     */
    private function sessionFetch(int $fetchOffset, int $correlationId, FetchMetadata $metadata): FetchRequest
    {
        return new FetchRequest(
            [$this->topic => [0 => $fetchOffset]],
            250,
            1,
            1048576,
            -1,
            self::CLIENT_ID,
            $correlationId,
            52428800,
            FetchRequest::READ_UNCOMMITTED,
            $metadata,
            [],
            FetchRequest::NO_RACK,
            null,
            [$this->topic => $this->topicId]
        );
    }

    /**
     * Appends the given values to the partition 0 of the topic of the current test
     *
     * @param list<string> $values Values of the records to append
     */
    private function produce(array $values): void
    {
        $records = [];
        foreach ($values as $index => $value) {
            $records[] = new Record($value)->withCreateTime(1700000000000 + $index * 1000);
        }

        $answer = $this->send(
            new ProduceRequestV12(
                [$this->topic => [0 => RecordBatch::fromRecords($records)]],
                1,
                self::REQUEST_TIMEOUT_MS,
                self::CLIENT_ID,
                3100
            ),
            ProduceResponseV12::class
        );

        self::assertSame(
            KafkaException::NO_ERROR,
            $answer->topics[$this->topic]->partitions[0]->errorCode,
            'the records of this test could not be produced'
        );
    }

    /**
     * Sends one request and reads the answer that belongs to it, optionally over a connection that is kept open
     *
     * @template T of \Protocol\Kafka\Protocol\Request\AbstractResponse
     *
     * @param class-string<T> $responseClass Class the answer is decoded with
     *
     * @return T
     */
    private function send(
        \Protocol\Kafka\Protocol\Request\AbstractRequest $request,
        string $responseClass,
        ?SocketStream $stream = null
    ): \Protocol\Kafka\Protocol\Request\AbstractResponse {
        $stream ??= $this->connect();
        $request->writeTo($stream);

        return $responseClass::unpack($stream);
    }

    /**
     * Waits until the fresh partition has a leader that answers, which a brand new topic needs a moment for
     */
    private function awaitTopic(string $topic): void
    {
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            try {
                $this->cluster()->reload();
                $this->admin->listOffsets([$topic => [0]], OffsetsRequest::LATEST);

                return;
            } catch (KafkaException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
            }
        } while (true);
    }

    private function cluster(): Cluster
    {
        return self::$sharedCluster ??= Cluster::bootstrap($this->configuration());
    }

    /**
     * @return array<string, mixed> Client configuration of this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,

            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,

            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
        ] + ConsumerConfig::getDefaultConfiguration();
    }
}
