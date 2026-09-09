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
use Protocol\Kafka\Admin\DeletedRecords;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Admin\RecordsToDelete;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\DeleteRecordsRequestPartition;
use Protocol\Kafka\Protocol\Data\DeleteRecordsRequestTopic;
use Protocol\Kafka\Protocol\Data\DeleteRecordsResponsePartition;
use Protocol\Kafka\Protocol\Data\DeleteRecordsResponseTopic;
use Protocol\Kafka\Protocol\Request\DeleteRecordsRequest;
use Protocol\Kafka\Protocol\Request\DeleteRecordsResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * Exercises the DeleteRecords api (key 21) against a real Kafka 0.11.0.3 broker.
 *
 * The api arrived with Kafka 0.11 (KIP-107) and is served by the LEADER of each partition. Every assertion here is
 * about the **low watermark** of a partition, which the answer reports and which three other places of the protocol
 * have to agree with: an Offsets request with `EARLIEST`, the `log_start_offset` of a Fetch v5 answer, and the
 * error a Fetch below it gets.
 *
 * @see docs/protocol/0.11.0.md, section "DeleteRecords API (key 21, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(Client::class)]
#[CoversClass(DeleteRecordsRequest::class)]
#[CoversClass(DeleteRecordsResponse::class)]
#[CoversClass(DeleteRecordsRequestTopic::class)]
#[CoversClass(DeleteRecordsRequestPartition::class)]
#[CoversClass(DeleteRecordsResponseTopic::class)]
#[CoversClass(DeleteRecordsResponsePartition::class)]
#[CoversClass(DeletedRecords::class)]
#[CoversClass(RecordsToDelete::class)]
final class DeleteRecordsApiTest extends IntegrationTestCase
{
    /**
     * How long to wait for a fresh topic to become servable, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * Records this test class writes into every topic it creates
     */
    private const int RECORD_COUNT = 5;

    private Cluster $cluster;

    private AdminClient $admin;

    private Client $client;

    /**
     * Topics this test class created, deleted again after every test
     *
     * @var list<string>
     */
    private array $createdTopics = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
        $this->client  = new Client($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    public function testDeletingBeforeAnOffsetMovesTheLowWatermarkOfThePartition(): void
    {
        $topic = $this->topicWithRecords('before');

        $result = $this->admin->deleteRecords([$topic => [0 => 2]]);

        self::assertInstanceOf(DeletedRecords::class, $result[$topic][0]);
        self::assertSame(2, $result[$topic][0]->lowWatermark, 'the offset the request named is the new watermark');
        self::assertSame(2, $this->earliestOffset($topic), 'and the Offsets api answers exactly that');
        self::assertSame(
            self::RECORD_COUNT,
            $this->latestOffset($topic),
            'the end of the log did not move, only its start'
        );
    }

    public function testDeletingEveryRecordMovesTheWatermarkToTheHighWatermark(): void
    {
        $topic = $this->topicWithRecords('all');

        $result = $this->admin->deleteRecords([$topic => [0 => RecordsToDelete::allRecords()]]);

        self::assertSame(
            self::RECORD_COUNT,
            $result[$topic][0]->lowWatermark,
            'the offset -1 asks for everything that is fully replicated'
        );
        self::assertSame(self::RECORD_COUNT, $this->earliestOffset($topic));
    }

    public function testDeletingBelowTheCurrentWatermarkChangesNothingAndIsNoError(): void
    {
        $topic = $this->topicWithRecords('again');
        $this->admin->deleteRecords([$topic => [0 => 3]]);

        $result = $this->admin->deleteRecords([$topic => [0 => 1]]);

        self::assertSame(3, $result[$topic][0]->lowWatermark, 'the watermark only ever moves forward');
        self::assertSame(3, $this->earliestOffset($topic));
    }

    public function testAnOffsetAboveTheHighWatermarkIsAnsweredWithOffsetOutOfRange(): void
    {
        $topic = $this->topicWithRecords('too-high');

        try {
            $this->admin->deleteRecords([$topic => [0 => self::RECORD_COUNT + 1]]);
            self::fail('an offset above the high watermark has to be reported');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(OffsetOutOfRangeException::class, $exception->getExceptions()[$topic][0]);
        }

        self::assertSame(0, $this->earliestOffset($topic), 'and nothing was deleted');
    }

    public function testANegativeOffsetOtherThanMinusOneIsAlsoOffsetOutOfRange(): void
    {
        // -2 is EARLIEST in the Offsets api, but this api only knows -1: `ReplicaManager.deleteRecordsOnLocalLog`
        // resolves -1 to the high watermark and rejects everything else below zero
        $topic = $this->topicWithRecords('negative');

        try {
            $this->admin->deleteRecords([$topic => [0 => -2]]);
            self::fail('a negative offset other than -1 has to be reported');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(OffsetOutOfRangeException::class, $exception->getExceptions()[$topic][0]);
        }
    }

    public function testTheClientRefusesATopicItHasNoLeaderFor(): void
    {
        // The request is split per partition leader, so a topic the metadata of this client does not know is
        // refused before a single byte goes out - exactly like a produce or a fetch of such a topic
        $topic = self::uniqueTopicName('t5-records-unknown');

        try {
            $this->client->deleteRecords([$topic => [0 => 1]]);
            self::fail('a topic the cluster does not have must not be reported as a success');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(InvalidTopicException::class, $exception->getExceptions()[$topic][0]);
        }
    }

    public function testTheBrokerAnswersAnUnknownTopicWithThreeAndCreatesNothing(): void
    {
        $topic = self::uniqueTopicName('t5-records-raw-unknown');

        $stream = $this->connect();
        new DeleteRecordsRequest([$topic => [0 => 1]], 30000, 't5-records', 4243)->writeTo($stream);
        $response = DeleteRecordsResponse::unpack($stream);

        self::assertSame(
            KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
            $response->topics[$topic]->partitions[0]->errorCode,
            'the answer holds one entry per partition of the request, with the error code 3'
        );
        self::assertSame(
            DeleteRecordsResponsePartition::INVALID_LOW_WATERMARK,
            $response->topics[$topic]->partitions[0]->lowWatermark
        );
        self::assertNotContains(
            $topic,
            $this->admin->listTopics(),
            'unlike a Metadata request, this one does not create the topic it names'
        );
    }

    public function testDeletingFromAPartitionTheTopicDoesNotHaveIsUnknownTopicOrPartition(): void
    {
        $topic = $this->topicWithRecords('no-partition');

        try {
            $this->client->deleteRecords([$topic => [0 => 1, 7 => 1]]);
            self::fail('the partition that does not exist has to be reported');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(UnknownTopicOrPartitionException::class, $exception->getExceptions()[$topic][7]);
            self::assertSame(
                1,
                $exception->getPartialResult()[$topic][0]->lowWatermark,
                'the partition that exists was deleted from all the same'
            );
        }
    }

    public function testAFetchBelowTheNewLowWatermarkIsOffsetOutOfRange(): void
    {
        $topic = $this->topicWithRecords('fetch');
        $this->admin->deleteRecords([$topic => [0 => 2]]);

        try {
            $this->client->fetch([$topic => [0 => 0]], 1000);
            self::fail('a fetch below the low watermark has to fail');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(OffsetOutOfRangeException::class, $exception->getExceptions()[$topic][0]);
        }

        // The five records were produced as one record batch, and a broker never splits a batch: a fetch that
        // starts inside one is answered with the whole batch, the records below the fetch offset included. Which
        // of them an application sees is decided by the consumer - `KafkaConsumer::poll()` drops everything below
        // its position - so the low-level `fetch()` reports the batch as it lies.
        $records = $this->client->fetch([$topic => [0 => 2]], 1000);
        self::assertSame(
            range(0, self::RECORD_COUNT - 1),
            array_column($records[$topic][0], 'offset'),
            'the record at the watermark and everything above it survived, in the batch it was written in'
        );
    }

    public function testAFetchV5ReportsTheNewLowWatermarkAsLogStartOffset(): void
    {
        // Fetch v5 (KIP-107) added `log_start_offset` to the partition header precisely because of this api. The
        // frame is written by hand here: the classes of Fetch v4 and v5 belong to another ticket of this line.
        $topic = $this->topicWithRecords('log-start');
        $this->admin->deleteRecords([$topic => [0 => 3]]);

        $header = $this->rawFetchV5PartitionHeader($topic, 3);

        self::assertSame(KafkaException::NO_ERROR, $header['errorCode']);
        self::assertSame(3, $header['logStartOffset'], 'the low watermark of the DeleteRecords answer');
        self::assertSame(self::RECORD_COUNT, $header['highWatermark']);

        $below = $this->rawFetchV5PartitionHeader($topic, 0);
        self::assertSame(KafkaException::OFFSET_OUT_OF_RANGE, $below['errorCode']);
    }

    /**
     * Creates a topic of one partition, fills it with {@see self::RECORD_COUNT} records and returns its name
     */
    private function topicWithRecords(string $purpose): string
    {
        $topic                 = self::uniqueTopicName("t5-records-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));

        // Every record is stamped with `now`: a time-based retention deletes a segment by the largest timestamp it
        // holds, so a record from the past could take the whole segment with it while the test runs
        $now      = (int) (microtime(true) * 1000);
        $messages = [];
        for ($index = 0; $index < self::RECORD_COUNT; $index++) {
            $messages[] = new Record("t5-record-{$index}", null, 0, null, $now);
        }

        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            try {
                $this->cluster->reload();
                $this->client->produce([$topic => [0 => $messages]]);

                return $topic;
            } catch (TopicPartitionRequestException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
            }
        } while (true);
    }

    /**
     * Returns the offset of the first record that is still readable
     */
    private function earliestOffset(string $topic): int
    {
        return $this->admin->listOffsets([$topic => [0]], OffsetsRequest::EARLIEST)[$topic][0];
    }

    /**
     * Returns the offset the next produced record will get
     */
    private function latestOffset(string $topic): int
    {
        return $this->admin->listOffsets([$topic => [0]], OffsetsRequest::LATEST)[$topic][0];
    }

    /**
     * Sends a hand-written Fetch v5 frame and returns the partition header of the answer
     *
     * @return array{errorCode: int, highWatermark: int, lastStableOffset: int, logStartOffset: int}
     */
    private function rawFetchV5PartitionHeader(string $topic, int $fetchOffset): array
    {
        $body = pack('nnN', 1 /* Fetch */, 5 /* version */, 4242)
            . pack('n', strlen('t5-records')) . 't5-records'
            . pack('N', 0xFFFFFFFF)                       // replica_id = -1
            . pack('N', 1000)                             // max_wait_time
            . pack('N', 0)                                // min_bytes
            . pack('N', 1048576)                          // max_bytes
            . pack('c', 0)                                // isolation_level = READ_UNCOMMITTED
            . pack('N', 1) . pack('n', strlen($topic)) . $topic . pack('N', 1)
            . pack('N', 0) . pack('J', $fetchOffset) . pack('J', 0) . pack('N', 1048576);

        $stream = $this->connect();
        $stream->writeBuffer(pack('N', strlen($body)) . $body);

        // Size, correlation id, throttle time, one topic with one partition
        $stream->read('NmessageSize');
        $stream->read('NcorrelationId');
        $stream->read('NthrottleTimeMs');
        $stream->read('NtopicCount');
        $stream->readString();
        $stream->read('NpartitionCount');
        $stream->read('Npartition');
        $errorCode = $stream->read('nerrorCode')['errorCode'];

        return [
            'errorCode'        => $errorCode > 0x7FFF ? $errorCode - 0x10000 : $errorCode,
            'highWatermark'    => $stream->read('JhighWatermark')['highWatermark'],
            'lastStableOffset' => $stream->read('JlastStableOffset')['lastStableOffset'],
            'logStartOffset'   => $stream->read('JlogStartOffset')['logStartOffset'],
        ];
    }

    /**
     * Client configuration pointing at the broker under test
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 't5-records',
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => 1,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }
}
