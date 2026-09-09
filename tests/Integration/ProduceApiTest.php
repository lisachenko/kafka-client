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
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;
use Protocol\Kafka\Protocol\Data\ProduceRequestPartition;
use Protocol\Kafka\Protocol\Data\ProduceRequestTopic;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartitionV0;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopic;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopicV0;
use Protocol\Kafka\Protocol\Request\FetchRequestV2;
use Protocol\Kafka\Protocol\Request\FetchResponseV2;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV0;
use Protocol\Kafka\Protocol\Request\ProduceRequestV1;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV0;
use Protocol\Kafka\Protocol\Request\ProduceResponseV1;
use Protocol\Kafka\Tests\Fixture\SpecMessageSet;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the Produce API against a real Kafka 0.10.2.2 broker.
 *
 * The broker validates the CRC of every message it appends, so a green run here also proves that the message sets
 * built by {@see SpecMessageSet} follow the specification. It is also the authority on what version 2 of the api
 * really answers: the `LogAppendTime` of a partition, which is -1 for a topic that keeps the `CreateTime` of the
 * producer and the clock of the broker for a topic with `message.timestamp.type=LogAppendTime`.
 *
 * @see docs/protocol/0.11.0.md, section "Produce API (key 0, v0, v1 and v2)"
 */
#[CoversClass(ProduceRequest::class)]
#[CoversClass(ProduceRequestV1::class)]
#[CoversClass(ProduceRequestV0::class)]
#[CoversClass(ProduceResponse::class)]
#[CoversClass(ProduceResponseV1::class)]
#[CoversClass(ProduceResponseV0::class)]
#[CoversClass(ProduceRequestTopic::class)]
#[CoversClass(ProduceRequestPartition::class)]
#[CoversClass(ProduceResponseTopic::class)]
#[CoversClass(ProduceResponseTopicV0::class)]
#[CoversClass(ProduceResponsePartition::class)]
#[CoversClass(ProduceResponsePartitionV0::class)]
#[CoversClass(Client::class)]
#[CoversClass(KafkaProducer::class)]
final class ProduceApiTest extends IntegrationTestCase
{
    /**
     * Client id sent along with every request of this test class
     */
    private const string CLIENT_ID = 'kafka-client-t4-produce';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * A fixed CreateTime for the produced records, 2017-03-12T13:20:00Z
     */
    private const int CREATE_TIME = 1489324800000;

    /**
     * Topic of the current test, created and given a leader by {@see ProduceApiTest::setUp()}
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t4-produce');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    public function testProduceWithAcksOneReturnsTheBaseOffsetOfEveryBatch(): void
    {
        $stream = $this->connect();

        $first = $this->produce($stream, SpecMessageSet::of([[null, 'a'], [null, 'b']]), 1, 11);
        self::assertSame(0, $first->errorCode);
        self::assertSame(0, $first->baseOffset, 'the first batch starts at the beginning of the log');

        $second = $this->produce($stream, SpecMessageSet::of([['key', 'c'], [null, 'd'], [null, 'e']]), 1, 12);
        self::assertSame(0, $second->errorCode);
        self::assertSame(2, $second->baseOffset, 'the offsets increase by the number of appended messages');

        $third = $this->produce($stream, SpecMessageSet::of([[null, 'f']]), 1, 13);
        self::assertSame(5, $third->baseOffset);
    }

    public function testProduceWithAcksMinusOneWaitsForTheInSyncReplicas(): void
    {
        $response = $this->produce($this->connect(), SpecMessageSet::of([[null, 'committed']]), -1, 21);

        self::assertSame(0, $response->errorCode);
        self::assertSame(0, $response->baseOffset);
    }

    public function testProduceWithAcksZeroIsNotAnsweredButStillAppends(): void
    {
        $stream  = $this->connect();
        $request = new ProduceRequest(
            [$this->topic => [0 => SpecMessageSet::of([[null, 'fire'], [null, 'forget']])]],
            0,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            31
        );

        self::assertFalse($request->expectsResponse());
        $request->writeTo($stream);

        // Nothing at all comes back for that request: the next answer on this connection belongs to the next request
        new MetadataRequest([$this->topic], self::CLIENT_ID, 32)->writeTo($stream);
        self::assertSame(32, MetadataResponse::unpack($stream)->getCorrelationId());

        // ... and the two messages were nevertheless appended, so the next batch starts at offset 2
        $acknowledged = $this->produce($stream, SpecMessageSet::of([[null, 'acked']]), 1, 33);

        self::assertSame(0, $acknowledged->errorCode);
        self::assertSame(2, $acknowledged->baseOffset);
    }

    public function testProducerSendsRecordsThroughTheCluster(): void
    {
        $producer = new KafkaProducer($this->producerConfiguration(1));

        $acknowledged = [];
        $collect      = static function (RecordMetadata $metadata) use (&$acknowledged): void {
            $acknowledged[] = $metadata;
        };

        $producer->send($this->topic, Record::fromValue('through the producer'), 0)->then($collect);
        $producer->send($this->topic, Record::fromKeyValue('key', 'and another one'), 0)->then($collect);

        // `batch.size` defaults to 0, so every record is sent on its own and both promises are already settled
        self::assertCount(2, $acknowledged);
        self::assertSame($this->topic, $acknowledged[0]->topic);
        self::assertSame(0, $acknowledged[0]->partition);
        self::assertSame([0, 1], array_column($acknowledged, 'offset'));
        self::assertCount(3, $producer->partitionsFor($this->topic));
    }

    public function testClientDoesNotWaitForAnAnswerWithAcksZero(): void
    {
        $configuration = $this->producerConfiguration(0) + ProducerConfig::getDefaultConfiguration();
        $cluster       = Cluster::bootstrap($configuration);

        $result = new Client($cluster, $configuration)
            ->produce([$this->topic => [1 => [Record::fromValue('no acknowledgement')]]]);

        self::assertSame([], $result, 'a fire-and-forget produce request has no response to report');

        // The very same connection carried the request, so the broker has appended it before answering this one
        $acknowledged = $this->produce(
            $cluster->leaderFor($this->topic, 1)->getConnection($configuration),
            SpecMessageSet::of([[null, 'acked']]),
            1,
            41,
            1
        );

        self::assertSame(0, $acknowledged->errorCode);
        self::assertSame(1, $acknowledged->baseOffset, 'the fire-and-forget message occupies the offset 0');
    }

    public function testEveryVersionOfTheAnswerCarriesTheFieldsOfItsOwnVersion(): void
    {
        $stream = $this->connect();

        new ProduceRequest(
            [$this->topic => [0 => SpecMessageSet::of([[null, 'version two']])]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            41
        )->writeTo($stream);
        $versionTwo = ProduceResponse::unpack($stream);

        self::assertSame(41, $versionTwo->getCorrelationId());
        self::assertSame(0, $versionTwo->topics[$this->topic]->partitions[0]->errorCode);
        self::assertSame(0, $versionTwo->throttleTime, 'the test broker enforces no producer quota');
        self::assertSame(-1, $versionTwo->topics[$this->topic]->partitions[0]->logAppendTime);

        new ProduceRequestV1(
            [$this->topic => [0 => SpecMessageSet::of([[null, 'version one']])]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            42
        )->writeTo($stream);
        $versionOne = ProduceResponseV1::unpack($stream);

        self::assertSame(42, $versionOne->getCorrelationId());
        self::assertSame(1, $versionOne->topics[$this->topic]->partitions[0]->baseOffset);
        self::assertSame(0, $versionOne->throttleTime);
        self::assertSame(
            $versionTwo->getMessageSize() - 8,
            $versionOne->getMessageSize(),
            'the LogAppendTime of the partition is the only difference between v2 and v1'
        );

        new ProduceRequestV0(
            [$this->topic => [0 => SpecMessageSet::of([[null, 'version zero']])]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            43
        )->writeTo($stream);
        $versionZero = ProduceResponseV0::unpack($stream);

        self::assertSame(43, $versionZero->getCorrelationId());
        self::assertSame(2, $versionZero->topics[$this->topic]->partitions[0]->baseOffset);
        self::assertSame(
            $versionOne->getMessageSize() - 4,
            $versionZero->getMessageSize(),
            'the ThrottleTime of version 1 is the only difference between v1 and v0'
        );
    }

    public function testTheAppendTimeOfATopicThatKeepsTheCreateTimeIsMinusOne(): void
    {
        $partition = $this->produce(
            $this->connect(),
            MessageSet::fromRecords([new Record('create time', null, 0, null, self::CREATE_TIME)])->toBuffer(),
            1,
            51
        );

        self::assertSame(0, $partition->errorCode);
        self::assertSame(
            ProduceResponsePartition::NO_LOG_APPEND_TIME,
            $partition->logAppendTime,
            'the broker stamped nothing, the timestamps of the producer are the ones the log holds'
        );
    }

    public function testALogAppendTimeTopicAnswersWithTheClockOfTheBroker(): void
    {
        $topic = $this->createLogAppendTimeTopic();

        $before    = (int) (microtime(true) * 1000);
        $partition = $this->produce(
            $this->connect(),
            MessageSet::fromRecords([
                new Record('append time', null, 0, null, self::CREATE_TIME),
                new Record('same batch', null, 0, null, self::CREATE_TIME + 1000),
            ])->toBuffer(),
            1,
            52,
            0,
            $topic
        );
        $after = (int) (microtime(true) * 1000);

        self::assertSame(0, $partition->errorCode);
        self::assertGreaterThanOrEqual($before, $partition->logAppendTime);
        self::assertLessThanOrEqual($after, $partition->logAppendTime);

        // That value is what the log really holds: the broker overwrote the CreateTime of every message of the
        // batch with it and marked the wrapper with the timestamp type LogAppendTime
        $stream = $this->connect();
        new FetchRequestV2([$topic => [0 => 0]], 1000, 1, 65536, -1, self::CLIENT_ID, 53)->writeTo($stream);
        $records = FetchResponseV2::unpack($stream)
            ->topics[$topic]
            ->partitions[0]
            ->getMessageSet()
            ->getRecords();

        self::assertCount(2, $records);
        self::assertSame(
            [$partition->logAppendTime, $partition->logAppendTime],
            array_map(static fn(Record $record): ?int => $record->timestamp, $records),
            'every message of the batch carries the append time the answer reported'
        );
        self::assertSame(
            [TimestampType::LOG_APPEND_TIME, TimestampType::LOG_APPEND_TIME],
            array_map(static fn(Record $record): int => $record->timestampType, $records)
        );
    }

    public function testAMessageFormatV0BatchIsAcceptedByAVersionTwoRequest(): void
    {
        // The broker does not check the message format against the api version: it converts whatever it is given
        // into the `message.format.version` of the topic, which is 0.10.2 on this container, so a magic 0 batch is
        // stored as message format v1 with the timestamp -1 (`Log.append` @ 0.10.2.2)
        $partition = $this->produce(
            $this->connect(),
            MessageSet::fromRecords([new Record('magic zero')], 0, Message::MAGIC_V0)->toBuffer(),
            1,
            54
        );

        self::assertSame(0, $partition->errorCode, 'a magic 0 message set is accepted by a version 2 request');
        self::assertSame(0, $partition->baseOffset);

        $stream = $this->connect();
        new FetchRequestV2([$this->topic => [0 => 0]], 1000, 1, 65536, -1, self::CLIENT_ID, 55)->writeTo($stream);
        $messageSet = FetchResponseV2::unpack($stream)->topics[$this->topic]->partitions[0]->getMessageSet();

        self::assertSame(Message::MAGIC_V1, $messageSet->getMagic(), 'the log holds it in the format of the topic');
        self::assertNull(
            $messageSet->getRecords()[0]->timestamp,
            'a message that was written without a timestamp is stored with -1, which is "no timestamp"'
        );
    }

    /**
     * Creates a topic whose broker stamps every message it appends with its own clock
     */
    private function createLogAppendTimeTopic(): string
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
        $topic = self::uniqueTopicName('t4-produce-lat');

        $errors = new AdminClient(Cluster::bootstrap($configuration), $configuration)->createTopics([
            new NewTopic($topic, 1, 1, [], ['message.timestamp.type' => 'LogAppendTime']),
        ]);
        self::assertSame([$topic => null], $errors, 'the controller created the LogAppendTime topic');

        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($topic);

        return $topic;
    }

    /**
     * Produces one message set to a partition of the topic of this test and returns the answer for that partition
     */
    private function produce(
        Stream $stream,
        string $messageSet,
        int $requiredAcks,
        int $correlationId,
        int $partition = 0,
        ?string $topic = null
    ): ProduceResponsePartition {
        $topic ??= $this->topic;
        $request = new ProduceRequest(
            [$topic => [$partition => $messageSet]],
            $requiredAcks,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            $correlationId
        );
        $request->writeTo($stream);

        $response = ProduceResponse::unpack($stream);
        self::assertSame($correlationId, $response->getCorrelationId());
        self::assertArrayHasKey($topic, $response->topics);

        return $response->topics[$topic]->partitions[$partition];
    }

    /**
     * @return array<string, mixed>
     */
    private function producerConfiguration(int $requiredAcks): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID         => self::CLIENT_ID,
            ProducerConfig::ACKS            => $requiredAcks,
            ProducerConfig::TIMEOUT_MS      => self::PRODUCE_TIMEOUT_MS,
        ];
    }
}
