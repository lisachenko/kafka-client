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
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;
use Protocol\Kafka\Protocol\Data\ProduceRequestPartition;
use Protocol\Kafka\Protocol\Data\ProduceRequestTopic;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopic;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV0;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV0;
use Protocol\Kafka\Tests\Fixture\SpecMessageSet;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the Produce API v0 against a real Kafka 0.8.2.2 broker.
 *
 * The broker validates the CRC of every message it appends, so a green run here also proves that the message sets
 * built by {@see SpecMessageSet} follow the specification.
 *
 * @see docs/protocol/0.9.0.md, section "Produce API (key 0, v0 and v1)"
 */
#[CoversClass(ProduceRequest::class)]
#[CoversClass(ProduceRequestV0::class)]
#[CoversClass(ProduceResponse::class)]
#[CoversClass(ProduceResponseV0::class)]
#[CoversClass(ProduceRequestTopic::class)]
#[CoversClass(ProduceRequestPartition::class)]
#[CoversClass(ProduceResponseTopic::class)]
#[CoversClass(ProduceResponsePartition::class)]
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

    public function testVersion1AnswerCarriesAThrottleTimeAndVersion0DoesNot(): void
    {
        $stream = $this->connect();

        new ProduceRequest(
            [$this->topic => [0 => SpecMessageSet::of([[null, 'throttled?']])]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            41
        )->writeTo($stream);
        $versionOne = ProduceResponse::unpack($stream);

        self::assertSame(41, $versionOne->getCorrelationId());
        self::assertSame(0, $versionOne->topics[$this->topic]->partitions[0]->errorCode);
        self::assertSame(0, $versionOne->throttleTime, 'the test broker enforces no producer quota');

        new ProduceRequestV0(
            [$this->topic => [0 => SpecMessageSet::of([[null, 'not throttled']])]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            42
        )->writeTo($stream);
        $versionZero = ProduceResponseV0::unpack($stream);

        self::assertSame(42, $versionZero->getCorrelationId());
        self::assertSame(1, $versionZero->topics[$this->topic]->partitions[0]->baseOffset);
        self::assertSame(
            $versionOne->getMessageSize() - 4,
            $versionZero->getMessageSize(),
            'the ThrottleTime of version 1 is the only difference between the two answers'
        );
    }

    /**
     * Produces one message set to a partition of the topic of this test and returns the answer for that partition
     */
    private function produce(
        Stream $stream,
        string $messageSet,
        int $requiredAcks,
        int $correlationId,
        int $partition = 0
    ): ProduceResponsePartition {
        $request = new ProduceRequest(
            [$this->topic => [$partition => $messageSet]],
            $requiredAcks,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            $correlationId
        );
        $request->writeTo($stream);

        $response = ProduceResponse::unpack($stream);
        self::assertSame($correlationId, $response->getCorrelationId());
        self::assertArrayHasKey($this->topic, $response->topics);

        return $response->topics[$this->topic]->partitions[$partition];
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
