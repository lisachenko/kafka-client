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
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownTopicIdException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Network\RetryPolicy;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV12;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV12;
use Protocol\Kafka\Tests\Fixture\RemovedVersionProbe;

/**
 * Produce v13 (Kafka 4.1, KIP-516) against the node: every topic is named by its id, in the request and the answer
 *
 * `ProduceRequest.json` @ 4.1.0: "Version 13 replaces topic names with topic IDs (KIP-516). May return
 * UNKNOWN_TOPIC_ID error code." The node resolves the id through its metadata cache, answers an id it does not have
 * with the per-partition 100, and closes the connection instead when the request was sent with acks = 0.
 * `Client::produce()` resolves the ids through the cluster and reloads it for an id that went stale.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v13)" and "The topic ids of the produce path (v13,
 *      KIP-516)"
 */
#[CoversClass(ProduceRequest::class)]
#[CoversClass(ProduceResponse::class)]
#[CoversClass(ProduceRequestV12::class)]
#[CoversClass(ProduceResponseV12::class)]
#[CoversClass(Client::class)]
#[CoversClass(Cluster::class)]
#[CoversClass(RetryPolicy::class)]
final class ProduceTopicIdsApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t2-41-produce';

    private const int TIMEOUT_MS = 30000;

    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * Id of a topic no cluster hosts
     */
    private const string UNKNOWN_TOPIC_ID = "\x0b\xad\xc0\xde\x0b\xad\xc0\xde\x0b\xad\xc0\xde\x0b\xad\xc0\xde";

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

        $this->admin = new AdminClient(Cluster::bootstrap($this->configuration()), $this->configuration());
        $this->topic = self::uniqueTopicName('t2-41-produce');
        $this->createTopic($this->topic);
        $this->topicId = $this->currentTopicIdOf($this->topic);
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

    public function testAVersionThirteenBatchIsAnsweredUnderTheIdOfItsTopic(): void
    {
        $stream = $this->connect();
        $answer = $this->send(
            $stream,
            new ProduceRequest(
                [$this->topic => [0 => $this->batch('by id')]],
                1,
                self::TIMEOUT_MS,
                self::CLIENT_ID,
                4101,
                null,
                [$this->topic => $this->topicId]
            ),
            ProduceResponse::class
        );

        self::assertSame(13, $answer::VERSION);
        self::assertCount(1, $answer->topics);
        self::assertSame($this->topicId, $answer->topics[0]->topicId, 'the answer names the topic by its id');
        self::assertSame('', $answer->topics[0]->topic, 'and not by its name');
        $partition = $answer->topics[0]->partitions[0];
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(0, $partition->baseOffset);
        self::assertSame(-1, $partition->logAppendTime);
        self::assertSame(0, $partition->logStartOffset);
        self::assertSame([], $answer->nodeEndpoints);

        // The same batch at version 12 is answered by name, with the very same partition entry
        $named = $this->send(
            $stream,
            new ProduceRequestV12([$this->topic => [0 => $this->batch('by name')]], 1, self::TIMEOUT_MS, self::CLIENT_ID, 4102),
            ProduceResponseV12::class
        );
        $namedPartition = $named->topics[$this->topic]->partitions[0];
        self::assertSame(KafkaException::NO_ERROR, $namedPartition->errorCode);
        self::assertSame(1, $namedPartition->baseOffset, 'both batches reached the same log');
        self::assertSame(Uuid::ZERO, $named->topics[$this->topic]->topicId);
    }

    public function testAnIdTheNodeDoesNotHaveIsRefusedWithTheCode100AndNothingIsAppended(): void
    {
        $stream = $this->connect();
        foreach ([1, -1] as $correlationId => $acks) {
            $answer = $this->send(
                $stream,
                new ProduceRequest(
                    [$this->topic => [0 => $this->batch('to nowhere')]],
                    $acks,
                    self::TIMEOUT_MS,
                    self::CLIENT_ID,
                    4110 + $correlationId,
                    null,
                    [$this->topic => self::UNKNOWN_TOPIC_ID]
                ),
                ProduceResponse::class
            );

            self::assertSame(self::UNKNOWN_TOPIC_ID, $answer->topics[0]->topicId, 'the id of the request comes back');
            $partition = $answer->topics[0]->partitions[0];
            self::assertSame(KafkaException::UNKNOWN_TOPIC_ID, $partition->errorCode, "acks = {$acks}");
            self::assertSame(-1, $partition->baseOffset);
            self::assertSame(-1, $partition->logStartOffset);
        }

        // The zero uuid, which this client refuses to write, is answered the same way: the frame of the real id with
        // its 16 bytes zeroed
        $frame = (string) new ProduceRequest(
            [$this->topic => [0 => $this->batch('to the zero id')]],
            1,
            self::TIMEOUT_MS,
            self::CLIENT_ID,
            4112,
            null,
            [$this->topic => $this->topicId]
        );
        $stream->write('a*', str_replace($this->topicId, Uuid::ZERO, $frame));
        $zero = ProduceResponse::unpack($stream);
        self::assertSame(Uuid::ZERO, $zero->topics[0]->topicId);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_ID, $zero->topics[0]->partitions[0]->errorCode);

        self::assertSame([$this->topic => [0 => 0]], $this->admin->listOffsets([$this->topic => [0]]), 'nothing was appended');
    }

    public function testAPartitionTheTopicDoesNotHaveIsRefusedWithTheCode3(): void
    {
        $answer = $this->send(
            $this->connect(),
            new ProduceRequest(
                [$this->topic => [7 => $this->batch('to partition 7')]],
                1,
                self::TIMEOUT_MS,
                self::CLIENT_ID,
                4120,
                null,
                [$this->topic => $this->topicId]
            ),
            ProduceResponse::class
        );

        self::assertSame($this->topicId, $answer->topics[0]->topicId);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $answer->topics[0]->partitions[7]->errorCode);
    }

    public function testAcksZeroWithTheIdOfTheTopicIsNotAnsweredButAppends(): void
    {
        $stream  = $this->connect();
        $request = new ProduceRequest(
            [$this->topic => [0 => $this->batch('fire and forget')]],
            0,
            self::TIMEOUT_MS,
            self::CLIENT_ID,
            4130,
            null,
            [$this->topic => $this->topicId]
        );
        self::assertFalse($request->expectsResponse());
        $request->writeTo($stream);

        // Nothing comes back for that request: the next answer on this connection belongs to the next request
        new MetadataRequest([$this->topic], false, self::CLIENT_ID, 4131)->writeTo($stream);
        self::assertSame(4131, MetadataResponse::unpack($stream)->getCorrelationId());

        // ... and the batch was appended nevertheless, so the next one starts at the offset 1
        $answer = $this->send(
            $stream,
            new ProduceRequest(
                [$this->topic => [0 => $this->batch('acknowledged')]],
                1,
                self::TIMEOUT_MS,
                self::CLIENT_ID,
                4132,
                null,
                [$this->topic => $this->topicId]
            ),
            ProduceResponse::class
        );
        self::assertSame(KafkaException::NO_ERROR, $answer->topics[0]->partitions[0]->errorCode);
        self::assertSame(1, $answer->topics[0]->partitions[0]->baseOffset);
    }

    public function testAnUnknownIdAtAcksZeroClosesTheConnection(): void
    {
        $probe = new RemovedVersionProbe(self::firstBootstrapServer());

        self::assertSame(
            RemovedVersionProbe::CLOSED,
            $probe->send(new ProduceRequest(
                [$this->topic => [0 => $this->batch('to nowhere, unanswered')]],
                0,
                self::TIMEOUT_MS,
                self::CLIENT_ID,
                4140,
                null,
                [$this->topic => self::UNKNOWN_TOPIC_ID]
            )),
            'a refused batch of a request the node would not answer closes the connection'
        );
        self::assertSame(
            RemovedVersionProbe::SILENT,
            $probe->send(new ProduceRequest(
                [$this->topic => [0 => $this->batch('to the topic, unanswered')]],
                0,
                self::TIMEOUT_MS,
                self::CLIENT_ID,
                4141,
                null,
                [$this->topic => $this->topicId]
            )),
            'and an accepted one leaves it open'
        );
    }

    public function testTheClientProducesByIdAndReportsThePartitionsUnderTheName(): void
    {
        $client = new Client(Cluster::bootstrap($this->configuration()), $this->configuration());

        $result = $client->produce([$this->topic => [0 => [new Record('through the client')]]]);

        self::assertSame([$this->topic], array_keys($result));
        self::assertInstanceOf(ProduceResponsePartition::class, $result[$this->topic][0]);
        self::assertSame(0, $result[$this->topic][0]->baseOffset);

        $records = $client->fetchPartitions([$this->topic => [0 => 0]], 1000);
        self::assertSame('through the client', $records[$this->topic][0]->getRecords()[0]->value);
    }

    public function testTheClientProducesToATopicThatWasCreatedAfterItReadTheMetadata(): void
    {
        $client = new Client(Cluster::bootstrap($this->configuration()), $this->configuration());
        $client->produce([$this->topic => [0 => [new Record('known topic')]]]);

        // A topic the cluster of the client has never seen: its leader and its id arrive with one reload
        $fresh = self::uniqueTopicName('t2-41-produce-fresh');
        $this->createTopic($fresh);
        try {
            $result = $client->produce([$fresh => [0 => [new Record('fresh topic')]]]);

            self::assertSame(0, $result[$fresh][0]->baseOffset);
        } finally {
            $this->admin->deleteTopics([$fresh]);
        }
    }

    public function testARecreatedTopicIsReportedOnceAndThenProducedToUnderItsNewId(): void
    {
        // No retry at all, the default of the producer: the stale id costs exactly one reported batch
        $client = new Client(
            Cluster::bootstrap($this->configuration()),
            [ClientConfig::RETRIES => 0] + $this->configuration()
        );
        self::assertSame(0, $client->produce([$this->topic => [0 => [new Record('first life')]]])[$this->topic][0]->baseOffset);

        $oldId = $this->topicId;
        $this->recreateTopic($this->topic);
        $newId = $this->currentTopicIdOf($this->topic);
        self::assertNotSame($oldId, $newId, 'a recreated topic has a new id');

        try {
            $client->produce([$this->topic => [0 => [new Record('to the deleted topic')]]]);
            self::fail('the batch for the id of the deleted topic is expected to be refused');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(UnknownTopicIdException::class, $exception->getExceptions()[$this->topic][0]);
        }

        // ... and the metadata was reloaded with the failure, so the next call names the new id
        $result = $client->produce([$this->topic => [0 => [new Record('second life')]]]);
        self::assertSame(0, $result[$this->topic][0]->baseOffset, 'the first record of the new topic');

        // With a retry left, the same situation is repaired inside one call
        $retrying = new Client(
            Cluster::bootstrap($this->configuration()),
            [ClientConfig::RETRIES => 2] + $this->configuration()
        );
        $retrying->produce([$this->topic => [0 => [new Record('second life, again')]]]);
        $this->recreateTopic($this->topic);
        $third = $retrying->produce([$this->topic => [0 => [new Record('third life')]]]);
        self::assertSame(0, $third[$this->topic][0]->baseOffset);
    }

    /**
     * @template T of ProduceResponse
     *
     * @param class-string<T> $responseClass
     *
     * @return T
     */
    private function send(SocketStream $stream, ProduceRequest $request, string $responseClass): ProduceResponse
    {
        $request->writeTo($stream);

        return $responseClass::unpack($stream);
    }

    private function batch(string $value): RecordBatch
    {
        return RecordBatch::fromRecords([new Record($value, 'k')->withCreateTime(1790000000000)]);
    }

    private function createTopic(string $topic): void
    {
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            // A topic that was only just deleted may still be on its way out: its name is taken until it is gone
            $error = $this->admin->createTopics([new NewTopic($topic, 1, 1)])[$topic];
            if ($error === null) {
                break;
            }
            usleep(250000);
        } while (microtime(true) < $deadline);
        self::assertNull($error ?? null, "the topic {$topic} could not be created");

        // A fresh partition needs a moment before its leader answers
        do {
            try {
                $this->admin->listOffsets([$topic => [0]]);

                return;
            } catch (KafkaException) {
                usleep(100000);
            }
        } while (microtime(true) < $deadline);

        self::fail("the topic {$topic} got no leader within the timeout");
    }

    private function recreateTopic(string $topic): void
    {
        $this->admin->deleteTopics([$topic]);
        $this->createTopic($topic);
    }

    /**
     * Asks the node for the id of the topic, without the cache of {@see IntegrationTestCase::topicIdOf()}: a
     * recreated topic has a new one
     */
    private function currentTopicIdOf(string $topic): string
    {
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            $stream = $this->connect();
            new MetadataRequest([$topic], false, self::CLIENT_ID, 4100)->writeTo($stream);
            $topicId = MetadataResponse::unpack($stream)->topics[$topic]->topicId ?? Uuid::ZERO;
            if (!Uuid::isZero($topicId)) {
                return $topicId;
            }
            usleep(100000);
        } while (microtime(true) < $deadline);

        self::fail("the node has no id for the topic {$topic}");
    }

    /**
     * @return array<string, mixed> Client configuration of this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => self::TIMEOUT_MS,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::TIMEOUT_MS,

            ProducerConfig::ACKS                    => ProducerConfig::ACKS_LEADER,
            ProducerConfig::TIMEOUT_MS              => self::TIMEOUT_MS,

            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
        ] + ConsumerConfig::getDefaultConfiguration() + ProducerConfig::getDefaultConfiguration();
    }
}
