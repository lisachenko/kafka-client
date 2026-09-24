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
use Protocol\Kafka\Admin\ProducerState;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersRequestMarker;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersRequestMarkerV1;
use Protocol\Kafka\Protocol\Request\EndTxnRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersRequest;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersRequestV1;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersResponse;
use Protocol\Kafka\Protocol\Request\WriteTxnMarkersResponseV1;

/**
 * WriteTxnMarkers (key 27) at the version 2 of Kafka 4.2 (KIP-1228) against the client listener of the 4.3.1 node.
 *
 * The api is broker-to-broker - what a transaction coordinator sends to the leaders of a transaction's partitions -
 * and this client has no method for it by design. The PLAINTEXT listener serves it because its `User:ANONYMOUS` is
 * a super user of the `StandardAuthorizer` and so holds the `ClusterAction` the api needs. What the version 2 adds is
 * the `transaction_version` of a marker, which decides how the leader checks the epoch of the marker: a marker of the
 * protocol v2 needs an epoch above the one of the transaction it ends, a legacy one only not below it.
 *
 * The transaction a test ends by hand is a real one of a transactional {@see KafkaProducer} of the protocol v2, which
 * aborts it afterwards; every topic of this class is deleted again. The transactional ids carry the `t4-42-wtm-`
 * prefix and a random suffix.
 *
 * @see docs/protocol/4.3.md, section "What Kafka 4.2 changed here (KIP-1228): the version 2"
 */
#[CoversClass(WriteTxnMarkersRequest::class)]
#[CoversClass(WriteTxnMarkersRequestV1::class)]
#[CoversClass(WriteTxnMarkersResponse::class)]
#[CoversClass(WriteTxnMarkersResponseV1::class)]
#[CoversClass(WriteTxnMarkersRequestMarker::class)]
#[CoversClass(WriteTxnMarkersRequestMarkerV1::class)]
final class WriteTxnMarkersApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t4-42-wtm';

    private const float TIMEOUT = 30.0;

    private Cluster $cluster;

    private AdminClient $admin;

    /**
     * @var list<string>
     */
    private array $createdTopics = [];

    private int $correlationId = 4270;

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    public function testAnEmptyVersionTwoFrameIsAnsweredWithAnEmptyArray(): void
    {
        $answer = $this->exchange(new WriteTxnMarkersRequest([], self::CLIENT_ID, $this->correlationId++));

        self::assertSame(2, $answer::VERSION);
        self::assertSame([], $answer->transactionMarkers);
    }

    /**
     * The marker of the protocol v2 at the epoch of an open transaction is refused, the legacy one ends it
     */
    public function testTheTransactionVersionDecidesHowTheEpochOfAMarkerIsChecked(): void
    {
        $topic    = $this->topic('open');
        $producer = $this->producerWithAnOpenTransaction($topic);
        $state    = $this->producerState($topic);
        $pid      = $state->producerId;
        $epoch    = $state->producerEpoch;

        try {
            self::assertTrue($state->hasOpenTransaction());

            $strict = $this->markerError(new WriteTxnMarkersRequestMarker($pid, $epoch, EndTxnRequest::ABORT, [$topic => [0]], 0, 2), $topic);
            self::assertSame(KafkaException::INVALID_PRODUCER_EPOCH, $strict, 'a marker of the protocol v2 needs an epoch above the transaction');
            self::assertTrue($this->producerState($topic)->hasOpenTransaction(), 'the refused marker ended nothing');

            $legacy = $this->markerError(new WriteTxnMarkersRequestMarker($pid, $epoch, EndTxnRequest::ABORT, [$topic => [0]], 0, 0), $topic);
            self::assertSame(KafkaException::NO_ERROR, $legacy, 'a legacy marker only needs the epoch of the transaction');
            self::assertFalse($this->producerState($topic)->hasOpenTransaction(), 'the legacy marker ended the transaction');

            $retry = $this->markerError(new WriteTxnMarkersRequestMarker($pid, $epoch, EndTxnRequest::ABORT, [$topic => [0]], 0, 2), $topic);
            self::assertSame(KafkaException::NO_ERROR, $retry, 'the idempotent retry of KAFKA-19999: no transaction is open any more');

            $unknown = $this->markerError(new WriteTxnMarkersRequestMarker($pid, $epoch + 1, EndTxnRequest::ABORT, [$topic => [7]], 0, 2), $topic, 7);
            self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $unknown);
        } finally {
            $producer->abortTransaction();
        }

        // The abort of the coordinator bumped the epoch - its marker reaches the partition after the EndTxn answer -
        // and a marker below that epoch is refused whatever its version
        $deadline = microtime(true) + self::TIMEOUT;
        while (($bumped = $this->producerState($topic))->producerEpoch === $epoch && microtime(true) < $deadline) {
            usleep(200000);
        }
        self::assertSame($epoch + 1, $bumped->producerEpoch, 'every end of a transaction of the protocol v2 bumps the epoch');
        self::assertSame(
            KafkaException::INVALID_PRODUCER_EPOCH,
            $this->markerError(new WriteTxnMarkersRequestMarker($pid, $epoch, EndTxnRequest::ABORT, [$topic => [0]], 0, 0), $topic)
        );
        self::assertSame(
            KafkaException::INVALID_PRODUCER_EPOCH,
            $this->markerError(new WriteTxnMarkersRequestMarker($pid, $epoch, EndTxnRequest::ABORT, [$topic => [0]]), $topic, 0, true),
            'the version 1 frame, whose markers have no transaction version'
        );
    }

    /**
     * Sends one marker of the given producer and returns the error code of the partition
     */
    private function markerError(
        WriteTxnMarkersRequestMarker $marker,
        string $topic,
        int $partition = 0,
        bool $versionOne = false
    ): int {
        $request = $versionOne
            ? new WriteTxnMarkersRequestV1([$marker], self::CLIENT_ID, $this->correlationId++)
            : new WriteTxnMarkersRequest([$marker], self::CLIENT_ID, $this->correlationId++);
        $answer  = $this->exchange($request);

        $result = $answer->transactionMarkers[$marker->producerId]->topics[$topic]->partitions[$partition] ?? null;
        self::assertNotNull($result, "the answer names {$topic}-{$partition}");

        return $result->errorCode;
    }

    /**
     * Sends one frame on a fresh connection to the one node, the leader of every partition, and decodes the answer
     */
    private function exchange(WriteTxnMarkersRequest $request): WriteTxnMarkersResponse
    {
        $stream = $this->connect();
        $request->writeTo($stream);

        return $request::VERSION >= 2 ? WriteTxnMarkersResponse::unpack($stream) : WriteTxnMarkersResponseV1::unpack($stream);
    }

    /**
     * The one producer the partition remembers, read with DescribeProducers (KIP-664)
     */
    private function producerState(string $topic): ProducerState
    {
        $producers = $this->admin->describeProducers([$topic => [0]])[$topic][0];
        self::assertIsArray($producers);
        self::assertCount(1, $producers);

        return $producers[0];
    }

    /**
     * A transactional producer of the protocol v2 whose transaction holds one record of the partition 0 of the topic
     */
    private function producerWithAnOpenTransaction(string $topic): KafkaProducer
    {
        $producer = new KafkaProducer([
            ProducerConfig::TRANSACTIONAL_ID          => 't4-42-wtm-tx-' . bin2hex(random_bytes(6)),
            ProducerConfig::CLIENT_ID                 => self::CLIENT_ID,
            ProducerConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ProducerConfig::REQUEST_TIMEOUT_MS        => 40000,
            ProducerConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ]);
        $producer->initTransactions();
        $producer->beginTransaction();
        $producer->send($topic, new Record('open', null, 0, null, (int) (microtime(true) * 1000)), 0);
        $producer->flush();

        return $producer;
    }

    private function topic(string $purpose): string
    {
        $topic                 = self::uniqueTopicName("t4-42-wtm-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));

        $deadline = microtime(true) + self::TIMEOUT;
        do {
            try {
                $this->cluster->reload();
                $this->admin->listOffsets([$topic => [0]], OffsetsRequest::LATEST);

                return $topic;
            } catch (KafkaException | TopicPartitionRequestException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
            }
        } while (true);
    }

    /**
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }
}
