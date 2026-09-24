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
use Protocol\Kafka\Common\Errors\InvalidTxnStateException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\TransactionAbortableException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\Internals\TransactionState;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV10;
use Protocol\Kafka\Protocol\Request\ProduceRequestV11;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV10;
use Protocol\Kafka\Protocol\Request\ProduceResponseV11;

/**
 * What **Kafka 3.8** adds to the produce path: the abortable transaction error of KIP-890.
 *
 * **Produce v11** leaves both halves of the api alone - `ProduceRequest.json` and `ProduceResponse.json` @ 3.8.1
 * declare no field of it and carry the one comment "Version 11 adds support for new error code
 * TRANSACTION_ABORTABLE (KIP-890)" - and changes one thing: the error code a transactional batch is refused with
 * when the transaction coordinator has not verified its partition. Above version 10 that is **120**
 * `TransactionAbortable`, "abort this transaction and carry on with the same transactional id"; through version
 * 10 it is the **48** `InvalidTxnState` with the message "Partition was not added to the transaction", which
 * reads like the end of the producer.
 *
 * `KafkaApis.handleProduceRequest` @ 3.9.2 decides that on the api version and on nothing else -
 * `val transactionSupportedOperation = if (request.header.apiVersion > 10) genericError else defaultError` - and
 * `AddPartitionsToTxnManager` @ 3.9.2 maps the 120 of the coordinator back to the 48 for everybody below. So the
 * two frames of this class are the very same request under two version numbers, and the difference is entirely
 * the broker's answer.
 *
 * **Kafka 4.0 added Produce v12 (KIP-890 part 2)**, the same frame once more, and on a node that finalizes
 * `transaction.version` 2 - the node of this line does - it changes what the very same transactional batch means:
 * "the produce request will also include the function for a AddPartitionsToTxn call", so the partition the
 * coordinator was never told about is added by the broker and the batch is appended, where version 11 is refused
 * the 120. That is why the client caps a transaction of the protocol v1 at {@see ProduceRequestV11}, see
 * {@see Client::produceVersion()}, and why every frame of the version 11 measurements below names that class.
 *
 * Every topic, group and transactional id of this class is named `t2-38-…`, so that it can run next to the other
 * suites on the shared node.
 *
 * @see docs/protocol/4.3.md, sections "The abortable transaction error of KIP-890 (v11)", "The transaction protocol
 *      v2 of KIP-890 part 2 (v12)" and "Produce API (key 0, v0 to v12)"
 */
#[CoversClass(ProduceRequest::class)]
#[CoversClass(ProduceResponse::class)]
#[CoversClass(ProduceRequestV10::class)]
#[CoversClass(ProduceResponseV10::class)]
#[CoversClass(ProduceRequestV11::class)]
#[CoversClass(ProduceResponseV11::class)]
#[CoversClass(TransactionAbortableException::class)]
#[CoversClass(TransactionManager::class)]
#[CoversClass(Client::class)]
final class TransactionAbortableProduceApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t2-38';

    private const int REQUEST_TIMEOUT_MS = 30000;

    /**
     * The partition that is added to the transaction, and the one that is not
     */
    private const int VERIFIED_PARTITION = 0;

    private const int UNVERIFIED_PARTITION = 1;

    private const float TOPIC_TIMEOUT = 30.0;

    private static ?Cluster $sharedCluster = null;

    private AdminClient $admin;

    private Client $client;

    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->admin  = new AdminClient($this->cluster(), $this->configuration());
        $this->client = new Client($this->cluster(), $this->configuration());
        $this->topic  = self::uniqueTopicName('t2-38-abortable');

        self::assertSame([$this->topic => null], $this->admin->createTopics([new NewTopic($this->topic, 2, 1)]));
        $this->awaitTopic($this->topic);
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

    public function testTheNodeServesVersionTwelveAndFinalizesTheTransactionProtocolV2(): void
    {
        $stream = $this->connect();

        try {
            new ApiVersionsRequest(self::CLIENT_ID, 3880)->writeTo($stream);
            $answer = ApiVersionsResponse::unpack($stream);
        } finally {
            $stream->disconnect();
        }

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertGreaterThanOrEqual(12, $answer->apiVersions[0]->maxVersion, 'the node serves Produce v12 (4.0)');
        self::assertSame(12, ProduceRequest::VERSION);
        self::assertSame(12, ProduceResponse::VERSION);
        self::assertSame(11, ProduceRequestV11::VERSION);
        self::assertSame(11, ProduceResponseV11::VERSION);
        self::assertSame(10, ProduceRequestV10::VERSION);
        self::assertSame(10, ProduceResponseV10::VERSION);

        // KIP-890 part 2 - the transaction protocol v2, which replaces the verification round trip - rides on the
        // cluster feature `transaction.version`, which a 3.9.2 node did not know and a 4.3.1 node finalizes at 2:
        // a transactional Produce v12 is the protocol v2 on this node, see
        // testAVersionTwelveTransactionalBatchAddsItsPartitionToTheTransactionItself()
        self::assertArrayHasKey('transaction.version', $answer->finalizedFeatures);
        self::assertSame(2, $answer->finalizedFeatures['transaction.version']->maxVersionLevel);
        self::assertSame(
            ProduceRequestV11::VERSION,
            Client::produceVersion(RecordBatch::MAGIC, ProduceRequestV11::VERSION),
            'the version a transaction of the protocol v1 is capped at'
        );
    }

    public function testAVersionTwelveTransactionalBatchAddsItsPartitionToTheTransactionItself(): void
    {
        // The same frame as the version 11 one below that is refused the 120: a transactional batch for a
        // partition no AddPartitionsToTxn announced. At version 12 on a `transaction.version` 2 node the broker
        // adds the partition itself (`AddPartitionsToTxnManager.produceRequestVersionToTransactionSupportedOperation`
        // @ 4.0.0: `if (version > 11) addPartition`) and appends the batch
        $transactionalId = self::uniqueTransactionalId('t2-40-v2');
        $this->client->getTransactionCoordinator($transactionalId);
        $idAndEpoch = $this->client->initProducerId($transactionalId, 60000);

        try {
            $twelve    = $this->send(
                $this->transactionalRequest(ProduceRequest::class, 3886, $transactionalId, $idAndEpoch),
                ProduceResponse::class
            );
            $partition = $twelve->topics[$this->topic]->partitions[self::UNVERIFIED_PARTITION];

            self::assertSame(KafkaException::NO_ERROR, $partition->errorCode, 'no 120: the broker added the partition');
            self::assertSame(0, $partition->baseOffset, 'and appended the batch, the first of the partition');
            self::assertNull($partition->errorMessage);
        } finally {
            $this->abortQuietly($transactionalId, $idAndEpoch);
        }
    }

    public function testAVersionElevenProduceIsTheVersionTenFrameWithAnotherApiVersion(): void
    {
        $ten    = $this->produceRequest(ProduceRequestV10::class, 3881, self::VERIFIED_PARTITION);
        $eleven = $this->produceRequest(ProduceRequestV11::class, 3881, self::VERIFIED_PARTITION);

        self::assertSame(
            bin2hex((string) $ten),
            substr_replace(bin2hex((string) $eleven), '000a', 12, 4),
            'KIP-890 added no field to the request: the api version of the header is the whole difference'
        );

        $answer    = $this->send($eleven, ProduceResponseV11::class);
        $partition = $answer->topics[$this->topic]->partitions[self::VERIFIED_PARTITION];

        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(0, $partition->baseOffset, 'the first batch of a fresh partition');
        self::assertNull($partition->errorMessage);
        self::assertSame([], $partition->recordErrors);
        self::assertNull($partition->currentLeader, 'nothing of KIP-951 either, on a one-broker node');
        self::assertSame([], $answer->nodeEndpoints);
        self::assertSame(
            strlen((string) $this->send($this->produceRequest(ProduceRequestV10::class, 3882), ProduceResponseV10::class)),
            strlen((string) $answer),
            'so the answer of version 11 is the answer of version 10, byte count and all'
        );
    }

    public function testAnUnverifiedPartitionIsAnsweredTheAbortableCodeAtElevenAndTheInvalidStateAtTen(): void
    {
        $transactionalId = self::uniqueTransactionalId('t2-38-abortable');
        $idAndEpoch      = $this->openTransactionOnTheVerifiedPartition($transactionalId);

        try {
            $eleven = $this->send(
                $this->transactionalRequest(ProduceRequestV11::class, 3883, $transactionalId, $idAndEpoch),
                ProduceResponseV11::class
            );
            $ten = $this->send(
                $this->transactionalRequest(ProduceRequestV10::class, 3884, $transactionalId, $idAndEpoch),
                ProduceResponseV10::class
            );

            $abortable = $eleven->topics[$this->topic]->partitions[self::UNVERIFIED_PARTITION];
            $legacy    = $ten->topics[$this->topic]->partitions[self::UNVERIFIED_PARTITION];

            self::assertSame(
                KafkaException::TRANSACTION_ABORTABLE,
                $abortable->errorCode,
                'above version 10 the verification failure travels as the 120 of KIP-890'
            );
            self::assertSame(-1, $abortable->baseOffset, 'nothing was appended');
            self::assertSame(-1, $abortable->logStartOffset);
            self::assertNull(
                $abortable->errorMessage,
                'ReplicaManager.handleProduceAppend @ 3.9.2 words the 48 alone, never the 120'
            );
            self::assertSame([], $abortable->recordErrors);
            self::assertNull($abortable->currentLeader, 'the 120 is not a code of KIP-951');

            self::assertSame(
                KafkaException::INVALID_TXN_STATE,
                $legacy->errorCode,
                'and through version 10 the very same condition is the 48'
            );
            self::assertSame(
                'Partition was not added to the transaction',
                $legacy->errorMessage,
                'which the broker does word'
            );
            self::assertGreaterThan(
                strlen((string) $eleven),
                strlen((string) $ten),
                'so the version 10 answer is the longer of the two, by the message it carries'
            );
        } finally {
            $this->abortQuietly($transactionalId, $idAndEpoch);
        }
    }

    public function testTheVerifiedPartitionOfTheSameTransactionIsAppendedAtVersionEleven(): void
    {
        $transactionalId = self::uniqueTransactionalId('t2-38-verified');
        $idAndEpoch      = $this->openTransactionOnTheVerifiedPartition($transactionalId);

        try {
            $answer = $this->send(
                $this->transactionalRequest(
                    ProduceRequestV11::class,
                    3885,
                    $transactionalId,
                    $idAndEpoch,
                    self::VERIFIED_PARTITION
                ),
                ProduceResponseV11::class
            );
            $partition = $answer->topics[$this->topic]->partitions[self::VERIFIED_PARTITION];

            self::assertSame(
                KafkaException::NO_ERROR,
                $partition->errorCode,
                'the partition the coordinator verified is appended, at version 11 as at every version before it'
            );
            self::assertSame(0, $partition->baseOffset);
        } finally {
            $this->abortQuietly($transactionalId, $idAndEpoch);
        }
    }

    public function testTheAbortableCodeLeavesTheProducerUsableWithTheSameTransactionalId(): void
    {
        // What the 120 is for: the batch fails, the transaction can not be committed any more, and the producer
        // carries on with the SAME transactional id once it has aborted. `Client::produce()` reports the code as
        // the exception of that partition and `TransactionManager::batchFailed()` moves the producer into
        // ABORTABLE_ERROR, from which only `abortTransaction()` leads out. The node finalizes `transaction.version`
        // 2, on which a producer of the protocol v2 never meets the 120 (its Produce v12 enrols the partition, see
        // the test below), so the producer of this test is held on the protocol v1 - Produce v11 inside the
        // transaction, AddPartitionsToTxn in front of it - which is what a 3.x cluster would make of it
        $transactionalId = self::uniqueTransactionalId('t2-38-recover');
        $manager         = $this->transactionManagerOfTheProtocolV1($transactionalId);

        $manager->initTransactions();
        $manager->beginTransaction();
        // Only the partition 0 is announced to the coordinator, so the batch below is the unverified one
        $manager->maybeAddPartitionsToTransaction([$this->topic => [self::VERIFIED_PARTITION => []]]);

        try {
            $this->client->produce(
                [$this->topic => [self::UNVERIFIED_PARTITION => [new Record('t2-38 unverified')]]],
                $manager
            );
            self::fail('a produce into a partition the coordinator has not verified can not succeed');
        } catch (TopicPartitionRequestException $exception) {
            $error = $exception->getExceptions()[$this->topic][self::UNVERIFIED_PARTITION];

            self::assertInstanceOf(TransactionAbortableException::class, $error);
            self::assertNotInstanceOf(InvalidTxnStateException::class, $error, 'the code of version 11, not the 48');
            self::assertSame(KafkaException::TRANSACTION_ABORTABLE, $error->getCode());
        }

        self::assertTrue($manager->hasAbortableError(), 'the transaction can not be committed any more');
        self::assertFalse($manager->hasFatalError(), 'but the producer itself is intact');
        self::assertSame(TransactionState::ABORTABLE_ERROR, $manager->currentState());

        try {
            $manager->commitTransaction();
            self::fail('an abortable error has to refuse the commit');
        } catch (TransactionAbortableException $expected) {
            self::assertSame(KafkaException::TRANSACTION_ABORTABLE, $expected->getCode());
        }

        $manager->abortTransaction();

        self::assertTrue($manager->isReady(), 'and the abort leaves the producer ready for the next transaction');
        self::assertSame($transactionalId, $manager->getTransactionalId(), 'with the same transactional id');

        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$this->topic => [self::VERIFIED_PARTITION => []]]);
        $accepted = $this->client->produce(
            [$this->topic => [self::VERIFIED_PARTITION => [new Record('t2-38 after the abort')]]],
            $manager
        );
        $manager->commitTransaction();

        // The offset 1, not 0: the partition was part of the transaction that was rolled back, so the ABORT
        // control batch the coordinator wrote into it took the offset 0 - a `read_committed` consumer sees
        // neither, a `read_uncommitted` one sees the record alone
        self::assertSame(
            1,
            $accepted[$this->topic][self::VERIFIED_PARTITION]->baseOffset,
            'the next transaction of the same producer commits, behind the marker of the aborted one'
        );
    }

    public function testAProducerOfTheProtocolV2EnrolsThePartitionWithItsProduceVersionTwelve(): void
    {
        // The other side of the version choice: on this node the transaction manager speaks the protocol v2, so
        // `Client::produce()` sends the transactional batch as Produce v12, which enrols the partition itself - the
        // very batch that is refused the 120 at version 11 above is appended and committed
        $transactionalId = self::uniqueTransactionalId('t2-40-v2-producer');
        $manager         = new TransactionManager($this->client, $transactionalId, 60000, $this->configuration());

        $manager->initTransactions();
        self::assertTrue($manager->isTransactionV2Enabled(), 'the node finalizes transaction.version 2');

        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$this->topic => [self::UNVERIFIED_PARTITION => []]]);
        $accepted = $this->client->produce(
            [$this->topic => [self::UNVERIFIED_PARTITION => [new Record('t2-40 enrolled by the produce')]]],
            $manager
        );
        $manager->commitTransaction();

        self::assertSame(0, $accepted[$this->topic][self::UNVERIFIED_PARTITION]->baseOffset);
        self::assertTrue($manager->isReady());
    }

    /**
     * A transaction manager held on the transaction protocol v1 even on a node that finalizes `transaction.version` 2
     */
    private function transactionManagerOfTheProtocolV1(string $transactionalId): TransactionManager
    {
        return new class ($this->client, $transactionalId, $this->configuration()) extends TransactionManager {
            /**
             * @param array<string, mixed> $configuration
             */
            public function __construct(Client $client, string $transactionalId, array $configuration)
            {
                parent::__construct($client, $transactionalId, 60000, $configuration);
            }

            protected function canEnrolPartitionsByProduce(): bool
            {
                return false;
            }
        };
    }

    /**
     * Opens a transaction whose only verified partition is {@see self::VERIFIED_PARTITION}
     */
    private function openTransactionOnTheVerifiedPartition(string $transactionalId): ProducerIdAndEpoch
    {
        $coordinator = $this->client->getTransactionCoordinator($transactionalId);
        $idAndEpoch  = $this->client->initProducerId($transactionalId, 60000);

        $this->client->addPartitionsToTxn(
            $coordinator,
            $transactionalId,
            $idAndEpoch,
            [$this->topic => [self::VERIFIED_PARTITION]]
        );

        return $idAndEpoch;
    }

    /**
     * Rolls the open transaction back, so that the next test of the shared node does not meet a 51
     */
    private function abortQuietly(string $transactionalId, ProducerIdAndEpoch $idAndEpoch): void
    {
        try {
            $this->client->endTxn(
                $this->client->getTransactionCoordinator($transactionalId),
                $transactionalId,
                $idAndEpoch,
                false
            );
        } catch (KafkaException) {
            // The coordinator expires an abandoned transaction by itself; a failed abort must not fail the test
        }
    }

    /**
     * Builds a plain, non-transactional produce request of this test class in the given version
     *
     * @param class-string<ProduceRequest> $requestClass
     */
    private function produceRequest(
        string $requestClass,
        int $correlationId,
        int $partition = self::VERIFIED_PARTITION
    ): ProduceRequest {
        return new $requestClass(
            [$this->topic => [$partition => RecordBatch::fromRecords([
                new Record('t2-38 one')->withCreateTime(1700000000000),
                new Record('t2-38 two')->withCreateTime(1700000001000),
            ])]],
            1,
            self::REQUEST_TIMEOUT_MS,
            self::CLIENT_ID,
            $correlationId
        );
    }

    /**
     * Builds the transactional produce request of this test class in the given version
     *
     * @param class-string<ProduceRequest> $requestClass
     */
    private function transactionalRequest(
        string $requestClass,
        int $correlationId,
        string $transactionalId,
        ProducerIdAndEpoch $idAndEpoch,
        int $partition = self::UNVERIFIED_PARTITION
    ): ProduceRequest {
        $batch = RecordBatch::fromRecords(
            [new Record('t2-38 unverified')->withCreateTime(1700000000000)],
            0,
            0,
            $idAndEpoch->producerId,
            $idAndEpoch->epoch,
            0,
            true
        );

        return new $requestClass(
            [$this->topic => [$partition => $batch]],
            -1,
            self::REQUEST_TIMEOUT_MS,
            self::CLIENT_ID,
            $correlationId,
            $transactionalId
        );
    }

    /**
     * Sends one request and reads the answer that belongs to it
     *
     * @template T of AbstractResponse
     *
     * @param class-string<T> $responseClass Class the answer is decoded with
     *
     * @return T
     */
    private function send(AbstractRequest $request, string $responseClass): AbstractResponse
    {
        $stream = $this->connect();

        try {
            $request->writeTo($stream);

            return $responseClass::unpack($stream);
        } finally {
            $stream->disconnect();
        }
    }

    /**
     * Waits until the fresh partitions have a leader that answers
     */
    private function awaitTopic(string $topic): void
    {
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            try {
                $this->cluster()->reload();
                $this->admin->listOffsets(
                    [$topic => [self::VERIFIED_PARTITION, self::UNVERIFIED_PARTITION]],
                    OffsetsRequest::LATEST
                );

                return;
            } catch (KafkaException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
            }
        } while (true);
    }

    /**
     * A transactional id no other run and no other suite of the shared node uses
     */
    private static function uniqueTransactionalId(string $prefix): string
    {
        return $prefix . '-' . bin2hex(random_bytes(6));
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
            ProducerConfig::TIMEOUT_MS              => self::REQUEST_TIMEOUT_MS,

            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
        ] + ConsumerConfig::getDefaultConfiguration();
    }
}
