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

use Closure;
use PHPUnit\Framework\Attributes\CoversClass;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\TransactionAbortableException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\Internals\TransactionState;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\EndTxnRequest;
use Protocol\Kafka\Protocol\Request\EndTxnResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequestV4;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponseV4;

/**
 * The transaction protocol v2 of KIP-890 part 2 (Kafka 4.0) on the 4.3.1 node, which finalizes it.
 *
 * `transaction.version` 2 is the default of a 4.x cluster: the partitions and the offsets of a transaction are
 * enrolled by the Produce (v12+) and TxnOffsetCommit (v5) requests themselves, and every EndTxn (v5) bumps the
 * epoch of the producer and answers the pair the next transaction runs under. What is measured here:
 *
 * * the frames - a TxnOffsetCommit v4 without an AddOffsetsToTxn is the 120, the same commit at the version 5 is
 *   accepted and starts the transaction, and the state table of the EndTxn v5 (the bump, the retry with the epoch
 *   below, the 48 of an empty commit, the bump of an empty abort, the 90 of an older epoch);
 * * {@see TransactionManager} on that protocol, with the offsets of a group as the content of the transaction: no
 *   AddOffsetsToTxn, the epoch one higher after every end, an abortable error aborted without an InitProducerId,
 *   and a transaction that enrolled nothing not ended at all.
 *
 * The Produce half of the protocol - a partition enrolled by a Produce v12 - is the one piece whose version the
 * writing client chooses ({@see TransactionManager::isTransactionV2Enabled()}); the manager takes the protocol v2
 * only when its Produce request can enrol a partition, which the manager of this class is told it can, because
 * its transactions write offsets alone.
 *
 * Every transactional id is unique per run, and every topic and group this class creates is deleted again.
 *
 * @see docs/protocol/4.3.md, sections "The transaction protocol v2 on a node that finalizes it (Kafka 4.0)",
 *      "EndTxn API (key 26, v0 to v5)" and "TxnOffsetCommit API (key 28, v0 to v5)"
 */
#[CoversClass(Client::class)]
#[CoversClass(TransactionManager::class)]
#[CoversClass(EndTxnRequest::class)]
#[CoversClass(EndTxnResponse::class)]
#[CoversClass(TxnOffsetCommitRequest::class)]
#[CoversClass(TxnOffsetCommitRequestV4::class)]
#[CoversClass(TxnOffsetCommitResponse::class)]
#[CoversClass(TxnOffsetCommitResponseV4::class)]
final class TransactionProtocolV2Test extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t4-40';

    private const int TRANSACTION_ABORTABLE = 120;

    private const int PRODUCER_FENCED = 90;

    private const float TIMEOUT = 30.0;

    private Cluster $cluster;

    private AdminClient $admin;

    private Client $client;

    /**
     * @var list<string>
     */
    private array $createdTopics = [];

    /**
     * @var list<string>
     */
    private array $createdGroups = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
        $this->client  = new Client($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        if ($this->createdGroups !== []) {
            try {
                $this->admin->deleteConsumerGroups($this->createdGroups);
            } catch (KafkaException) {
                // a group whose transaction never committed an offset does not exist
            }
            $this->createdGroups = [];
        }
        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    public function testTheTransactionCoordinatorFinalizesTheLevelTwo(): void
    {
        $transactionalId = $this->transactionalId('feature');
        $answer          = $this->client->apiVersions($this->client->getTransactionCoordinator($transactionalId));

        self::assertGreaterThanOrEqual(0, $answer->finalizedFeaturesEpoch);
        self::assertArrayHasKey(TransactionManager::TRANSACTION_VERSION_FEATURE, $answer->finalizedFeatures);
        self::assertSame(
            TransactionManager::TRANSACTION_VERSION_2,
            $answer->finalizedFeatures[TransactionManager::TRANSACTION_VERSION_FEATURE]->maxVersionLevel
        );

        $manager = new TransactionManager($this->client, $transactionalId, 60000, $this->configuration());
        $manager->initTransactions();

        self::assertSame(
            ProduceRequest::VERSION > TransactionManager::LAST_PRODUCE_VERSION_BEFORE_TRANSACTION_V2,
            $manager->isTransactionV2Enabled(),
            'the protocol v2 is taken as soon as the Produce request of this client can enrol a partition'
        );
    }

    public function testTheVersionFiveOfTxnOffsetCommitEnrolsWhatTheVersionFourIsRefusedThe120For(): void
    {
        $topic           = $this->topic('offsets');
        $group           = $this->group('offsets');
        $transactionalId = $this->transactionalId('offsets');
        $idAndEpoch      = $this->client->initProducerId($transactionalId, 60000);

        $atVersionFour = $this->exchange(
            new TxnOffsetCommitRequestV4(
                $transactionalId,
                $group,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                [$topic => [0 => 7]],
                null,
                self::CLIENT_ID,
                4040
            ),
            TxnOffsetCommitResponseV4::class
        );

        self::assertSame(
            self::TRANSACTION_ABORTABLE,
            $atVersionFour->topics[$topic]->partitions[0]->errorCode,
            'the version 4 only verifies the offsets partition, and the transaction does not hold it'
        );

        $atVersionFive = $this->exchange(
            new TxnOffsetCommitRequest(
                $transactionalId,
                $group,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                [$topic => [0 => 7]],
                null,
                self::CLIENT_ID,
                4041
            ),
            TxnOffsetCommitResponse::class
        );

        self::assertSame(
            KafkaException::NO_ERROR,
            $atVersionFive->topics[$topic]->partitions[0]->errorCode,
            'the version 5 enrols it'
        );

        $next = $this->client->endTxnBumpingEpoch(
            $this->client->getTransactionCoordinator($transactionalId),
            $transactionalId,
            $idAndEpoch,
            EndTxnRequest::COMMIT
        );

        self::assertEquals(new ProducerIdAndEpoch($idAndEpoch->producerId, $idAndEpoch->epoch + 1), $next);
        self::assertSame(7, $this->awaitCommittedOffset($group, $topic), 'the transaction it started is committed');
    }

    public function testTheStateTableOfTheEndTxnVersionFive(): void
    {
        $topic           = $this->topic('end');
        $group           = $this->group('end');
        $transactionalId = $this->transactionalId('end');
        $first           = $this->client->initProducerId($transactionalId, 60000);
        $coordinator     = $this->client->getTransactionCoordinator($transactionalId);

        $this->client->txnOffsetCommit($coordinator, $transactionalId, $group, $first, [$topic => [0 => 3]], null, true);

        $bumped = $this->endTxn($transactionalId, $first, EndTxnRequest::COMMIT, 4050);
        self::assertSame(KafkaException::NO_ERROR, $bumped->errorCode);
        self::assertSame($first->producerId, $bumped->producerId);
        self::assertSame($first->epoch + 1, $bumped->producerEpoch, 'every end of a transaction bumps the epoch');

        $retry = $this->endTxnOnceTheMarkersAreWritten($transactionalId, $first, EndTxnRequest::COMMIT, 4051);
        self::assertSame(KafkaException::NO_ERROR, $retry->errorCode, 'the epoch below the current one is a retry');
        self::assertSame($first->epoch + 1, $retry->producerEpoch, 'answered with the pair the original got');

        $second = new ProducerIdAndEpoch($bumped->producerId, $bumped->producerEpoch);

        $empty = $this->endTxn($transactionalId, $second, EndTxnRequest::COMMIT, 4052);
        self::assertSame(KafkaException::INVALID_TXN_STATE, $empty->errorCode, 'a commit of nothing');
        self::assertFalse($empty->hasProducerIdAndEpoch(), 'an error carries the defaults -1/-1');

        $emptyAbort = $this->endTxn($transactionalId, $second, EndTxnRequest::ABORT, 4053);
        self::assertSame(KafkaException::NO_ERROR, $emptyAbort->errorCode, 'an abort of nothing is accepted');
        self::assertSame($second->epoch + 1, $emptyAbort->producerEpoch, 'and bumps the epoch all the same');

        $fenced = $this->endTxnOnceTheMarkersAreWritten($transactionalId, $first, EndTxnRequest::ABORT, 4054);
        self::assertSame(self::PRODUCER_FENCED, $fenced->errorCode, 'two epochs behind is a fenced producer');
        self::assertSame(-1, $fenced->producerId);
    }

    public function testATransactionOfTheManagerRunsWithoutAddOffsetsToTxnAndBumpsTheEpoch(): void
    {
        $topic   = $this->topic('manager');
        $group   = $this->group('manager');
        $manager = $this->manager($this->transactionalId('manager'));

        $manager->initTransactions();
        self::assertTrue($manager->isTransactionV2Enabled());
        $initialized = $manager->getProducerIdAndEpoch();

        $manager->beginTransaction();
        $manager->sendOffsetsToTransaction([$topic => [0 => 11]], $group);
        $manager->commitTransaction();

        self::assertSame($initialized->producerId, $manager->getProducerIdAndEpoch()->producerId);
        self::assertSame($initialized->epoch + 1, $manager->getProducerIdAndEpoch()->epoch);
        self::assertSame(11, $this->awaitCommittedOffset($group, $topic));

        // The next transaction runs under the bumped epoch, which the coordinator accepts as the current one
        $manager->beginTransaction();
        $manager->sendOffsetsToTransaction([$topic => [0 => 12]], $group);
        $manager->commitTransaction();

        self::assertSame($initialized->epoch + 2, $manager->getProducerIdAndEpoch()->epoch);
        self::assertSame(12, $this->awaitCommittedOffset($group, $topic, 12));
    }

    public function testAnAbortableErrorIsAbortedByTheEndTxnAloneAndTheProducerGoesOn(): void
    {
        $topic   = $this->topic('abortable');
        $group   = $this->group('abortable');
        $manager = $this->manager($this->transactionalId('abortable'));

        $manager->initTransactions();
        $initialized = $manager->getProducerIdAndEpoch();

        $manager->beginTransaction();
        $manager->sendOffsetsToTransaction([$topic => [0 => 21]], $group);
        $manager->transitionToAbortableError(new NetworkException(['test' => 'an abortable error']));
        $manager->abortTransaction();

        self::assertSame(TransactionState::READY, $manager->currentState());
        self::assertEquals(
            new ProducerIdAndEpoch($initialized->producerId, $initialized->epoch + 1),
            $manager->getProducerIdAndEpoch(),
            'the EndTxn v5 of the abort bumped the epoch - one, not the two an InitProducerId would add'
        );

        $manager->beginTransaction();
        $manager->sendOffsetsToTransaction([$topic => [0 => 22]], $group);
        $manager->commitTransaction();

        self::assertSame(22, $this->awaitCommittedOffset($group, $topic), 'the aborted 21 was never committed');
    }

    public function testTheCodeTransactionAbortableOfTheProtocolV1LeavesTheProducerAbortable(): void
    {
        $topic           = $this->topic('abortable-v1');
        $group           = $this->group('abortable-v1');
        $transactionalId = $this->transactionalId('abortable-v1');
        $idAndEpoch      = $this->client->initProducerId($transactionalId, 60000);

        try {
            $this->client->txnOffsetCommit(
                $this->client->getGroupCoordinator($group),
                $transactionalId,
                $group,
                $idAndEpoch,
                [$topic => [0 => 31]]
            );
            self::fail('a TxnOffsetCommit v4 without an AddOffsetsToTxn in front of it is refused');
        } catch (TransactionAbortableException $refused) {
            self::assertSame(self::TRANSACTION_ABORTABLE, $refused->getCode());
        }
    }

    public function testATransactionThatEnrolledNothingIsNotEndedAtTheCoordinator(): void
    {
        $topic   = $this->topic('empty');
        $group   = $this->group('empty');
        $manager = $this->manager($this->transactionalId('empty'));

        $manager->initTransactions();
        $initialized = $manager->getProducerIdAndEpoch();

        $manager->beginTransaction();
        $manager->commitTransaction();
        $manager->beginTransaction();
        $manager->abortTransaction();

        self::assertEquals($initialized, $manager->getProducerIdAndEpoch(), 'no EndTxn, no bump');

        $manager->beginTransaction();
        $manager->sendOffsetsToTransaction([$topic => [0 => 41]], $group);
        $manager->commitTransaction();

        self::assertSame(41, $this->awaitCommittedOffset($group, $topic));
    }

    /**
     * A manager of the protocol v2 whose transactions hold offsets alone, so no Produce request is involved
     */
    private function manager(string $transactionalId): TransactionManager
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
                return true;
            }
        };
    }

    private function endTxn(
        string $transactionalId,
        ProducerIdAndEpoch $idAndEpoch,
        bool $result,
        int $correlationId
    ): EndTxnResponse {
        return $this->exchange(
            new EndTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                $result,
                self::CLIENT_ID,
                $correlationId
            ),
            EndTxnResponse::class
        );
    }

    /**
     * Repeats an EndTxn v5 while the coordinator answers the 51 of the markers of the previous end
     */
    private function endTxnOnceTheMarkersAreWritten(
        string $transactionalId,
        ProducerIdAndEpoch $idAndEpoch,
        bool $result,
        int $correlationId
    ): EndTxnResponse {
        $deadline = microtime(true) + self::TIMEOUT;
        do {
            $answer = $this->endTxn($transactionalId, $idAndEpoch, $result, $correlationId++);
            if ($answer->errorCode !== KafkaException::CONCURRENT_TRANSACTIONS) {
                return $answer;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        return $answer;
    }

    /**
     * Sends one frame to the one node on a fresh connection and decodes the answer
     *
     * @template T of object
     *
     * @param class-string<T> $responseClass
     *
     * @return T
     */
    private function exchange(AbstractRequest $request, string $responseClass): object
    {
        $stream = $this->connect();
        $request->writeTo($stream);

        return $responseClass::unpack($stream);
    }

    /**
     * Waits until an OffsetFetch of the group answers the offset of a committed transaction
     */
    private function awaitCommittedOffset(string $group, string $topic, ?int $expected = null): int
    {
        $coordinator = $this->client->getGroupCoordinator($group);
        $deadline    = microtime(true) + self::TIMEOUT;
        do {
            $committed = $this->committedOffset($coordinator, $group, $topic);
            if ($committed !== -1 && ($expected === null || $committed === $expected)) {
                return $committed;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The committed offset of {$group} did not become visible within " . self::TIMEOUT . ' seconds');
    }

    private function committedOffset(Node $coordinator, string $group, string $topic): int
    {
        return $this->retrying(
            fn(): int => $this->client->fetchGroupOffsets($coordinator, $group, [$topic => [0]])[$topic][0]
        );
    }

    /**
     * Repeats a read while `__consumer_offsets` is still loading on the shared node (14, 15, 16)
     *
     * @template T
     *
     * @param Closure(): T $read
     *
     * @return T
     */
    private function retrying(Closure $read): mixed
    {
        $deadline = microtime(true) + self::TIMEOUT;
        do {
            try {
                return $read();
            } catch (KafkaException $error) {
                if (microtime(true) >= $deadline) {
                    throw $error;
                }
                usleep(200000);
            }
        } while (true);
    }

    private function topic(string $purpose): string
    {
        $topic                 = self::uniqueTopicName("t4-40-v2-{$purpose}");
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

    private function group(string $purpose): string
    {
        $group                 = 't4-40-v2-' . $purpose . '-group-' . bin2hex(random_bytes(6));
        $this->createdGroups[] = $group;

        return $group;
    }

    /**
     * A transactional id no other run uses: the coordinator forgets one only after `transactional.id.expiration.ms`
     */
    private function transactionalId(string $purpose): string
    {
        return 't4-40-v2-' . $purpose . '-txn-' . bin2hex(random_bytes(6));
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
