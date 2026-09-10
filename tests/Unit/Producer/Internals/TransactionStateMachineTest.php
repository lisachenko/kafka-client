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

namespace Protocol\Kafka\Tests\Unit\Producer\Internals;

use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\ConcurrentTransactionsException;
use Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\NotLeaderForPartitionException;
use Protocol\Kafka\Common\Errors\ProducerFencedException;
use Protocol\Kafka\Common\Errors\TopicAuthorizationFailedException;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\Internals\TransactionState;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\ClusterFixture;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\FakeClient;

/**
 * The transactional half of {@see TransactionManager}: the state machine of KIP-98 and the four requests it sends.
 *
 * The client below the manager is a double, so what is checked here is the *order and the shape* of the requests
 * and what each answer does to the state; that they are the frames a broker accepts is checked by
 * `tests/Unit/Protocol/Request/TransactionApiTest.php` and by the wire vectors, and that a broker really behaves
 * this way by `tests/Integration/TransactionalProducerTest.php`.
 *
 * @see docs/protocol/1.1.md, section "Transactions"
 */
#[CoversClass(TransactionManager::class)]
#[CoversClass(TransactionState::class)]
final class TransactionStateMachineTest extends TestCase
{
    private const string TOPIC = 'transactions';

    private const string TRANSACTIONAL_ID = 'tx-1';

    public function testAnUninitializedProducerRefusesEveryTransactionalCall(): void
    {
        $manager = $this->manager();

        self::assertSame(TransactionState::UNINITIALIZED, $manager->currentState());

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Invalid transition attempted from the state UNINITIALIZED');

        $manager->beginTransaction();
    }

    public function testTheTransactionalMethodsAreRefusedOnAnIdempotentProducer(): void
    {
        $manager = new TransactionManager(new FakeClient(ClusterFixture::withPartitions([self::TOPIC => [0 => 1]])));

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Transactional method invoked on a producer without a transactional.id');

        $manager->initTransactions();
    }

    public function testInitTransactionsAsksTheTransactionCoordinatorForTheProducerId(): void
    {
        $client  = $this->client([new ProducerIdAndEpoch(42, 3)]);
        $manager = new TransactionManager($client, self::TRANSACTIONAL_ID, 30000);

        $manager->initTransactions();

        self::assertSame(TransactionState::READY, $manager->currentState());
        self::assertTrue($manager->isReady());
        self::assertSame(42, $manager->getProducerIdAndEpoch()->producerId);
        self::assertSame(3, $manager->getProducerIdAndEpoch()->epoch);
        self::assertSame(
            [['transactionalId' => self::TRANSACTIONAL_ID, 'transactionTimeoutMs' => 30000]],
            $client->initProducerIdCalls
        );
    }

    public function testInitTransactionsIsRefusedASecondTime(): void
    {
        $manager = $this->manager();
        $manager->initTransactions();

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Invalid transition attempted from the state READY to the state INITIALIZING');

        $manager->initTransactions();
    }

    public function testConcurrentTransactionsOnTheInitializationIsRetried(): void
    {
        $client                       = $this->client([new ProducerIdAndEpoch(42, 3)]);
        $client->initProducerIdErrors = [new ConcurrentTransactionsException(), new ConcurrentTransactionsException()];
        $manager                      = $this->manager($client);

        // The coordinator answers 51 while it is aborting whatever the previous incarnation of the id left open
        $manager->initTransactions();

        self::assertCount(3, $client->initProducerIdCalls, 'the request is repeated until the coordinator is ready');
        self::assertSame(42, $manager->getProducerIdAndEpoch()->producerId);
    }

    public function testACoordinatorErrorMakesTheCoordinatorBeLookedUpAgain(): void
    {
        $client                                          = $this->client();
        $client->transactionErrors['addPartitionsToTxn'] = [new NotCoordinatorForGroupException()];
        $manager                                         = $this->manager($client);
        $manager->initTransactions();
        $manager->beginTransaction();

        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => ['a record']]]);

        self::assertSame(
            [['transaction', self::TRANSACTIONAL_ID], ['transaction', self::TRANSACTIONAL_ID]],
            $client->coordinatorLookups,
            'the cached coordinator is thrown away and looked up again'
        );
    }

    public function testAPartitionIsEnrolledOnceAndOnlyOnce(): void
    {
        $client  = $this->client();
        $manager = $this->manager($client);
        $manager->initTransactions();
        $manager->beginTransaction();

        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => ['one'], 1 => ['two']]]);
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => ['three']]]);

        self::assertSame(
            [['addPartitionsToTxn', self::TRANSACTIONAL_ID, [self::TOPIC => [0, 1]]]],
            $client->transactionCalls,
            'the second flush touches no partition the coordinator does not know yet'
        );
        self::assertTrue($manager->isPartitionAdded(new TopicPartition(self::TOPIC, 0)));
        self::assertTrue($manager->isPartitionAdded(new TopicPartition(self::TOPIC, 1)));
    }

    public function testSendOffsetsToTransactionSendsBothHalvesToTheirOwnCoordinator(): void
    {
        $client  = $this->client();
        $manager = $this->manager($client);
        $manager->initTransactions();
        $manager->beginTransaction();

        $offsets = [self::TOPIC => [0 => new OffsetAndMetadata(17)]];

        $manager->sendOffsetsToTransaction($offsets, 'my-group');

        self::assertSame(
            [
                ['addOffsetsToTxn', self::TRANSACTIONAL_ID, 'my-group'],
                ['txnOffsetCommit', self::TRANSACTIONAL_ID, 'my-group', $offsets],
            ],
            $client->transactionCalls
        );
        self::assertSame(
            [['transaction', self::TRANSACTIONAL_ID], ['group', 'my-group']],
            $client->coordinatorLookups,
            'the offsets go to the group coordinator, everything else to the transaction coordinator'
        );
    }

    public function testSendingOffsetsOutsideATransactionIsRefused(): void
    {
        $manager = $this->manager();
        $manager->initTransactions();

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Can not send offsets to a transaction: the producer is in the state READY');

        $manager->sendOffsetsToTransaction([self::TOPIC => [0 => 17]], 'my-group');
    }

    public function testAnEmptyOffsetMapSendsNothingAtAll(): void
    {
        $client  = $this->client();
        $manager = $this->manager($client);
        $manager->initTransactions();
        $manager->beginTransaction();

        $manager->sendOffsetsToTransaction([], 'my-group');

        self::assertSame([], $client->transactionCalls);
    }

    public function testACommitEndsTheTransactionAndForgetsItsPartitions(): void
    {
        $client  = $this->client();
        $manager = $this->manager($client);
        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => ['one']]]);

        $manager->commitTransaction();

        self::assertSame(TransactionState::READY, $manager->currentState());
        self::assertSame(['endTxn', self::TRANSACTIONAL_ID, true], $client->transactionCalls[1]);
        self::assertFalse(
            $manager->isPartitionAdded(new TopicPartition(self::TOPIC, 0)),
            'the next transaction enrols its partitions again'
        );

        // ... and the same producer may open the next transaction right away
        $manager->beginTransaction();
        self::assertTrue($manager->isInTransaction());
    }

    public function testAnAbortEndsTheTransactionWithTheOtherResult(): void
    {
        $client  = $this->client();
        $manager = $this->manager($client);
        $manager->initTransactions();
        $manager->beginTransaction();

        $manager->abortTransaction();

        self::assertSame([['endTxn', self::TRANSACTIONAL_ID, false]], $client->transactionCalls);
        self::assertSame(TransactionState::READY, $manager->currentState());
    }

    public function testAFailedBatchMakesTheTransactionAbortable(): void
    {
        $client  = $this->client();
        $manager = $this->manager($client);
        $manager->initTransactions();
        $manager->beginTransaction();
        $error = new NotLeaderForPartitionException();

        // An error that says nothing about the producer state still ends the transaction: the batch may or may not
        // be in the log, so it can not be committed any more
        $manager->batchFailed(new TopicPartition(self::TOPIC, 0), $error, 1, $manager->getProducerIdAndEpoch());

        self::assertSame(TransactionState::ABORTABLE_ERROR, $manager->currentState());
        self::assertTrue($manager->hasAbortableError());
        self::assertSame($error, $manager->lastError());
        self::assertFalse($manager->hasFatalError(), 'an abortable error is not the end of the producer');
    }

    public function testOnlyAnAbortLeadsOutOfAnAbortableError(): void
    {
        $manager = $this->manager();
        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->transitionToAbortableError(new TopicAuthorizationFailedException());

        try {
            $manager->commitTransaction();
            self::fail('a transaction in an abortable state can not be committed');
        } catch (TopicAuthorizationFailedException) {
            // the error of the state is what every call but the abort reports
        }

        $manager->abortTransaction();

        self::assertSame(TransactionState::READY, $manager->currentState());
        self::assertNull($manager->lastError());
    }

    public function testAnAbortableErrorRefusesEverySend(): void
    {
        $manager = $this->manager();
        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->transitionToAbortableError(new TopicAuthorizationFailedException());

        $this->expectException(TopicAuthorizationFailedException::class);

        $manager->failIfNotReadyForSend();
    }

    public function testAFencedProducerIsFatalAndStaysFatal(): void
    {
        $manager = $this->manager();
        $manager->initTransactions();
        $manager->beginTransaction();
        $fenced = new ProducerFencedException();

        $manager->batchFailed(new TopicPartition(self::TOPIC, 0), $fenced, 1, $manager->getProducerIdAndEpoch());

        self::assertSame(TransactionState::FATAL_ERROR, $manager->currentState());
        self::assertTrue($manager->hasFatalError());

        // Not even an abort helps: there is nothing this producer may send any more
        $this->expectExceptionObject($fenced);

        $manager->abortTransaction();
    }

    public function testASendOutsideATransactionIsRefused(): void
    {
        $manager = $this->manager();
        $manager->initTransactions();

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Can not send in the state READY');

        $manager->failIfNotReadyForSend();
    }

    public function testASendBeforeTheInitializationIsRefused(): void
    {
        $manager = $this->manager();

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Can not send before initTransactions() was called');

        $manager->failIfNotReadyForSend();
    }

    public function testAnIdempotentProducerMaySendWhenever(): void
    {
        $manager = new TransactionManager(new FakeClient(ClusterFixture::withPartitions([self::TOPIC => [0 => 1]])));

        $manager->failIfNotReadyForSend();

        self::assertSame(TransactionState::UNINITIALIZED, $manager->currentState(), 'and stays out of the machine');
    }

    public function testTheCoordinatorRetriesAreBoundedByADeadline(): void
    {
        // `metadata.fetch.timeout.ms` of 0 makes the deadline of the coordinator retries expire right away, so the
        // `retries` of the policy are what is left of the budget
        $client                              = $this->client();
        $client->transactionErrors['endTxn'] = array_fill(0, 20, new GroupCoordinatorNotAvailableException());
        $manager                             = new TransactionManager($client, self::TRANSACTIONAL_ID, 60000, [
            ClientConfig::RETRIES                   => 3,
            ClientConfig::RETRY_BACKOFF_MS          => 0,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 0,
        ]);
        $manager->initTransactions();
        $manager->beginTransaction();

        try {
            $manager->commitTransaction();
            self::fail('a coordinator that never becomes available has to be reported');
        } catch (GroupCoordinatorNotAvailableException) {
            self::assertCount(4, array_filter(
                $client->transactionCalls,
                static fn(array $call): bool => $call[0] === 'endTxn'
            ), 'the first attempt and the `retries` after it');
        }

        self::assertSame(TransactionState::ABORTABLE_ERROR, $manager->currentState());
    }

    public function testACoordinatorThatBecomesAvailableIsWaitedFor(): void
    {
        // The three coordinator codes and the 51 say "ask again", not "the request failed", so their budget is the
        // deadline of `metadata.fetch.timeout.ms` and not the `retries` of a batch, which default to 0
        $client                              = $this->client();
        $client->transactionErrors['endTxn'] = array_fill(0, 5, new ConcurrentTransactionsException());
        $manager                             = new TransactionManager($client, self::TRANSACTIONAL_ID, 60000, [
            ClientConfig::RETRIES                   => 0,
            ClientConfig::RETRY_BACKOFF_MS          => 0,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 5000,
        ]);
        $manager->initTransactions();
        $manager->beginTransaction();

        $manager->commitTransaction();

        self::assertSame(TransactionState::READY, $manager->currentState());
    }

    /**
     * Every transition of `TransactionManager.State` @ 0.11.0.3, as the Java client declares them
     *
     * @return \Generator<string, array{0: TransactionState, 1: TransactionState, 2: bool}>
     */
    public static function transitions(): \Generator
    {
        yield 'uninitialized to initializing'  => [TransactionState::UNINITIALIZED, TransactionState::INITIALIZING, true];
        yield 'ready to initializing'          => [TransactionState::READY, TransactionState::INITIALIZING, false];
        yield 'initializing to ready'          => [TransactionState::INITIALIZING, TransactionState::READY, true];
        yield 'committing to ready'            => [TransactionState::COMMITTING_TRANSACTION, TransactionState::READY, true];
        yield 'aborting to ready'              => [TransactionState::ABORTING_TRANSACTION, TransactionState::READY, true];
        yield 'in transaction to ready'        => [TransactionState::IN_TRANSACTION, TransactionState::READY, false];
        yield 'ready to in transaction'        => [TransactionState::READY, TransactionState::IN_TRANSACTION, true];
        yield 'in transaction to committing'   => [TransactionState::IN_TRANSACTION, TransactionState::COMMITTING_TRANSACTION, true];
        yield 'in transaction to aborting'     => [TransactionState::IN_TRANSACTION, TransactionState::ABORTING_TRANSACTION, true];
        yield 'abortable error to aborting'    => [TransactionState::ABORTABLE_ERROR, TransactionState::ABORTING_TRANSACTION, true];
        yield 'abortable error to committing'  => [TransactionState::ABORTABLE_ERROR, TransactionState::COMMITTING_TRANSACTION, false];
        yield 'in transaction to abortable'    => [TransactionState::IN_TRANSACTION, TransactionState::ABORTABLE_ERROR, true];
        yield 'committing to abortable'        => [TransactionState::COMMITTING_TRANSACTION, TransactionState::ABORTABLE_ERROR, true];
        yield 'ready to abortable'             => [TransactionState::READY, TransactionState::ABORTABLE_ERROR, false];
        yield 'ready to fatal'                 => [TransactionState::READY, TransactionState::FATAL_ERROR, true];
        yield 'fatal to ready'                 => [TransactionState::FATAL_ERROR, TransactionState::READY, false];
        yield 'fatal to fatal'                 => [TransactionState::FATAL_ERROR, TransactionState::FATAL_ERROR, true];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('transitions')]
    public function testTheStateMachineIsTheOneOfTheJavaClient(
        TransactionState $from,
        TransactionState $to,
        bool $allowed
    ): void {
        self::assertSame($allowed, $from->canTransitionTo($to));
    }

    public function testOnlyTheTwoErrorStatesAreErrorStates(): void
    {
        self::assertTrue(TransactionState::ABORTABLE_ERROR->isError());
        self::assertTrue(TransactionState::FATAL_ERROR->isError());
        self::assertFalse(TransactionState::IN_TRANSACTION->isError());
        self::assertFalse(TransactionState::READY->isError());
    }

    private function manager(?FakeClient $client = null): TransactionManager
    {
        return new TransactionManager(
            $client ?? $this->client(),
            self::TRANSACTIONAL_ID,
            60000,
            [ClientConfig::RETRIES => 3, ClientConfig::RETRY_BACKOFF_MS => 0]
        );
    }

    /**
     * @param list<ProducerIdAndEpoch> $producerIds
     */
    private function client(array $producerIds = []): FakeClient
    {
        $client              = new FakeClient(ClusterFixture::withPartitions([self::TOPIC => [0 => 1, 1 => 1]]));
        $client->producerIds = $producerIds;

        return $client;
    }
}
