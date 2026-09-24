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

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\TransactionAbortableException;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\ConsumerGroupMetadata;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\Internals\TransactionState;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\ClusterFixture;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\FakeClient;

/**
 * The transaction protocol v2 of KIP-890 part 2 (Kafka 4.0) in {@see TransactionManager}.
 *
 * The protocol is decided by the finalized `transaction.version` the transaction coordinator reports in its
 * ApiVersions answer, as `TransactionManager.maybeUpdateTransactionV2Enabled()` @ 4.0.0 decides it: at the level 2
 * the partitions and the offsets of a transaction are enrolled by the Produce and TxnOffsetCommit requests
 * themselves and every EndTxn bumps the epoch; below it - and on a 3.x node, which finalizes no such feature - the
 * producer keeps the protocol v1 of every line below. The client under the manager is a double; the node's side of
 * the same story is `tests/Integration/TransactionProtocolV2Test.php`.
 *
 * @see docs/protocol/4.3.md, section "The transaction protocol v2 on a node that finalizes it (Kafka 4.0)"
 */
#[CoversClass(TransactionManager::class)]
final class TransactionProtocolV2Test extends TestCase
{
    private const string TOPIC = 'transactions';

    private const string TRANSACTIONAL_ID = 'tx-v2';

    private const string GROUP = 'group-v2';

    public function testANodeWithoutTheFeatureKeepsTheProtocolOfTheLinesBelow(): void
    {
        $client  = $this->client(null);
        $manager = $this->manager($client);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => []]]);
        $manager->sendOffsetsToTransaction([self::TOPIC => [0 => 5]], self::GROUP);
        $manager->commitTransaction();

        self::assertFalse($manager->isTransactionV2Enabled(), 'a 3.x node finalizes no transaction.version');
        self::assertSame(
            ['addPartitionsToTxn', 'addOffsetsToTxn', 'txnOffsetCommit', 'endTxn'],
            array_column($client->transactionCalls, 0)
        );
        self::assertCount(5, $client->transactionCalls[2], 'the TxnOffsetCommit of the protocol v1');
        self::assertSame(1000, $manager->getProducerIdAndEpoch()->producerId);
        self::assertSame(0, $manager->getProducerIdAndEpoch()->epoch, 'an EndTxn v4 leaves the epoch alone');
    }

    public function testTheLevelOneIsNotTheProtocolV2(): void
    {
        $manager = $this->manager($this->client(1));
        $manager->initTransactions();

        self::assertFalse($manager->isTransactionV2Enabled(), 'TV_1 is the flexible state records alone');
    }

    public function testAProducerWhoseProduceRequestCannotEnrolAPartitionKeepsTheProtocolV1(): void
    {
        $manager = $this->manager($this->client(2), false);
        $manager->initTransactions();

        self::assertFalse($manager->isTransactionV2Enabled());
    }

    public function testTheProduceRequestOfThisClientDecidesWhetherItCanEnrolAPartition(): void
    {
        $manager = new TransactionManager($this->client(2), self::TRANSACTIONAL_ID, 30000);
        $manager->initTransactions();

        self::assertSame(
            ProduceRequest::VERSION > TransactionManager::LAST_PRODUCE_VERSION_BEFORE_TRANSACTION_V2,
            $manager->isTransactionV2Enabled(),
            'the protocol v2 needs a Produce request of the version 12 or above'
        );
    }

    public function testAProducerWithoutATransactionIsNotOnTheProtocolV2(): void
    {
        $client  = $this->client(2);
        $manager = new TransactionManager($client);

        self::assertFalse($manager->isTransactionV2Enabled());
        self::assertSame(0, $client->apiVersionsCalls, 'an idempotent producer never asks');
    }

    public function testATransactionOfTheProtocolV2EnrolsNothingAndEndsWithTheEpochBump(): void
    {
        $client  = $this->client(2, [new ProducerIdAndEpoch(42, 7)]);
        $manager = $this->manager($client);

        $manager->initTransactions();
        self::assertTrue($manager->isTransactionV2Enabled());

        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => [], 1 => []]]);
        self::assertTrue($manager->isPartitionAdded(new TopicPartition(self::TOPIC, 1)), 'the Produce v12 enrols it');

        $manager->incrementSequenceNumber(new TopicPartition(self::TOPIC, 0), 3);
        $manager->sendOffsetsToTransaction([self::TOPIC => [0 => 5]], new ConsumerGroupMetadata(self::GROUP, 4, 'm-1'));
        $manager->commitTransaction();

        self::assertSame(
            ['txnOffsetCommit', 'endTxnBumpingEpoch'],
            array_column($client->transactionCalls, 0),
            'no AddPartitionsToTxn and no AddOffsetsToTxn in the protocol v2'
        );
        self::assertSame('v2', $client->transactionCalls[0][5], 'the offsets go out as a TxnOffsetCommit v5');
        self::assertSame(true, $client->transactionCalls[1][2], 'a commit');
        self::assertEquals(new ProducerIdAndEpoch(42, 7), $client->transactionCalls[1][3]);

        self::assertSame(42, $manager->getProducerIdAndEpoch()->producerId);
        self::assertSame(8, $manager->getProducerIdAndEpoch()->epoch, 'the next transaction runs under the new epoch');
        self::assertSame(
            0,
            $manager->sequenceNumber(new TopicPartition(self::TOPIC, 0)),
            'and numbers every partition from 0 again'
        );
        self::assertSame(TransactionState::READY, $manager->currentState());
        self::assertFalse($manager->isPartitionAdded(new TopicPartition(self::TOPIC, 1)));
        self::assertCount(1, $client->initProducerIdCalls, 'the bump needs no InitProducerId');
    }

    public function testAnExhaustedEpochComesBackAsANewProducerId(): void
    {
        $client                    = $this->client(2, [new ProducerIdAndEpoch(42, 32766)]);
        $client->bumpedProducerIds = [new ProducerIdAndEpoch(43, 0)];
        $manager                   = $this->manager($client);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => []]]);
        $manager->commitTransaction();

        self::assertEquals(new ProducerIdAndEpoch(43, 0), $manager->getProducerIdAndEpoch());
    }

    public function testAnAnswerWithoutAProducerIdLeavesTheProducerWhereItIs(): void
    {
        $client                    = $this->client(2, [new ProducerIdAndEpoch(42, 7)]);
        $client->bumpedProducerIds = [ProducerIdAndEpoch::none()];
        $manager                   = $this->manager($client);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => []]]);
        $manager->commitTransaction();

        self::assertEquals(new ProducerIdAndEpoch(42, 7), $manager->getProducerIdAndEpoch());
    }

    public function testATransactionThatEnrolledNothingIsNotEndedAtTheCoordinator(): void
    {
        $client  = $this->client(2, [new ProducerIdAndEpoch(42, 7)]);
        $manager = $this->manager($client);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->commitTransaction();
        $manager->beginTransaction();
        $manager->abortTransaction();

        self::assertSame([], $client->transactionCalls, 'the node answers an EndTxn of an empty transaction the 48');
        self::assertSame(7, $manager->getProducerIdAndEpoch()->epoch);
        self::assertSame(TransactionState::READY, $manager->currentState());
    }

    public function testAnAbortableErrorIsAbortedWithTheEpochBumpOfTheEndTxnAndNoInitProducerId(): void
    {
        $client  = $this->client(2, [new ProducerIdAndEpoch(42, 7)]);
        $manager = $this->manager($client);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => []]]);
        $manager->batchFailed(new TopicPartition(self::TOPIC, 0), new TransactionAbortableException());

        self::assertTrue($manager->hasAbortableError());

        $manager->abortTransaction();

        self::assertSame(['endTxnBumpingEpoch'], array_column($client->transactionCalls, 0));
        self::assertSame(false, $client->transactionCalls[0][2], 'an abort');
        self::assertCount(1, $client->initProducerIdCalls, 'KIP-360 has nothing left to do');
        self::assertEquals(new ProducerIdAndEpoch(42, 8), $manager->getProducerIdAndEpoch());
        self::assertSame(TransactionState::READY, $manager->currentState());
    }

    public function testAnOffsetCommitOfTheProtocolV2StartsTheTransactionEvenWhenItFails(): void
    {
        $client                                       = $this->client(2, [new ProducerIdAndEpoch(42, 7)]);
        $client->transactionErrors['txnOffsetCommit'] = [new TransactionAbortableException()];
        $manager                                      = $this->manager($client);

        $manager->initTransactions();
        $manager->beginTransaction();

        try {
            $manager->sendOffsetsToTransaction([self::TOPIC => [0 => 5]], self::GROUP);
            self::fail('the 120 of the group coordinator is reported');
        } catch (TransactionAbortableException) {
            self::assertTrue($manager->hasAbortableError());
        }

        $manager->abortTransaction();

        self::assertSame(['txnOffsetCommit', 'endTxnBumpingEpoch'], array_column($client->transactionCalls, 0));
    }

    public function testAClusterThatMovesUpToTheLevelTwoEndsTheTransactionWithV4AndBumpsTheEpochOnce(): void
    {
        $client  = $this->client(0, [new ProducerIdAndEpoch(42, 7), new ProducerIdAndEpoch(42, 8)]);
        $manager = $this->manager($client);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => []]]);

        $client->transactionVersion = 2;
        $manager->commitTransaction();

        self::assertSame(['addPartitionsToTxn', 'endTxn'], array_column($client->transactionCalls, 0));
        self::assertTrue($manager->isTransactionV2Enabled(), 'the next transaction speaks the protocol v2');
        self::assertCount(2, $client->initProducerIdCalls, '"to fence the old V1 transaction epoch"');
        self::assertSame(42, $client->initProducerIdCalls[1]['producerId']);
        self::assertSame(7, $client->initProducerIdCalls[1]['producerEpoch']);
        self::assertEquals(new ProducerIdAndEpoch(42, 8), $manager->getProducerIdAndEpoch());

        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => []]]);
        $manager->commitTransaction();

        self::assertSame(
            ['addPartitionsToTxn', 'endTxn', 'endTxnBumpingEpoch'],
            array_column($client->transactionCalls, 0)
        );
        self::assertSame(9, $manager->getProducerIdAndEpoch()->epoch);
    }

    public function testAClusterThatMovesDownEndsTheTransactionWithV5AndGoesBackToTheProtocolV1(): void
    {
        $client  = $this->client(2, [new ProducerIdAndEpoch(42, 7)]);
        $manager = $this->manager($client);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => []]]);

        $client->transactionVersion = 1;
        $manager->commitTransaction();

        self::assertSame(['endTxnBumpingEpoch'], array_column($client->transactionCalls, 0));
        self::assertFalse($manager->isTransactionV2Enabled());
        self::assertCount(1, $client->initProducerIdCalls, 'moving down fences nothing');

        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => []]]);
        $manager->commitTransaction();

        self::assertSame(
            ['endTxnBumpingEpoch', 'addPartitionsToTxn', 'endTxn'],
            array_column($client->transactionCalls, 0)
        );
    }

    public function testAnApiVersionsAnswerThatDoesNotArriveLeavesTheProtocolWhereItIs(): void
    {
        $client  = $this->client(2, [new ProducerIdAndEpoch(42, 7)]);
        $manager = $this->manager($client);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([self::TOPIC => [0 => []]]);

        $client->apiVersionsErrors = [new NetworkException()];
        $manager->commitTransaction();

        self::assertTrue($manager->isTransactionV2Enabled());
        self::assertSame(['endTxnBumpingEpoch'], array_column($client->transactionCalls, 0));
    }

    public function testAnApiVersionsAnswerThatDoesNotArriveOnTheInitializationKeepsTheProtocolV1(): void
    {
        $client                    = $this->client(2);
        $client->apiVersionsErrors = [new NetworkException()];
        $manager                   = $this->manager($client);

        $manager->initTransactions();

        self::assertSame(TransactionState::READY, $manager->currentState());
        self::assertFalse($manager->isTransactionV2Enabled());
    }

    public function testAPeerThatRefusesTheApiVersionsRequestKeepsTheProtocolV1(): void
    {
        // A peer older than Kafka 3.9 answers the ApiVersions v4 with the 35
        $client                       = $this->client(2);
        $client->apiVersionsErrorCode = 35;
        $manager                      = $this->manager($client);

        $manager->initTransactions();

        self::assertFalse($manager->isTransactionV2Enabled());
    }

    /**
     * A manager whose client writes Produce v12, unless told otherwise
     */
    private function manager(FakeClient $client, bool $canEnrolPartitionsByProduce = true): TransactionManager
    {
        return new class ($client, $canEnrolPartitionsByProduce) extends TransactionManager {
            public function __construct(FakeClient $client, private readonly bool $enrols)
            {
                parent::__construct($client, 'tx-v2', 30000);
            }

            protected function canEnrolPartitionsByProduce(): bool
            {
                return $this->enrols;
            }
        };
    }

    /**
     * @param int|null                 $transactionVersion Finalized `transaction.version`, null for none at all
     * @param list<ProducerIdAndEpoch> $producerIds
     */
    private function client(?int $transactionVersion, array $producerIds = []): FakeClient
    {
        $client                     = new FakeClient(ClusterFixture::withPartitions([self::TOPIC => [0 => 1, 1 => 1]]));
        $client->transactionVersion = $transactionVersion;
        $client->producerIds        = $producerIds;

        return $client;
    }
}
