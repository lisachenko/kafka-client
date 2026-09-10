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
use Protocol\Kafka\Common\Errors\DuplicateSequenceNumberException;
use Protocol\Kafka\Common\Errors\NotLeaderForPartitionException;
use Protocol\Kafka\Common\Errors\OutOfOrderSequenceException;
use Protocol\Kafka\Common\Errors\ProducerFencedException;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\ClusterFixture;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\FakeClient;

/**
 * The bookkeeping of the idempotent producer: one producer id, one sequence per topic-partition, and the three
 * error codes of KIP-98.
 *
 * @see docs/protocol/1.1.md, section "The idempotent producer"
 */
#[CoversClass(TransactionManager::class)]
#[CoversClass(ProducerIdAndEpoch::class)]
final class TransactionManagerTest extends TestCase
{
    private const string TOPIC = 'orders';

    public function testAProducerWithoutATransactionalIdIsIdempotentOnly(): void
    {
        $manager = $this->manager();

        self::assertFalse($manager->isTransactional());
        self::assertNull($manager->getTransactionalId());
        self::assertFalse($manager->hasProducerId());
        self::assertFalse($manager->getProducerIdAndEpoch()->isValid());
    }

    public function testTheProducerIdIsAskedForOnceAndKept(): void
    {
        $client  = $this->client([new ProducerIdAndEpoch(2000, 0), new ProducerIdAndEpoch(2001, 0)]);
        $manager = new TransactionManager($client);

        $first  = $manager->maybeInitProducerId();
        $second = $manager->maybeInitProducerId();

        self::assertSame(2000, $first->producerId);
        self::assertSame(0, $first->epoch);
        self::assertSame($first, $second, 'The producer id is only asked for once');
        self::assertCount(1, $client->initProducerIdCalls);
        self::assertSame(
            ['transactionalId' => null, 'transactionTimeoutMs' => 60000],
            $client->initProducerIdCalls[0]
        );
        self::assertTrue($manager->hasProducerId());
    }

    public function testATransactionalIdAndItsTimeoutTravelIntoTheRequest(): void
    {
        $client  = $this->client([new ProducerIdAndEpoch(7, 3)]);
        $manager = new TransactionManager($client, 'tx-1', 30000);

        self::assertTrue($manager->isTransactional());
        self::assertSame('tx-1', $manager->getTransactionalId());
        self::assertSame(30000, $manager->getTransactionTimeoutMs());

        $manager->maybeInitProducerId();

        self::assertSame(
            ['transactionalId' => 'tx-1', 'transactionTimeoutMs' => 30000],
            $client->initProducerIdCalls[0]
        );
    }

    public function testTheSequenceOfEveryTopicPartitionStartsAtZeroAndCountsRecords(): void
    {
        $manager   = $this->manager();
        $partition = new TopicPartition(self::TOPIC, 0);
        $other     = new TopicPartition(self::TOPIC, 1);

        self::assertSame(0, $manager->sequenceNumber($partition));
        self::assertSame(0, $manager->sequenceNumber($other));

        $manager->incrementSequenceNumber($partition, 3);

        self::assertSame(3, $manager->sequenceNumber($partition));
        self::assertSame(0, $manager->sequenceNumber($other), 'The sequence space is per topic-partition');
    }

    public function testTheSequenceWrapsAroundTheLargestInt32LikeARecordBatchDoes(): void
    {
        $manager   = $this->manager();
        $partition = new TopicPartition(self::TOPIC, 0);

        $manager->incrementSequenceNumber($partition, TransactionManager::MAX_SEQUENCE);
        self::assertSame(TransactionManager::MAX_SEQUENCE, $manager->sequenceNumber($partition));

        // The last sequence of a one-record batch that starts at MAX_SEQUENCE is MAX_SEQUENCE, so the next base
        // sequence is the 0 that a record batch computes for the very same wrap-around
        $manager->incrementSequenceNumber($partition, 1);
        self::assertSame(0, $manager->sequenceNumber($partition));
    }

    public function testTheBaseSequencesOfAWholeBatchAreReportedByTopicAndPartition(): void
    {
        $manager = $this->manager();
        $manager->incrementSequenceNumber(new TopicPartition(self::TOPIC, 1), 5);

        self::assertSame(
            [self::TOPIC => [0 => 0, 1 => 5], 'other' => [3 => 0]],
            $manager->baseSequences([self::TOPIC => [0 => 'x', 1 => 'y'], 'other' => [3 => 'z']])
        );
    }

    public function testAnAcknowledgedBatchMovesTheSequenceOn(): void
    {
        $manager    = $this->manager();
        $idAndEpoch = $manager->maybeInitProducerId();
        $partition  = new TopicPartition(self::TOPIC, 0);

        $manager->batchCompleted($partition, 2, $idAndEpoch);

        self::assertSame(2, $manager->sequenceNumber($partition));
    }

    public function testTheAnswerOfABatchOfAProducerIdThatIsGoneIsIgnored(): void
    {
        $manager   = $this->manager();
        $partition = new TopicPartition(self::TOPIC, 0);
        $manager->maybeInitProducerId();

        $manager->batchCompleted($partition, 2, new ProducerIdAndEpoch(999, 0));

        self::assertSame(0, $manager->sequenceNumber($partition), 'A batch of an old producer id changes nothing');
    }

    public function testAnOutOfOrderSequenceThrowsTheProducerIdAway(): void
    {
        $client     = $this->client([new ProducerIdAndEpoch(2000, 0), new ProducerIdAndEpoch(2001, 0)]);
        $manager    = new TransactionManager($client);
        $idAndEpoch = $manager->maybeInitProducerId();
        $partition  = new TopicPartition(self::TOPIC, 0);
        $manager->batchCompleted($partition, 4, $idAndEpoch);

        $manager->batchFailed($partition, new OutOfOrderSequenceException(), 1, $idAndEpoch);

        self::assertFalse($manager->hasProducerId(), 'The producer id is gone');
        self::assertFalse($manager->hasFatalError(), 'An out of order sequence is not fatal for an idempotent producer');
        self::assertSame(0, $manager->sequenceNumber($partition), 'Every sequence starts over');
        self::assertSame(2001, $manager->maybeInitProducerId()->producerId, 'The next batch asks for a new id');
    }

    public function testATransactionalProducerKeepsItsProducerIdOnAnOutOfOrderSequence(): void
    {
        $manager    = new TransactionManager($this->client([new ProducerIdAndEpoch(2000, 1)]), 'tx-1');
        $idAndEpoch = $manager->maybeInitProducerId();
        $partition  = new TopicPartition(self::TOPIC, 0);

        $manager->batchFailed($partition, new OutOfOrderSequenceException(), 1, $idAndEpoch);

        self::assertTrue($manager->hasProducerId());
        self::assertSame(2000, $manager->getProducerIdAndEpoch()->producerId);
    }

    public function testResettingATransactionalProducerIsRefused(): void
    {
        $manager = new TransactionManager($this->client(), 'tx-1');
        $manager->maybeInitProducerId();

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Can not reset the producer state of a transactional producer');

        $manager->resetProducerId();
    }

    public function testAFencedProducerIsFinishedForGood(): void
    {
        $manager    = $this->manager();
        $idAndEpoch = $manager->maybeInitProducerId();
        $fenced     = new ProducerFencedException();

        $manager->batchFailed(new TopicPartition(self::TOPIC, 0), $fenced, 1, $idAndEpoch);

        self::assertTrue($manager->hasFatalError());
        self::assertSame($fenced, $manager->lastFatalError());

        $this->expectExceptionObject($fenced);

        $manager->maybeInitProducerId();
    }

    public function testADuplicateSequenceCountsAsAnAppend(): void
    {
        $manager    = $this->manager();
        $idAndEpoch = $manager->maybeInitProducerId();
        $partition  = new TopicPartition(self::TOPIC, 0);

        $manager->batchFailed($partition, new DuplicateSequenceNumberException(), 2, $idAndEpoch);

        self::assertSame(2, $manager->sequenceNumber($partition), 'The batch is in the log, its sequences are used');
        self::assertTrue($manager->hasProducerId());
        self::assertFalse($manager->hasFatalError());
    }

    public function testAnErrorThatSaysNothingAboutTheProducerStateLeavesItAlone(): void
    {
        $manager    = $this->manager();
        $idAndEpoch = $manager->maybeInitProducerId();
        $partition  = new TopicPartition(self::TOPIC, 0);
        $manager->batchCompleted($partition, 3, $idAndEpoch);

        $manager->batchFailed($partition, new NotLeaderForPartitionException(), 1, $idAndEpoch);

        self::assertTrue($manager->hasProducerId());
        self::assertFalse($manager->hasFatalError());
        self::assertSame(3, $manager->sequenceNumber($partition), 'The batch is sent again with the same sequence');
    }

    public function testTheFailureOfABatchOfAnOldProducerIdChangesNothing(): void
    {
        $manager = $this->manager();
        $manager->maybeInitProducerId();

        $manager->batchFailed(
            new TopicPartition(self::TOPIC, 0),
            new ProducerFencedException(),
            1,
            new ProducerIdAndEpoch(999, 0)
        );

        self::assertFalse($manager->hasFatalError());
    }

    public function testTheProducerIdAndEpochPairKnowsWhetherItNamesARealProducer(): void
    {
        $none = ProducerIdAndEpoch::none();

        self::assertFalse($none->isValid());
        self::assertSame(RecordBatch::NO_PRODUCER_ID, $none->producerId);
        self::assertSame(RecordBatch::NO_PRODUCER_EPOCH, $none->epoch);
        self::assertSame('(producerId=-1, epoch=-1)', (string) $none);

        $real = new ProducerIdAndEpoch(0, 0);

        self::assertTrue($real->isValid(), 'The producer id 0 is a valid one, only -1 is not');
        self::assertTrue($real->matches(0, 0));
        self::assertFalse($real->matches(0, 1));
    }

    /**
     * @param list<ProducerIdAndEpoch> $producerIds
     */
    private function manager(array $producerIds = []): TransactionManager
    {
        return new TransactionManager($this->client($producerIds));
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
