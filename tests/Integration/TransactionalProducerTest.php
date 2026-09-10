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
use Protocol\Kafka\Common\Errors\ProducerFencedException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Record\ControlRecordType;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\AbortedTransactionFilter;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\Internals\TransactionState;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequest;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponse;
use Protocol\Kafka\Protocol\Request\EndTxnRequest;
use Protocol\Kafka\Protocol\Request\EndTxnResponse;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * Exercises the transactional producer of KIP-98 against a real Kafka 0.11.0.3 broker.
 *
 * What can only be seen against a broker is checked here: that a committed transaction really becomes visible to a
 * `read_committed` reader and an aborted one never does, that the last stable offset of a partition stays behind
 * its high watermark while a transaction is open, that the coordinator writes the COMMIT and ABORT control batches
 * into the partitions, that a second producer of the same transactional id fences the first one, and what the
 * coordinator answers to the illegal state transitions of the api.
 *
 * @see docs/protocol/1.1.md, section "Transactions"
 */
#[CoversClass(Client::class)]
#[CoversClass(TransactionManager::class)]
#[CoversClass(TransactionState::class)]
#[CoversClass(KafkaProducer::class)]
#[CoversClass(AddPartitionsToTxnRequest::class)]
#[CoversClass(AddPartitionsToTxnResponse::class)]
#[CoversClass(EndTxnRequest::class)]
#[CoversClass(EndTxnResponse::class)]
final class TransactionalProducerTest extends IntegrationTestCase
{
    /**
     * How long to wait for a fresh topic to become servable, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * How long to wait for the control batches of a finished transaction to reach their partitions, in seconds
     */
    private const float MARKER_TIMEOUT = 30.0;

    /**
     * How long to wait for the coordinator to roll an expired transaction back, in seconds.
     *
     * `transaction.abort.timed.out.transaction.cleanup.interval.ms` is 60 seconds by default and the container
     * does not lower it, so this is the one test of the line that has to wait a whole minute; measured on the
     * 0.11.0.3 container, the rollback arrived after 57 seconds.
     */
    private const float EXPIRY_TIMEOUT = 120.0;

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

    public function testACommittedTransactionIsVisibleToBothIsolationLevels(): void
    {
        $topic   = $this->topic('commit');
        $manager = $this->manager($this->transactionalId('commit'));
        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);

        $this->client->produce([$topic => [0 => $this->records(['one', 'two'])]], $manager);
        $manager->commitTransaction();

        $this->awaitMarker($topic);

        self::assertSame(['one', 'two'], $this->read($topic, FetchRequest::READ_COMMITTED));
        self::assertSame(['one', 'two'], $this->read($topic, FetchRequest::READ_UNCOMMITTED));
        self::assertSame(
            ControlRecordType::COMMIT,
            $this->markerOf($topic),
            'the coordinator appended a COMMIT control batch to the partition'
        );
    }

    public function testAnAbortedTransactionIsVisibleToReadUncommittedOnly(): void
    {
        $topic   = $this->topic('abort');
        $manager = $this->manager($this->transactionalId('abort'));
        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);

        $this->client->produce([$topic => [0 => $this->records(['rolled', 'back'])]], $manager);
        $manager->abortTransaction();

        $this->awaitMarker($topic);

        self::assertSame([], $this->read($topic, FetchRequest::READ_COMMITTED), 'the records were rolled back');
        self::assertSame(
            ['rolled', 'back'],
            $this->read($topic, FetchRequest::READ_UNCOMMITTED),
            'they are still in the log, an ABORT marker behind them'
        );
        self::assertSame(ControlRecordType::ABORT, $this->markerOf($topic));

        $partition = $this->partitionOf($topic, FetchRequest::READ_COMMITTED);
        self::assertNotNull($partition->abortedTransactions);
        self::assertCount(1, $partition->abortedTransactions, 'and the answer names the transaction that was aborted');
        self::assertSame(0, $partition->abortedTransactions[0]->firstOffset);
    }

    public function testTheLastStableOffsetStaysBehindTheHighWatermarkWhileATransactionIsOpen(): void
    {
        $topic   = $this->topic('open');
        $manager = $this->manager($this->transactionalId('open'));
        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);

        $this->client->produce([$topic => [0 => $this->records(['not committed yet'])]], $manager);

        $open = $this->partitionOf($topic, FetchRequest::READ_COMMITTED);
        self::assertSame(1, $open->highWaterMarkOffset, 'the record is in the log and fully replicated');
        self::assertSame(0, $open->lastStableOffset, 'but nothing of the open transaction is stable yet');
        self::assertSame([], $this->read($topic, FetchRequest::READ_COMMITTED), 'so the answer is cut at the LSO');
        self::assertSame(['not committed yet'], $this->read($topic, FetchRequest::READ_UNCOMMITTED));

        $uncommitted = $this->partitionOf($topic, FetchRequest::READ_UNCOMMITTED);
        self::assertSame(-1, $uncommitted->lastStableOffset, 'a read_uncommitted answer carries no last stable offset');
        self::assertNull($uncommitted->abortedTransactions);

        $manager->commitTransaction();
    }

    public function testTheOffsetsApiAnswersTheLastStableOffsetToAReadCommittedClient(): void
    {
        $topic   = $this->topic('lso-offsets');
        $manager = $this->manager($this->transactionalId('lso-offsets'));
        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['open'])]], $manager);

        $committed   = new Client($this->cluster, $this->consumerConfiguration(FetchRequest::READ_COMMITTED));
        $uncommitted = new Client($this->cluster, $this->consumerConfiguration(FetchRequest::READ_UNCOMMITTED));

        // The Offsets api carries the isolation level from version 2 on, for exactly this reason: a consumer that
        // waits for the end of the log must not wait for records it is never going to be shown
        self::assertSame(
            0,
            $committed->fetchTopicPartitionOffsets([$topic => [0 => OffsetsRequest::LATEST]])[$topic][0],
            'a read_committed LATEST is the last stable offset'
        );
        self::assertSame(
            1,
            $uncommitted->fetchTopicPartitionOffsets([$topic => [0 => OffsetsRequest::LATEST]])[$topic][0],
            'a read_uncommitted LATEST is the log end offset'
        );

        $manager->commitTransaction();
    }

    public function testASecondProducerOfTheSameTransactionalIdFencesTheFirst(): void
    {
        $topic           = $this->topic('fence');
        $transactionalId = $this->transactionalId('fence');

        $first = $this->manager($transactionalId);
        $first->initTransactions();
        $first->beginTransaction();
        $first->maybeAddPartitionsToTransaction([$topic => [0 => []]]);

        // A second incarnation of the same id: its InitProducerId bumps the epoch, which fences the one above
        $second = $this->manager($transactionalId);
        $second->initTransactions();

        self::assertSame(
            $first->getProducerIdAndEpoch()->producerId,
            $second->getProducerIdAndEpoch()->producerId,
            '`__transaction_state` holds one producer id per transactional id'
        );
        // The epoch is bumped by **at least** one: `handleInitProducerId` bumps it, aborts the transaction the old
        // incarnation left open and answers 51 (ConcurrentTransactions) while it does so, and the retry of that
        // request bumps it again - so a fencing that has to roll a transaction back costs more than one epoch
        self::assertGreaterThan($first->getProducerIdAndEpoch()->epoch, $second->getProducerIdAndEpoch()->epoch);

        try {
            $first->commitTransaction();
            self::fail('the fenced producer must not be able to commit');
        } catch (ProducerFencedException) {
            // 47 InvalidProducerEpoch, the fencing of KIP-98
        }

        self::assertSame(TransactionState::FATAL_ERROR, $first->currentState(), 'and it never recovers');

        // The second producer writes its own transaction on the same partition without noticing anything
        $second->beginTransaction();
        $second->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['from the new epoch'])]], $second);
        $second->commitTransaction();

        $this->awaitMarker($topic);

        self::assertSame(['from the new epoch'], $this->read($topic, FetchRequest::READ_COMMITTED));
    }

    public function testABatchOfAFencedProducerIsRefusedByTheLogItself(): void
    {
        $topic           = $this->topic('fenced-batch');
        $transactionalId = $this->transactionalId('fenced-batch');

        $first = $this->manager($transactionalId);
        $first->initTransactions();
        $first->beginTransaction();
        $first->maybeAddPartitionsToTransaction([$topic => [0 => []]]);

        $second = $this->manager($transactionalId);
        $second->initTransactions();

        try {
            $this->client->produce([$topic => [0 => $this->records(['fenced'])]], $first);
            self::fail('a batch of the old epoch has to be refused');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(ProducerFencedException::class, $exception->getExceptions()[$topic][0]);
        }

        self::assertTrue($first->hasFatalError());
        self::assertTrue(
            $second->isReady(),
            'the new incarnation is untouched: its InitProducerId rolled the old transaction back for it'
        );
    }

    public function testAnAbortOfACommittedTransactionIsAnInvalidStateAndASecondCommitIsNot(): void
    {
        $topic           = $this->topic('states');
        $transactionalId = $this->transactionalId('states');
        $manager         = $this->manager($transactionalId);
        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['committed'])]], $manager);
        $manager->commitTransaction();

        $coordinator        = $this->client->getTransactionCoordinator($transactionalId);
        $producerIdAndEpoch = $manager->getProducerIdAndEpoch();

        // `CompleteCommit` accepts the result it already has ...
        $this->client->endTxn($coordinator, $transactionalId, $producerIdAndEpoch, EndTxnRequest::COMMIT);

        // ... and refuses the other one
        $this->expectException(InvalidTxnStateException::class);

        $this->client->endTxn($coordinator, $transactionalId, $producerIdAndEpoch, EndTxnRequest::ABORT);
    }

    public function testAddPartitionsToTxnReportsTheStateOfTheIdOnEveryPartition(): void
    {
        $topic           = $this->topic('per-partition');
        $transactionalId = $this->transactionalId('per-partition');
        $manager         = $this->manager($transactionalId);
        $manager->initTransactions();
        $stale = $manager->getProducerIdAndEpoch();

        // A second incarnation bumps the epoch; the stale one is answered 47 per partition, not at the top level
        $this->manager($transactionalId)->initTransactions();

        $this->expectException(ProducerFencedException::class);

        $this->client->addPartitionsToTxn(
            $this->client->getTransactionCoordinator($transactionalId),
            $transactionalId,
            $stale,
            [$topic => [0]]
        );
    }

    public function testTheProducerWritesAWholeTransactionThroughItsPublicApi(): void
    {
        $topic    = $this->topic('producer');
        $producer = $this->producer($this->transactionalId('producer'));

        $producer->initTransactions();
        $producer->beginTransaction();
        for ($index = 0; $index < 5; $index++) {
            $producer->send($topic, Record::fromValue("t8-transaction-{$index}"), 0);
        }
        $producer->commitTransaction();

        $this->awaitMarker($topic);

        self::assertSame(
            array_map(static fn(int $index): string => "t8-transaction-{$index}", range(0, 4)),
            $this->read($topic, FetchRequest::READ_COMMITTED)
        );
    }

    public function testTheProducerRollsBackWhatItSentWhenTheTransactionIsAborted(): void
    {
        $topic    = $this->topic('producer-abort');
        $producer = $this->producer($this->transactionalId('producer-abort'));

        $producer->initTransactions();
        $producer->beginTransaction();
        $producer->send($topic, Record::fromValue('written'), 0);
        $producer->flush();
        $producer->abortTransaction();

        // ... and the next transaction of the same producer is written normally
        $producer->beginTransaction();
        $producer->send($topic, Record::fromValue('kept'), 0);
        $producer->commitTransaction();

        $this->awaitMarker($topic, 2);

        self::assertSame(['kept'], $this->read($topic, FetchRequest::READ_COMMITTED));
        self::assertSame(['written', 'kept'], $this->read($topic, FetchRequest::READ_UNCOMMITTED));
    }

    public function testATransactionThatSpansTwoTopicsIsCommittedAsAWhole(): void
    {
        $first    = $this->topic('two-a');
        $second   = $this->topic('two-b');
        $producer = $this->producer($this->transactionalId('two-topics'));

        $producer->initTransactions();
        $producer->beginTransaction();
        $producer->send($first, Record::fromValue('a'), 0);
        $producer->send($second, Record::fromValue('b'), 0);
        $producer->commitTransaction();

        $this->awaitMarker($first);
        $this->awaitMarker($second);

        self::assertSame(['a'], $this->read($first, FetchRequest::READ_COMMITTED));
        self::assertSame(['b'], $this->read($second, FetchRequest::READ_COMMITTED));
    }

    public function testTheCommittedOffsetsOfAGroupBecomeVisibleWithTheTransactionAndNotBefore(): void
    {
        $topic           = $this->topic('offsets');
        $group           = self::uniqueTopicName('t8-transactional-offsets-group');
        $transactionalId = $this->transactionalId('offsets');
        $manager         = $this->manager($transactionalId);
        $coordinator     = $this->client->getGroupCoordinator($group);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['derived'])]], $manager);
        $manager->sendOffsetsToTransaction([$topic => [0 => 7]], $group);

        self::assertSame(
            -1,
            $this->client->fetchGroupOffsets($coordinator, $group, [$topic => [0]])[$topic][0],
            'the offset is in `__consumer_offsets` as a transactional record, invisible while the transaction runs'
        );

        $manager->commitTransaction();
        $this->awaitMarker($topic);

        $committed = $this->awaitCommittedOffset($coordinator, $group, $topic);

        self::assertSame(7, $committed, 'and it becomes visible with the very commit that made the records visible');
    }

    public function testAnAbortedTransactionLeavesTheGroupWithTheOffsetsItHadBefore(): void
    {
        $topic           = $this->topic('offsets-abort');
        $group           = self::uniqueTopicName('t8-transactional-offsets-abort-group');
        $manager         = $this->manager($this->transactionalId('offsets-abort'));
        $coordinator     = $this->client->getGroupCoordinator($group);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['rolled back'])]], $manager);
        $manager->sendOffsetsToTransaction([$topic => [0 => 7]], $group);
        $manager->abortTransaction();

        $this->awaitMarker($topic);

        self::assertSame(
            -1,
            $this->client->fetchGroupOffsets($coordinator, $group, [$topic => [0]])[$topic][0],
            'the offsets of an aborted transaction are never handed to the group'
        );
    }

    public function testAnExpiredTransactionIsAbortedByTheCoordinator(): void
    {
        $topic           = $this->topic('timeout');
        $transactionalId = $this->transactionalId('timeout');
        // The shortest timeout the coordinator accepts; it checks for expired transactions every
        // `transaction.abort.timed.out.transaction.cleanup.interval.ms`, which is 60 seconds by default
        $manager = new TransactionManager($this->client, $transactionalId, 1000, $this->configuration());

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['abandoned'])]], $manager);

        self::assertSame(
            0,
            $this->partitionOf($topic, FetchRequest::READ_COMMITTED)->lastStableOffset,
            'the transaction is open, so nothing of it is stable'
        );

        $this->awaitMarker($topic, 1, self::EXPIRY_TIMEOUT);

        self::assertSame(ControlRecordType::ABORT, $this->markerOf($topic), 'the coordinator rolled it back');
        self::assertSame([], $this->read($topic, FetchRequest::READ_COMMITTED));

        // Expiring a transaction bumps the epoch of the id, so the producer that abandoned it is fenced
        try {
            $manager->commitTransaction();
            self::fail('the producer of an expired transaction must not be able to commit it');
        } catch (ProducerFencedException) {
            // 47 InvalidProducerEpoch
        }
    }

    public function testASendOutsideATransactionIsRefusedByTheProducer(): void
    {
        $topic    = $this->topic('outside');
        $producer = $this->producer($this->transactionalId('outside'));
        $producer->initTransactions();

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('Can not send in the state READY');

        $producer->send($topic, Record::fromValue('nowhere'), 0);
    }

    /**
     * Builds a transaction manager of a fresh transactional id
     */
    private function manager(string $transactionalId): TransactionManager
    {
        return new TransactionManager($this->client, $transactionalId, 60000, $this->configuration());
    }

    /**
     * Builds a transactional producer that talks to the broker under test
     */
    private function producer(string $transactionalId): KafkaProducer
    {
        return new KafkaProducer([
            ProducerConfig::TRANSACTIONAL_ID          => $transactionalId,
            ProducerConfig::BATCH_SIZE                => 1024 * 1024,
            ProducerConfig::CLIENT_ID                 => 't8-transactional',
            ProducerConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ProducerConfig::REQUEST_TIMEOUT_MS        => 40000,
            ProducerConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ]);
    }

    /**
     * Returns the values of the records a reader of the given isolation level sees in the first partition.
     *
     * The broker only *bounds* a `read_committed` answer by the last stable offset and names the transactions it
     * aborted; dropping their records is the client's business, which is what
     * {@see AbortedTransactionFilter::committedRecords()} does inside the fetch loop of the consumer.
     *
     * @return list<string|null>
     */
    private function read(string $topic, int $isolationLevel): array
    {
        $partition = $this->partitionOf($topic, $isolationLevel);
        $records   = $isolationLevel === FetchRequest::READ_COMMITTED
            ? AbortedTransactionFilter::committedRecords($partition)
            : $partition->getRecords();

        return array_map(static fn(Record $record): ?string => $record->value, $records);
    }

    /**
     * Fetches the first partition of a topic with the given isolation level
     */
    private function partitionOf(string $topic, int $isolationLevel): FetchedPartition
    {
        $client = new Client($this->cluster, $this->consumerConfiguration($isolationLevel));

        return $client->fetchPartitions([$topic => [0 => 0]], 5000)[$topic][0];
    }

    /**
     * Returns the control record type of the last batch of a partition
     */
    private function markerOf(string $topic): int
    {
        $batches = $this->partitionOf($topic, FetchRequest::READ_UNCOMMITTED)->getMemoryRecords()->getBatches();
        $last    = end($batches);

        self::assertInstanceOf(RecordBatch::class, $last);
        self::assertTrue($last->isControlBatch(), 'the last batch of a finished transaction is its marker');

        return ControlRecordType::parse($last->getRecords()[0]->key);
    }

    /**
     * Waits until the coordinator has written the control batches of the finished transactions into a partition.
     *
     * `EndTxn` is answered as soon as the coordinator decided the outcome, so the markers arrive a moment later;
     * a reader that looked before them would see an open transaction.
     */
    private function awaitMarker(string $topic, int $expectedMarkers = 1, ?float $timeout = null): void
    {
        $timeout ??= self::MARKER_TIMEOUT;
        $deadline  = microtime(true) + $timeout;
        do {
            $markers = 0;
            foreach ($this->partitionOf($topic, FetchRequest::READ_UNCOMMITTED)->getMemoryRecords()->getBatches() as $batch) {
                if ($batch instanceof RecordBatch && $batch->isControlBatch()) {
                    $markers++;
                }
            }
            if ($markers >= $expectedMarkers) {
                return;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The transaction markers of {$topic} did not arrive within {$timeout} seconds");
    }

    /**
     * Waits until an OffsetFetch of the group answers the offset that a committed transaction wrote.
     *
     * The group coordinator only materializes a transactional offset commit into its cache once the COMMIT marker
     * of the transaction reached the `__consumer_offsets` partition, which happens after `EndTxn` was answered.
     */
    private function awaitCommittedOffset(Node $coordinator, string $group, string $topic): int
    {
        $deadline = microtime(true) + self::MARKER_TIMEOUT;
        do {
            $committed = $this->client->fetchGroupOffsets($coordinator, $group, [$topic => [0]])[$topic][0];
            if ($committed !== -1) {
                return $committed;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The committed offset of {$group} did not become visible within " . self::MARKER_TIMEOUT . ' seconds');
    }

    /**
     * Builds the records of a batch, all of them stamped with the clock of the test.
     *
     * The timestamp is `now` rather than a fixed one, because a time-based retention deletes a segment by the
     * largest timestamp it holds.
     *
     * @param list<string> $values
     *
     * @return list<Record>
     */
    private function records(array $values): array
    {
        $timestamp = (int) (microtime(true) * 1000);

        return array_map(
            static fn(string $value): Record => new Record($value, null, 0, null, $timestamp),
            $values
        );
    }

    /**
     * Creates a topic of one partition for this test and waits until the broker serves it
     */
    private function topic(string $purpose): string
    {
        $topic                 = self::uniqueTopicName("t8-transactional-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));

        // A fresh topic answers 3, 5 or 6 for a moment, until its leader is elected and known to this client
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
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
     * Returns a transactional id no other test of this branch uses
     */
    private function transactionalId(string $purpose): string
    {
        return self::uniqueTopicName("t8-transactional-{$purpose}-txn");
    }

    /**
     * Client configuration of the producer side
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 't8-transactional',
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * Client configuration of a reader of the given isolation level
     *
     * @return array<string, mixed>
     */
    private function consumerConfiguration(int $isolationLevel): array
    {
        return [ConsumerConfig::ISOLATION_LEVEL => $isolationLevel] + $this->configuration();
    }
}
