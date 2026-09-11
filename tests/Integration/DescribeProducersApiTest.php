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
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\DescribeProducersRequest;
use Protocol\Kafka\Protocol\Request\DescribeProducersResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * Exercises DescribeProducers (key 61, v0) of KIP-664 against a real Kafka 2.8.2 broker.
 *
 * The api reads out the `ProducerStateManager` of a partition - the table that makes the idempotent producer of
 * KIP-98 work - and it is the first api of this protocol that does so: until Kafka 2.8 the only way to see which
 * producer ids a partition remembered was to dump its log segments on the broker's disk. The state itself is not
 * new, so what this class verifies is that the numbers the api reports are the very ones the produce path of
 * {@see IdempotentProducerTest} and {@see TransactionalProducerTest} put there: the same producer id, the epoch,
 * the sequence of the last batch, and - the field that only a transaction sets - the first offset of a
 * transaction that is still open in this partition.
 *
 * The request is routed **per leader**, not to any node and not to a coordinator: a partition's producer state
 * lives in its log, so only the broker that holds the leader replica can answer for it.
 *
 * @see docs/protocol/2.8.md, section "DescribeProducers API (key 61, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeProducersRequest::class)]
#[CoversClass(DescribeProducersResponse::class)]
#[CoversClass(ProducerState::class)]
final class DescribeProducersApiTest extends IntegrationTestCase
{
    /**
     * How long to wait for a fresh topic to become servable, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

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

    /**
     * A partition nobody wrote to answers the error code 0 and an empty list, not an error
     */
    public function testAPartitionWithoutAProducerAnswersAnEmptyList(): void
    {
        $topic = $this->topic('empty');

        self::assertSame([$topic => [0 => []]], $this->admin->describeProducers([$topic => [0]]));
    }

    /**
     * A batch of an idempotent producer leaves exactly the state the producer stamped onto it
     */
    public function testAnIdempotentProducerIsReportedWithItsIdEpochAndLastSequence(): void
    {
        $topic   = $this->topic('idempotent');
        $manager = new TransactionManager($this->client);
        $before  = (int) (microtime(true) * 1000);

        $this->client->produce([$topic => [0 => $this->records(['one', 'two', 'three'])]], $manager);

        $producerIdAndEpoch = $manager->getProducerIdAndEpoch();
        $states             = $this->admin->describeProducers([$topic => [0]])[$topic][0];

        self::assertIsArray($states, 'a partition that could be read answers a list, not an exception');
        self::assertCount(1, $states, 'one producer wrote to this partition');

        $state = $states[0];

        self::assertInstanceOf(ProducerState::class, $state);
        self::assertSame($producerIdAndEpoch->producerId, $state->producerId);
        self::assertSame($producerIdAndEpoch->epoch, $state->producerEpoch);
        self::assertSame(0, $state->producerEpoch, 'a producer without a transactional id never leaves the epoch 0');
        self::assertSame(
            2,
            $state->lastSequence,
            'the sequence of the LAST RECORD of the last batch - three records numbered 0, 1 and 2'
        );
        self::assertGreaterThanOrEqual($before, $state->lastTimestamp);
        self::assertSame(-1, $state->coordinatorEpoch, 'no transaction coordinator owns an idempotent producer');
        self::assertFalse($state->hasOpenTransaction());
        self::assertNull(
            $state->currentTransactionStartOffset,
            'the -1 of the wire is the absence of an open transaction, and this client says so with null'
        );
    }

    /**
     * Every batch moves the last sequence on, and the state stays one entry of one producer id
     */
    public function testTheLastSequenceFollowsEveryBatchOfTheSameProducer(): void
    {
        $topic   = $this->topic('sequence');
        $manager = new TransactionManager($this->client);

        $this->client->produce([$topic => [0 => $this->records(['first'])]], $manager);

        self::assertSame(0, $this->onlyStateOf($topic)->lastSequence);

        $this->client->produce([$topic => [0 => $this->records(['second', 'third'])]], $manager);

        self::assertSame(2, $this->onlyStateOf($topic)->lastSequence, 'the batch carried the sequences 1 and 2');
    }

    /**
     * An open transaction is the one thing this api reports that no other api of this protocol does
     *
     * `ProducerStateEntry.currentTxnFirstOffset` is set by the first batch of a transaction in the partition and
     * cleared by its marker, so the field is readable exactly between the produce and the commit - and the offset
     * it holds is the base offset of that first batch, the offset a read-committed consumer stops at.
     *
     * The **coordinator epoch** next to it moves the other way round, which is the surprise of this api: it is
     * written by the **marker**, not by the batch, so a producer whose very first transaction is still open is
     * reported with the -1 of a producer that has none, and only after the commit does the partition know which
     * coordinator owned it. Measured on the container, both ways round.
     */
    public function testAnOpenTransactionIsReportedWithItsFirstOffsetAndClearedByItsMarker(): void
    {
        $topic           = $this->topic('transaction');
        $transactionalId = self::uniqueTopicName('t1-producers-txn');
        $manager         = new TransactionManager($this->client, $transactionalId, 60000, $this->configuration());

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);

        $appended = $this->client->produce([$topic => [0 => $this->records(['inside'])]], $manager);
        $open     = $this->onlyStateOf($topic);

        self::assertTrue($open->hasOpenTransaction());
        self::assertSame(
            $appended[$topic][0]->baseOffset,
            $open->currentTransactionStartOffset,
            'the first offset of the transaction in this partition, not the offset of the last batch'
        );
        self::assertSame(
            -1,
            $open->coordinatorEpoch,
            'no marker has been written yet, so the partition does not know the coordinator of this transaction'
        );
        self::assertSame(
            $manager->getProducerIdAndEpoch()->producerId,
            $open->producerId,
            'a transactional producer is remembered by the same table as an idempotent one'
        );

        $manager->commitTransaction();
        $this->awaitClosedTransaction($topic);

        $closed = $this->onlyStateOf($topic);

        self::assertFalse($closed->hasOpenTransaction(), 'the commit marker cleared the first offset');
        self::assertNull($closed->currentTransactionStartOffset);
        self::assertSame($open->producerId, $closed->producerId, 'and the producer itself is still remembered');
        self::assertGreaterThanOrEqual(
            0,
            $closed->coordinatorEpoch,
            'and the marker wrote the epoch of the coordinator that owned it'
        );
    }

    /**
     * The second transaction of the same producer keeps the coordinator epoch of the marker before it
     *
     * The distinction matters to a caller who reads the pair: a coordinator epoch of 0 or more next to an open
     * transaction says nothing about *that* transaction, it is what the previous marker of this producer left
     * behind. An abort clears the first offset exactly as a commit does.
     */
    public function testTheCoordinatorEpochOfTheMarkerOutlivesItsTransaction(): void
    {
        $topic           = $this->topic('second-transaction');
        $transactionalId = self::uniqueTopicName('t1-producers-second-txn');
        $manager         = new TransactionManager($this->client, $transactionalId, 60000, $this->configuration());

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['committed'])]], $manager);
        $manager->commitTransaction();
        $this->awaitClosedTransaction($topic);

        $afterCommit = $this->onlyStateOf($topic);

        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $appended = $this->client->produce([$topic => [0 => $this->records(['aborted'])]], $manager);
        $open     = $this->onlyStateOf($topic);

        self::assertTrue($open->hasOpenTransaction());
        self::assertSame(
            $appended[$topic][0]->baseOffset,
            $open->currentTransactionStartOffset,
            'the second transaction starts after the commit marker of the first one'
        );
        self::assertSame(
            $afterCommit->coordinatorEpoch,
            $open->coordinatorEpoch,
            'which is the epoch the marker of the FIRST transaction wrote, not one of this transaction'
        );

        $manager->abortTransaction();
        $this->awaitClosedTransaction($topic);

        self::assertFalse(
            $this->onlyStateOf($topic)->hasOpenTransaction(),
            'an abort marker clears the first offset exactly as a commit marker does'
        );
    }

    /**
     * Two producers of one partition are two entries, and the api answers both of them
     */
    public function testAPartitionRemembersEveryProducerThatWroteToIt(): void
    {
        $topic = $this->topic('two-producers');
        $first = new TransactionManager($this->client);
        $this->client->produce([$topic => [0 => $this->records(['from the first'])]], $first);

        $second = new TransactionManager($this->client);
        $this->client->produce([$topic => [0 => $this->records(['from the second'])]], $second);

        $states = $this->admin->describeProducers([$topic => [0]])[$topic][0];

        self::assertIsArray($states);
        self::assertCount(2, $states);
        $reported = array_map(static fn(ProducerState $state): int => $state->producerId, $states);
        $expected = [$first->getProducerIdAndEpoch()->producerId, $second->getProducerIdAndEpoch()->producerId];
        sort($reported);
        sort($expected);

        self::assertSame($expected, $reported, 'both producer ids, in whatever order the state table holds them');
    }

    /**
     * A partition that has no leader here is reported per partition, and the other partitions of the request answer
     *
     * The client cannot route a request about a partition the cluster does not have, so this is the raw frame: the
     * answer carries one entry per requested partition, and the unknown one is the error code **3** with the
     * message of `KafkaApis.handleDescribeProducersRequest`.
     */
    public function testAnUnknownPartitionIsReportedInItsOwnEntry(): void
    {
        $topic  = $this->topic('unknown-partition');
        $stream = $this->connect();

        new DescribeProducersRequest([$topic => [0, 7]], 't1-producers', 6101)->writeTo($stream);
        $partitions = DescribeProducersResponse::unpack($stream)->topics[$topic]->partitions;

        self::assertCount(2, $partitions, 'one entry per requested partition');
        self::assertArrayHasKey(0, $partitions);
        self::assertArrayHasKey(7, $partitions);
        self::assertSame(KafkaException::NO_ERROR, $partitions[0]->errorCode);
        self::assertNull($partitions[0]->errorMessage);
        self::assertSame([], $partitions[0]->activeProducers);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $partitions[7]->errorCode);
        self::assertSame([], $partitions[7]->activeProducers);
    }

    /**
     * A topic the cluster does not have cannot be routed at all, and is reported as such by the client
     */
    public function testAnUnknownTopicIsRefusedBeforeTheRequestIsEvenSent(): void
    {
        $topic = self::uniqueTopicName('t1-producers-unknown-topic');

        // The metadata of this client knows no such topic, and a full reload does not create one either - the
        // request is never sent, so no partition entry of it can be answered
        $this->expectException(InvalidTopicException::class);

        $this->admin->describeProducers([$topic => [0]]);
    }

    /**
     * Returns the only producer the first partition of the topic remembers
     */
    private function onlyStateOf(string $topic): ProducerState
    {
        $states = $this->admin->describeProducers([$topic => [0]])[$topic][0];

        self::assertIsArray($states, 'the partition could be read');
        self::assertCount(1, $states);
        self::assertInstanceOf(ProducerState::class, $states[0]);

        return $states[0];
    }

    /**
     * Waits until the marker of the committed transaction was written into the partition and cleared its offset
     */
    private function awaitClosedTransaction(string $topic): void
    {
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            if (!$this->onlyStateOf($topic)->hasOpenTransaction()) {
                return;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail('the commit marker was not written within ' . self::TOPIC_TIMEOUT . ' seconds');
    }

    /**
     * Builds the records of a batch, all of them stamped with the same `CreateTime`
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
        $topic                 = self::uniqueTopicName("t1-producers-{$purpose}");
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
     * Client configuration pointing at the broker under test
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 't1-producers',
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }
}
