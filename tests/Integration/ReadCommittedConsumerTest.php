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
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\AbortedTransactionFilter;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * The read side of KIP-98 against a real Kafka 1.1.1 broker: `isolation.level = read_committed`.
 *
 * A `read_committed` consumer is the only thing that makes a transaction worth anything, and three of its
 * properties can only be seen against a broker: that the answer stops at the *last stable offset* while a
 * transaction is open, that the records of an **aborted** transaction really do arrive and really are dropped by
 * the consumer, and that a control batch never reaches an application. The last test of the class is the
 * consume-transform-produce loop that all of this exists for.
 *
 * @see docs/protocol/1.1.md, section "Transactions"
 */
#[CoversClass(KafkaConsumer::class)]
#[CoversClass(AbortedTransactionFilter::class)]
#[CoversClass(KafkaProducer::class)]
#[CoversClass(Client::class)]
final class ReadCommittedConsumerTest extends IntegrationTestCase
{
    /**
     * How long to wait for a fresh topic to become servable, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * How long to wait for the records of a committed transaction to become visible, in seconds
     */
    private const float VISIBILITY_TIMEOUT = 30.0;

    private Cluster $cluster;

    private AdminClient $admin;

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
    }

    protected function tearDown(): void
    {
        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    public function testAReadCommittedConsumerSeesNothingOfAnOpenTransaction(): void
    {
        $topic    = $this->topic('open');
        $producer = $this->producer($this->transactionalId('open'));
        $producer->initTransactions();
        $producer->beginTransaction();
        $producer->send($topic, Record::fromValue('still open'), 0);
        $producer->flush();

        self::assertSame([], $this->poll($topic, ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED));
        self::assertSame(['still open'], $this->poll($topic, ConsumerConfig::ISOLATION_LEVEL_READ_UNCOMMITTED));

        $producer->commitTransaction();

        self::assertSame(
            ['still open'],
            $this->pollUntilVisible($topic, ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED),
            'and everything of it at once when it is committed'
        );
    }

    public function testAReadCommittedConsumerDropsTheRecordsOfAnAbortedTransaction(): void
    {
        $topic    = $this->topic('aborted');
        $producer = $this->producer($this->transactionalId('aborted'));
        $producer->initTransactions();

        $producer->beginTransaction();
        $producer->send($topic, Record::fromValue('rolled back'), 0);
        $producer->flush();
        $producer->abortTransaction();

        $producer->beginTransaction();
        $producer->send($topic, Record::fromValue('committed'), 0);
        $producer->commitTransaction();

        self::assertSame(
            ['committed'],
            $this->pollUntilVisible($topic, ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED)
        );
        self::assertSame(
            ['rolled back', 'committed'],
            $this->poll($topic, ConsumerConfig::ISOLATION_LEVEL_READ_UNCOMMITTED),
            'the aborted records are still in the log and a read_uncommitted consumer shows them'
        );
    }

    public function testTheBrokerAnswersTheAbortedRecordsAndTheConsumerIsTheOneThatDropsThem(): void
    {
        $topic    = $this->topic('unfiltered');
        $producer = $this->producer($this->transactionalId('unfiltered'));
        $producer->initTransactions();
        $producer->beginTransaction();
        $producer->send($topic, Record::fromValue('rolled back'), 0);
        $producer->flush();
        $producer->abortTransaction();

        $client    = new Client($this->cluster, $this->consumerConfiguration(
            ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED
        ));
        $partition = $this->awaitStableOffset($client, $topic, 2);

        // What the raw answer of a `read_committed` fetch holds: the aborted records, the ABORT control batch and
        // the array that names the transaction - the broker filters nothing at all
        self::assertSame(
            ['rolled back'],
            array_map(static fn(Record $record): ?string => $record->value, $partition->getRecords()),
            'the record layer only drops the control batch'
        );
        self::assertCount(2, $partition->getMemoryRecords()->getBatches(), 'the data batch and its ABORT marker');
        self::assertNotNull($partition->abortedTransactions);
        self::assertCount(1, $partition->abortedTransactions);

        self::assertSame(
            [],
            AbortedTransactionFilter::committedRecords($partition),
            'and the filter of the fetch loop is what makes them invisible'
        );
    }

    public function testEndOffsetsAnswersTheLastStableOffsetToAReadCommittedConsumer(): void
    {
        $topic    = $this->topic('end-offsets');
        $producer = $this->producer($this->transactionalId('end-offsets'));
        $producer->initTransactions();
        $producer->beginTransaction();
        $producer->send($topic, Record::fromValue('open'), 0);
        $producer->flush();

        $committed   = $this->consumer($topic, ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED);
        $uncommitted = $this->consumer($topic, ConsumerConfig::ISOLATION_LEVEL_READ_UNCOMMITTED);

        self::assertSame(
            0,
            $committed->endOffsets([$topic => [0]])[$topic][0],
            'a read_committed consumer must not wait for records it is never going to be shown'
        );
        self::assertSame(
            1,
            $uncommitted->endOffsets([$topic => [0]])[$topic][0],
            'a read_uncommitted consumer sees the log end offset'
        );

        $producer->commitTransaction();
        $committed->close();
        $uncommitted->close();
    }

    public function testAControlBatchNeverReachesAnApplication(): void
    {
        $topic    = $this->topic('markers');
        $producer = $this->producer($this->transactionalId('markers'));
        $producer->initTransactions();

        foreach (['one', 'two'] as $value) {
            $producer->beginTransaction();
            $producer->send($topic, Record::fromValue($value), 0);
            $producer->commitTransaction();
        }

        $records = $this->pollUntilVisible($topic, ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED, 2);

        // Two records and two markers are in the log, and the offsets of the records are 0 and 2 because the
        // marker of the first transaction sits between them
        self::assertSame(['one', 'two'], $records);
        self::assertSame(
            4,
            $this->consumer($topic, ConsumerConfig::ISOLATION_LEVEL_READ_UNCOMMITTED)->endOffsets([$topic => [0]])[$topic][0],
            'the control batches take an offset of their own in the log'
        );
    }

    public function testTheConsumeTransformProduceLoopIsAtomic(): void
    {
        $input    = $this->topic('input');
        $output   = $this->topic('output');
        $group    = self::uniqueTopicName('t8-read-committed-loop-group');
        $producer = $this->producer($this->transactionalId('loop'));

        // The input of the loop, written without a transaction
        $plain = new KafkaProducer($this->producerConfiguration());
        foreach (['one', 'two', 'three'] as $value) {
            $plain->send($input, Record::fromValue($value), 0);
        }
        $plain->flush();

        $consumer = $this->consumer($input, ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED, $group);
        $producer->initTransactions();

        $consumed = [];
        $deadline = microtime(true) + self::VISIBILITY_TIMEOUT;
        do {
            $polled = $consumer->poll(1000)[$input][0] ?? [];
            if ($polled === []) {
                continue;
            }
            $producer->beginTransaction();
            foreach ($polled as $record) {
                $consumed[] = $record->value;
                $producer->send($output, Record::fromValue(strtoupper((string) $record->value)), 0);
            }
            $producer->flush();
            // The offsets of the input are committed by the producer, inside the transaction that holds the output
            $producer->sendOffsetsToTransaction([$input => [0 => $consumer->position($input, 0)]], $group);
            $producer->commitTransaction();
        } while (count($consumed) < 3 && microtime(true) < $deadline);

        self::assertSame(['one', 'two', 'three'], $consumed);
        self::assertSame(
            ['ONE', 'TWO', 'THREE'],
            $this->pollUntilVisible($output, ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED, 3)
        );
        // The offsets of a transactional commit become visible with the COMMIT marker that reaches
        // `__consumer_offsets`, so they arrive a moment after the transaction was committed
        $deadline = microtime(true) + self::VISIBILITY_TIMEOUT;
        do {
            $committed = $consumer->committed([$input => [0]])[$input][0];
            if ($committed !== -1) {
                break;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::assertSame(3, $committed, 'the group continues behind the records the transaction consumed');

        $consumer->close();
    }

    /**
     * Polls a topic once with a consumer of the given isolation level and returns the values it saw
     *
     * @return list<string|null>
     */
    private function poll(string $topic, string $isolationLevel): array
    {
        $consumer = $this->consumer($topic, $isolationLevel);
        $records  = $consumer->poll(2000)[$topic][0] ?? [];
        $consumer->close();

        return array_map(static fn(Record $record): ?string => $record->value, $records);
    }

    /**
     * Polls a topic until it holds the expected number of records, which is what a committed transaction needs.
     *
     * `EndTxn` is answered before the control batches reach the partitions, so a poll right after the commit may
     * still be cut at the last stable offset.
     *
     * @return list<string|null>
     */
    private function pollUntilVisible(string $topic, string $isolationLevel, int $expected = 1): array
    {
        $deadline = microtime(true) + self::VISIBILITY_TIMEOUT;
        do {
            $values = $this->poll($topic, $isolationLevel);
            if (count($values) >= $expected) {
                return $values;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The records of {$topic} did not become visible within " . self::VISIBILITY_TIMEOUT . ' seconds');
    }

    /**
     * Waits until the last stable offset of a partition reached the given value and returns the answer
     */
    private function awaitStableOffset(Client $client, string $topic, int $lastStableOffset): \Protocol\Kafka\Common\FetchedPartition
    {
        $deadline = microtime(true) + self::VISIBILITY_TIMEOUT;
        do {
            $partition = $client->fetchPartitions([$topic => [0 => 0]], 2000)[$topic][0];
            if ($partition->lastStableOffset >= $lastStableOffset) {
                return $partition;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The last stable offset of {$topic} did not reach {$lastStableOffset}");
    }

    /**
     * Builds a consumer of the given isolation level, assigned to the first partition of a topic
     */
    private function consumer(string $topic, string $isolationLevel, ?string $groupId = null): KafkaConsumer
    {
        $consumer = new KafkaConsumer([
            ConsumerConfig::ISOLATION_LEVEL    => $isolationLevel,
            ConsumerConfig::GROUP_ID           => $groupId ?? self::uniqueTopicName('t8-read-committed-group'),
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
            ConsumerConfig::CLIENT_ID          => 't8-read-committed',
            ConsumerConfig::BOOTSTRAP_SERVERS  => ['tcp://' . self::firstBootstrapServer()],
        ]);
        $consumer->assign([$topic => new PartitionsForTopic($topic, [0])]);

        return $consumer;
    }

    /**
     * Builds a transactional producer that talks to the broker under test
     */
    private function producer(string $transactionalId): KafkaProducer
    {
        return new KafkaProducer(
            [ProducerConfig::TRANSACTIONAL_ID => $transactionalId] + $this->producerConfiguration()
        );
    }

    /**
     * Creates a topic of one partition for this test and waits until the broker serves it
     */
    private function topic(string $purpose): string
    {
        $topic                 = self::uniqueTopicName("t8-read-committed-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));

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
        return self::uniqueTopicName("t8-read-committed-{$purpose}-txn");
    }

    /**
     * @return array<string, mixed>
     */
    private function producerConfiguration(): array
    {
        return [
            ProducerConfig::BATCH_SIZE                => 1024 * 1024,
            ProducerConfig::CLIENT_ID                 => 't8-read-committed',
            ProducerConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ProducerConfig::REQUEST_TIMEOUT_MS        => 40000,
            ProducerConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 't8-read-committed',
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * @return array<string, mixed>
     */
    private function consumerConfiguration(string $isolationLevel): array
    {
        return [ConsumerConfig::ISOLATION_LEVEL => $isolationLevel] + $this->configuration();
    }
}
