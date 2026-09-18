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
use Protocol\Kafka\Admin\TransactionDescription;
use Protocol\Kafka\Admin\TransactionListing;
use Protocol\Kafka\Admin\TransactionState;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\TransactionalIdNotFoundException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\DescribeTransactionsRequest;
use Protocol\Kafka\Protocol\Request\DescribeTransactionsResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * Exercises the two apis Kafka 3.0 added to the admin surface against the 3.9.2 node: DescribeTransactions
 * (key 65) and ListTransactions (key 66).
 *
 * Where {@see DescribeProducersApiTest} reads the producer state a **log** keeps, these two read the state the
 * **transaction coordinator** keeps: which transactional ids it knows, in which state each of them is, which
 * producer id and epoch it handed out, and which partitions belong to the transaction that is open now. The pair
 * is what makes a hanging transaction diagnosable from a client at all - until Kafka 3.0 the coordinator's half
 * could only be read by dumping `__transaction_state` on the broker's disk.
 *
 * The node is shared by the whole suite, and ListTransactions answers the transactions of **every** suite that
 * ever ran against it, so every assertion about a listing here is either bounded by the producer id of this test
 * or a superset assertion.
 *
 * @see docs/protocol/3.9.md, sections "DescribeTransactions API (key 65, v0)" and "ListTransactions API (key 66, v0 and v1)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeTransactionsRequest::class)]
#[CoversClass(DescribeTransactionsResponse::class)]
#[CoversClass(TransactionDescription::class)]
#[CoversClass(TransactionListing::class)]
#[CoversClass(TransactionState::class)]
final class TransactionsAdminApiTest extends IntegrationTestCase
{
    /**
     * How long to wait for a fresh topic or for a marker of the coordinator, in seconds
     */
    private const float TIMEOUT = 30.0;

    /**
     * Transaction timeout of every producer of this class, in milliseconds
     */
    private const int TRANSACTION_TIMEOUT_MS = 60000;

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
     * An id that never ran `InitProducerId` is the error code 105 in its own entry, not an exception of the call
     */
    public function testAnIdTheCoordinatorDoesNotKnowIsTheCode105(): void
    {
        $unknown = self::uniqueTopicName('t1-txadmin-nobody');

        $described = $this->admin->describeTransactions([$unknown]);

        self::assertArrayHasKey($unknown, $described);
        self::assertInstanceOf(TransactionalIdNotFoundException::class, $described[$unknown]);
        self::assertSame(KafkaException::TRANSACTIONAL_ID_NOT_FOUND, $described[$unknown]->getCode());
    }

    /**
     * `InitProducerId` alone puts the id into `Empty`, and `beginTransaction()` does not move it
     *
     * The distinction is the surprise of this api: `beginTransaction()` is a client-side flag of the producer and
     * reaches no broker at all. The coordinator hears of a transaction with the first `AddPartitionsToTxn`, and
     * only then does the state become `Ongoing`.
     */
    public function testBeginTransactionAloneLeavesTheCoordinatorInEmpty(): void
    {
        $transactionalId = self::uniqueTopicName('t1-txadmin-empty');
        $manager         = $this->transactionManagerFor($transactionalId);

        $manager->initTransactions();
        $afterInit = $this->describe($transactionalId);

        self::assertSame(TransactionState::Empty, $afterInit->state);
        self::assertFalse($afterInit->hasOpenTransaction());
        self::assertNull($afterInit->transactionStartTimeMs, 'the -1 of the wire is "no transaction ever"');
        self::assertSame([], $afterInit->topicPartitions);
        self::assertSame(self::TRANSACTION_TIMEOUT_MS, $afterInit->transactionTimeoutMs);
        self::assertSame($manager->getProducerIdAndEpoch()->producerId, $afterInit->producerId);
        self::assertSame($manager->getProducerIdAndEpoch()->epoch, $afterInit->producerEpoch);
        self::assertSame(
            self::clusterBrokers()[array_key_first(self::clusterBrokers())]->nodeId,
            $afterInit->coordinatorId,
            'the one broker of the container coordinates every transactional id'
        );

        $manager->beginTransaction();

        self::assertSame(
            TransactionState::Empty,
            $this->describe($transactionalId)->state,
            'beginTransaction() never reaches the coordinator'
        );
    }

    /**
     * The partitions of the open transaction are the ones an AddPartitionsToTxn named, and the markers clear them
     */
    public function testAnOpenTransactionNamesItsPartitionsAndTheCommitClearsThem(): void
    {
        $topic           = $this->topic('open');
        $transactionalId = self::uniqueTopicName('t1-txadmin-open');
        $manager         = $this->transactionManagerFor($transactionalId);
        $before          = (int) (microtime(true) * 1000);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['inside'])]], $manager);

        $open = $this->describe($transactionalId);

        self::assertSame(TransactionState::Ongoing, $open->state);
        self::assertTrue($open->hasOpenTransaction());
        self::assertNotNull($open->transactionStartTimeMs);
        self::assertGreaterThanOrEqual($before, $open->transactionStartTimeMs);
        self::assertSame(
            ["{$topic}-0"],
            array_map(strval(...), $open->topicPartitions),
            'the partitions the coordinator was told about, not the ones that were written to'
        );
        self::assertSame($manager->getProducerIdAndEpoch()->producerId, $open->producerId);

        $manager->commitTransaction();
        $settled = $this->describeUntilSettled($transactionalId);

        self::assertSame(TransactionState::CompleteCommit, $settled->state);
        self::assertFalse($settled->hasOpenTransaction());
        self::assertSame([], $settled->topicPartitions, 'every marker has been written');
        self::assertSame(
            $open->transactionStartTimeMs,
            $settled->transactionStartTimeMs,
            'the start time of a transaction outlives it - the coordinator never clears it'
        );
    }

    /**
     * An abort ends in `CompleteAbort`, and the entries of several ids come back in the order of the request
     */
    public function testAnAbortIsReportedAndSeveralIdsTravelInOneFrame(): void
    {
        $topic           = $this->topic('abort');
        $aborted         = self::uniqueTopicName('t1-txadmin-abort');
        $unknown         = self::uniqueTopicName('t1-txadmin-absent');
        $manager         = $this->transactionManagerFor($aborted);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['rolled back'])]], $manager);
        $manager->abortTransaction();
        $this->describeUntilSettled($aborted);

        $described = $this->admin->describeTransactions([$unknown, $aborted]);

        self::assertSame([$unknown, $aborted], array_keys($described), 'the order of the request');
        self::assertInstanceOf(TransactionalIdNotFoundException::class, $described[$unknown]);
        self::assertInstanceOf(TransactionDescription::class, $described[$aborted]);
        self::assertSame(TransactionState::CompleteAbort, $described[$aborted]->state);
        self::assertSame([], $described[$aborted]->topicPartitions);
    }

    /**
     * A transactional id that is the empty string is refused per entry, and the other ids are still answered
     *
     * The client cannot ask for it through {@see AdminClient::describeTransactions()} - the coordinator lookup of
     * an empty key fails first - so this is the raw frame.
     */
    public function testTheEmptyTransactionalIdIsRefusedInItsOwnEntry(): void
    {
        $transactionalId = self::uniqueTopicName('t1-txadmin-mixed');
        $this->transactionManagerFor($transactionalId)->initTransactions();

        $stream = $this->connect();
        new DescribeTransactionsRequest(['', $transactionalId], 't1-txadmin', 6511)->writeTo($stream);
        $states = DescribeTransactionsResponse::unpack($stream)->transactionStates;

        self::assertCount(2, $states, 'one entry per requested id');
        self::assertSame(
            KafkaException::INVALID_REQUEST,
            $states['']->errorCode,
            'the coordinator refuses an empty transactional id, and this api has no top-level error code'
        );
        self::assertSame('', $states['']->transactionState, 'a refused entry carries the defaults behind the id');
        self::assertSame(0, $states['']->transactionStartTimeMs, 'the 0 of the specification, not the -1 of Empty');
        self::assertSame(KafkaException::NO_ERROR, $states[$transactionalId]->errorCode);
        self::assertSame('Empty', $states[$transactionalId]->transactionState);
    }

    /**
     * The listing names the transaction of this producer, with the three fields the api carries
     */
    public function testTheListingNamesTheTransactionOfItsProducer(): void
    {
        $topic           = $this->topic('listed');
        $transactionalId = self::uniqueTopicName('t1-txadmin-listed');
        $manager         = $this->transactionManagerFor($transactionalId);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['listed'])]], $manager);

        $producerId = $manager->getProducerIdAndEpoch()->producerId;
        $listed     = $this->admin->listTransactions([], [$producerId]);

        self::assertSame([$transactionalId], array_keys($listed), 'the producer id bounds the answer to this one');
        self::assertSame($producerId, $listed[$transactionalId]->producerId);
        self::assertSame(TransactionState::Ongoing, $listed[$transactionalId]->state);

        self::assertArrayHasKey(
            $transactionalId,
            $this->admin->listTransactions(),
            'and the unfiltered listing of the cluster contains it as well'
        );

        $manager->commitTransaction();
        $this->describeUntilSettled($transactionalId);

        self::assertSame(
            TransactionState::CompleteCommit,
            $this->admin->listTransactions([], [$producerId])[$transactionalId]->state,
            'the listing follows the state of the coordinator'
        );
    }

    /**
     * The two filters are ANDed, and a state filter that matches nothing is an empty answer and not an error
     */
    public function testAStateFilterBoundsTheListingAndIsMatchedVerbatim(): void
    {
        $topic           = $this->topic('filtered');
        $transactionalId = self::uniqueTopicName('t1-txadmin-filtered');
        $manager         = $this->transactionManagerFor($transactionalId);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['filtered'])]], $manager);

        $producerId = $manager->getProducerIdAndEpoch()->producerId;
        $unknown    = null;

        self::assertSame(
            [$transactionalId],
            array_keys($this->admin->listTransactions([TransactionState::Ongoing], [$producerId], $unknown)),
            'the state of this transaction'
        );
        self::assertSame([], $unknown, 'a state the coordinator knows is not reported');

        self::assertSame(
            [],
            $this->admin->listTransactions([TransactionState::CompleteAbort], [$producerId], $unknown),
            'a filter that matches nothing is an empty answer with the error code 0'
        );
        self::assertSame([], $unknown);

        self::assertSame(
            [],
            $this->admin->listTransactions(['ongoing'], [$producerId], $unknown),
            'the names are matched verbatim, so a lower-case one matches nothing at all'
        );
        self::assertSame(
            ['ongoing'],
            $unknown,
            'and comes back in unknown_state_filters, while the error code of the answer stays 0'
        );

        self::assertSame(
            [$transactionalId],
            array_keys($this->admin->listTransactions(['nonsense', 'Ongoing'], [$producerId], $unknown)),
            'an unknown name next to a known one is reported and the known one still filters'
        );
        self::assertSame(['nonsense'], $unknown);
    }

    /**
     * Describes one transactional id and insists that the coordinator answered it
     */
    private function describe(string $transactionalId): TransactionDescription
    {
        $described = $this->admin->describeTransactions([$transactionalId])[$transactionalId];

        self::assertInstanceOf(TransactionDescription::class, $described, 'the coordinator knows this id');

        return $described;
    }

    /**
     * Describes one transactional id until no transaction of it is in flight any more
     *
     * A commit or an abort returns as soon as the coordinator has written its decision, while the markers of the
     * partitions are written afterwards - the state passes through `PrepareCommit`/`PrepareAbort` on the way.
     */
    private function describeUntilSettled(string $transactionalId): TransactionDescription
    {
        $deadline = microtime(true) + self::TIMEOUT;
        do {
            $described = $this->describe($transactionalId);
            if (!$described->hasOpenTransaction()) {
                return $described;
            }
            usleep(100000);
        } while (microtime(true) < $deadline);

        self::fail("the markers of {$transactionalId} were not written within " . self::TIMEOUT . ' seconds');
    }

    /**
     * Builds a transactional producer of this test class
     */
    private function transactionManagerFor(string $transactionalId): TransactionManager
    {
        return new TransactionManager(
            $this->client,
            $transactionalId,
            self::TRANSACTION_TIMEOUT_MS,
            $this->configuration()
        );
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
        $topic                 = self::uniqueTopicName("t1-txadmin-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));

        // A fresh topic answers 3, 5 or 6 for a moment, until its leader is elected and known to this client
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
     * Client configuration pointing at the broker under test
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 't1-txadmin',
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }
}
