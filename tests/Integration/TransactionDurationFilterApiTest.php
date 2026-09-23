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
use Protocol\Kafka\Admin\TransactionListing;
use Protocol\Kafka\Admin\TransactionState;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\ListTransactionsRequest;
use Protocol\Kafka\Protocol\Request\ListTransactionsRequestV0;
use Protocol\Kafka\Protocol\Request\ListTransactionsResponse;
use Protocol\Kafka\Protocol\Request\ListTransactionsResponseV0;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * The `duration_filter` of ListTransactions v1 (key 66, Kafka 3.8, KIP-994) against the 3.9.2 KRaft node.
 *
 * Version 1 appends one int64 to the request of Kafka 3.0 and leaves the answer untouched. The filter is an
 * **age** in milliseconds and it is measured against `txnStartTimestamp`, the moment the current transaction
 * opened - which is a timestamp the coordinator never clears, so it outlives the transaction it belongs to, and
 * which is the **-1** of "never started" for an id that has only ever run `InitProducerId`.
 *
 * The node is shared and its coordinator holds the transactional ids of every suite that ever ran against it, so
 * every assertion here is bounded by the producer ids of this class.
 *
 * @see docs/protocol/4.3.md, sections "ListTransactions API (key 66, v0 and v1)" and "The duration filter of
 *      KIP-994 (v1)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(ListTransactionsRequest::class)]
#[CoversClass(ListTransactionsRequestV0::class)]
#[CoversClass(ListTransactionsResponse::class)]
#[CoversClass(ListTransactionsResponseV0::class)]
#[CoversClass(TransactionListing::class)]
final class TransactionDurationFilterApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t1-38-dur';

    /**
     * How long to wait for a fresh topic or for the markers of a commit, in seconds
     */
    private const float TIMEOUT = 30.0;

    /**
     * A filter no transaction of this class can be older than
     */
    private const int A_MINUTE = 60000;

    private Cluster $cluster;

    private AdminClient $admin;

    private Client $client;

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
        $this->client  = new Client($this->cluster, $this->configuration());
    }

    /**
     * A transaction that is seconds old passes a filter of 0 and fails one of a minute
     */
    public function testTheFilterSelectsTheTransactionsThatAreOlderThanIt(): void
    {
        $topic           = $this->topic('open');
        $transactionalId = self::uniqueTopicName('t1-38-dur-open');
        $manager         = $this->transactionManagerFor($transactionalId);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['inside'])]], $manager);

        $producerId = $manager->getProducerIdAndEpoch()->producerId;

        $unfiltered = $this->admin->listTransactions([], [$producerId]);
        self::assertArrayHasKey($transactionalId, $unfiltered);
        self::assertSame(TransactionState::Ongoing, $unfiltered[$transactionalId]->state);

        $everything = $this->admin->listTransactions([], [$producerId], durationFilterMs: 0);
        self::assertArrayHasKey(
            $transactionalId,
            $everything,
            'the filter 0 is "older than zero milliseconds", which every started transaction is'
        );

        $aMinute = $this->admin->listTransactions([], [$producerId], durationFilterMs: self::A_MINUTE);
        self::assertArrayNotHasKey(
            $transactionalId,
            $aMinute,
            'and a transaction of this test is never a minute old'
        );

        $manager->commitTransaction();
        $this->awaitState($transactionalId, $producerId, TransactionState::CompleteCommit);
    }

    /**
     * The age is the age of the TRANSACTION, and the coordinator keeps its start time after the commit
     */
    public function testTheAgeOutlivesTheTransactionItBelongsTo(): void
    {
        $topic           = $this->topic('committed');
        $transactionalId = self::uniqueTopicName('t1-38-dur-committed');
        $manager         = $this->transactionManagerFor($transactionalId);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['inside'])]], $manager);
        $manager->commitTransaction();

        $producerId = $manager->getProducerIdAndEpoch()->producerId;
        $this->awaitState($transactionalId, $producerId, TransactionState::CompleteCommit);

        self::assertArrayHasKey(
            $transactionalId,
            $this->admin->listTransactions([], [$producerId], durationFilterMs: 0),
            'the committed transaction is still older than zero milliseconds'
        );
        self::assertArrayNotHasKey(
            $transactionalId,
            $this->admin->listTransactions([], [$producerId], durationFilterMs: self::A_MINUTE),
            'and it is selected by the age of the transaction, not by the age of the commit'
        );
    }

    /**
     * An id that never began a transaction passes EVERY duration filter, because its start time is -1
     */
    public function testAnIdThatNeverStartedATransactionPassesEveryFilter(): void
    {
        $transactionalId = self::uniqueTopicName('t1-38-dur-empty');
        $manager         = $this->transactionManagerFor($transactionalId);

        $manager->initTransactions();
        $producerId = $manager->getProducerIdAndEpoch()->producerId;

        $listed = $this->admin->listTransactions([], [$producerId], durationFilterMs: self::A_MINUTE);

        self::assertArrayHasKey(
            $transactionalId,
            $listed,
            'its txnStartTimestamp is -1, and now - (-1) is older than any filter a client can send'
        );
        self::assertSame(TransactionState::Empty, $listed[$transactionalId]->state);
    }

    /**
     * The three filters are ANDed: a state that matches and a duration that does not is an empty answer
     */
    public function testTheThreeFiltersAreAnded(): void
    {
        $topic           = $this->topic('anded');
        $transactionalId = self::uniqueTopicName('t1-38-dur-anded');
        $manager         = $this->transactionManagerFor($transactionalId);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['inside'])]], $manager);

        $producerId = $manager->getProducerIdAndEpoch()->producerId;

        self::assertArrayHasKey(
            $transactionalId,
            $this->admin->listTransactions([TransactionState::Ongoing], [$producerId], durationFilterMs: 0)
        );
        self::assertArrayNotHasKey(
            $transactionalId,
            $this->admin->listTransactions(
                [TransactionState::Ongoing],
                [$producerId],
                durationFilterMs: self::A_MINUTE
            ),
            'the state matches and the duration does not, so the entry is dropped'
        );
        self::assertArrayNotHasKey(
            $transactionalId,
            $this->admin->listTransactions([TransactionState::CompleteAbort], [$producerId], durationFilterMs: 0),
            'and the other way round'
        );

        $manager->commitTransaction();
        $this->awaitState($transactionalId, $producerId, TransactionState::CompleteCommit);
    }

    /**
     * A version 0 frame is answered as if it carried the filter -1, and its answer is the version 1 answer
     */
    public function testTheVersion0FrameListsAsIfTheFilterWereMinusOne(): void
    {
        $topic           = $this->topic('version0');
        $transactionalId = self::uniqueTopicName('t1-38-dur-version0');
        $manager         = $this->transactionManagerFor($transactionalId);

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->maybeAddPartitionsToTransaction([$topic => [0 => []]]);
        $this->client->produce([$topic => [0 => $this->records(['inside'])]], $manager);

        $producerId = $manager->getProducerIdAndEpoch()->producerId;

        $stream = $this->connect();
        new ListTransactionsRequestV0([], [$producerId], self::CLIENT_ID, 3801)->writeTo($stream);
        $atVersion0 = ListTransactionsResponseV0::unpack($stream);

        new ListTransactionsRequest([], [$producerId], self::CLIENT_ID, 3802, -1)->writeTo($stream);
        $atVersion1 = ListTransactionsResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $atVersion0->errorCode);
        self::assertArrayHasKey($transactionalId, $atVersion0->transactionStates);
        self::assertSame(
            array_keys($atVersion0->transactionStates),
            array_keys($atVersion1->transactionStates),
            'a version below the filter lists what the filter -1 lists'
        );
        self::assertSame(
            $atVersion0->transactionStates[$transactionalId]->transactionState,
            $atVersion1->transactionStates[$transactionalId]->transactionState
        );

        $manager->commitTransaction();
        $this->awaitState($transactionalId, $producerId, TransactionState::CompleteCommit);
    }

    /**
     * Waits until the coordinator reports the expected state of an id, so that the next test finds it settled
     */
    private function awaitState(string $transactionalId, int $producerId, TransactionState $expected): void
    {
        $deadline = microtime(true) + self::TIMEOUT;
        do {
            $listed = $this->admin->listTransactions([], [$producerId]);
            if (($listed[$transactionalId] ?? null)?->state === $expected) {
                return;
            }
            usleep(100000);
        } while (microtime(true) < $deadline);

        self::fail("{$transactionalId} did not reach {$expected->value} within " . self::TIMEOUT . ' seconds');
    }

    /**
     * Builds a transactional producer of this test class
     */
    private function transactionManagerFor(string $transactionalId): TransactionManager
    {
        return new TransactionManager($this->client, $transactionalId, self::A_MINUTE, $this->configuration());
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
        $topic = self::uniqueTopicName("t1-38-dur-{$purpose}");

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
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }
}
