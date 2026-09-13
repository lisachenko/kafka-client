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
use Protocol\Kafka\Common\Errors\UnstableOffsetCommitException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV6;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV6;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * The stable offsets of KIP-447 (Kafka 2.5) against a real Kafka 2.8.2 broker.
 *
 * A transactional producer writes the offsets of the consumer it feeds into `__consumer_offsets` as part of its
 * transaction, and the coordinator keeps such an offset in `pendingTransactionalOffsetCommits` until that
 * transaction commits. Every OffsetFetch below version 7 answers the last **stable** offset in the meantime and
 * says nothing about the pending one, so a consumer that starts from it may read records the transaction is about
 * to abort. **Version 7** added the boolean `require_stable`, which makes the coordinator answer such a partition
 * with the retriable error code **88** (`UnstableOffsetCommit`) instead - `GroupMetadataManager.getOffsets`
 * @ 2.8.2 - and this client sends it exactly when `isolation.level` is `read_committed`.
 *
 * Every topic, group and transactional id of this class is named `t3-25-…`, so that the tests can run next to the
 * other suites on the shared container.
 *
 * @see docs/protocol/2.8.md, sections "Stable offsets and the 88 of KIP-447 (Kafka 2.5)" and "OffsetFetch API
 *      (key 9, v0 to v7)"
 */
#[CoversClass(OffsetFetchRequest::class)]
#[CoversClass(OffsetFetchResponse::class)]
#[CoversClass(OffsetFetchRequestV6::class)]
#[CoversClass(OffsetFetchResponseV6::class)]
#[CoversClass(UnstableOffsetCommitException::class)]
#[CoversClass(Client::class)]
#[CoversClass(KafkaConsumer::class)]
final class StableOffsetsApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-25-stable';

    private const int REQUEST_TIMEOUT_MS = 30000;

    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * The cluster is resolved once: every test of this class talks to the same brokers
     */
    private static ?Cluster $sharedCluster = null;

    private Client $client;

    private AdminClient $admin;

    /**
     * Topic of the current test, deleted again when it ends
     */
    private string $topic;

    /**
     * Transaction that the current test left open, aborted in the tear-down so that nothing stays pending
     */
    private ?TransactionManager $openTransaction = null;

    protected function setUp(): void
    {
        parent::setUp();

        $this->client = new Client($this->cluster(), $this->configuration());
        $this->admin  = new AdminClient($this->cluster(), $this->configuration());
        $this->topic  = self::uniqueTopicName('t3-25-stable');

        self::assertSame([$this->topic => null], $this->admin->createTopics([new NewTopic($this->topic, 1, 1)]));
        $this->awaitTopic($this->topic);
    }

    protected function tearDown(): void
    {
        // An open transaction of a test that failed would hold the partition back for every test after it
        if ($this->openTransaction !== null) {
            try {
                $this->openTransaction->abortTransaction();
            } catch (KafkaException) {
                // The transaction is gone with its producer id, which is all this clean-up wanted
            }
            $this->openTransaction = null;
        }

        if (isset($this->topic)) {
            try {
                $this->admin->deleteTopics([$this->topic]);
            } catch (KafkaException) {
                // A broker that can not delete the topic right now must not fail the test that just passed
            }
        }

        parent::tearDown();
    }

    /**
     * With nothing pending the flag changes nothing at all: both frames answer the committed offset
     */
    public function testTheFlagChangesNothingWhileNoTransactionHoldsThePartition(): void
    {
        $groupId     = self::uniqueGroupName();
        $coordinator = $this->client->getGroupCoordinator($groupId);

        $this->client->commitGroupOffsets($coordinator, $groupId, '', -1, [$this->topic => [0 => 25]], -1);

        self::assertSame(
            [$this->topic => [0 => 25]],
            $this->client->fetchGroupOffsets($coordinator, $groupId, [$this->topic => [0]]),
            'require_stable = false answers the last commit'
        );
        self::assertSame(
            [$this->topic => [0 => 25]],
            $this->client->fetchGroupOffsets($coordinator, $groupId, [$this->topic => [0]], true),
            'and require_stable = true answers the very same offset'
        );
    }

    /**
     * A transactional offset commit that has not ended makes the same request answer the 88, and only that one
     */
    public function testAPendingTransactionalOffsetIsAnsweredWithTheEightyEight(): void
    {
        $groupId     = self::uniqueGroupName();
        $coordinator = $this->client->getGroupCoordinator($groupId);

        $this->client->commitGroupOffsets($coordinator, $groupId, '', -1, [$this->topic => [0 => 25]], -1);

        $manager = $this->beginTransactionWithOffsets($groupId, 42);

        self::assertSame(
            [$this->topic => [0 => 25]],
            $this->client->fetchGroupOffsets($coordinator, $groupId, [$this->topic => [0]]),
            'without the flag the coordinator answers the last STABLE offset, not the pending 42'
        );

        try {
            $this->client->fetchGroupOffsets($coordinator, $groupId, [$this->topic => [0]], true);
            self::fail('A pending transactional offset commit has to be answered with the 88 of KIP-447');
        } catch (UnstableOffsetCommitException $exception) {
            self::assertSame(
                KafkaException::UNSTABLE_OFFSET_COMMIT,
                $exception->getCode(),
                'the retriable code a client waits out'
            );
            self::assertSame($this->topic, $exception->getContext()['topic'] ?? null);
        }

        $manager->commitTransaction();
        $this->openTransaction = null;

        self::assertSame(
            42,
            $this->awaitStableOffset($coordinator, $groupId),
            'and the very same request answers the offset of the transaction once it has committed'
        );
    }

    /**
     * The flag belongs to version 7 alone: the version below has no way to ask and no way to be refused
     */
    public function testTheVersionBelowSevenAnswersTheStableOffsetWithoutBeingAsked(): void
    {
        $groupId     = self::uniqueGroupName();
        $coordinator = $this->client->getGroupCoordinator($groupId);

        $this->client->commitGroupOffsets($coordinator, $groupId, '', -1, [$this->topic => [0 => 25]], -1);

        $manager = $this->beginTransactionWithOffsets($groupId, 42);
        $stream  = $coordinator->getConnection($this->configuration());

        new OffsetFetchRequestV6($groupId, [$this->topic => [0]], self::CLIENT_ID, 1201)->writeTo($stream);
        $answer = OffsetFetchResponseV6::unpack($stream);

        $partition = $answer->topics[$this->topic]->partitions[0];

        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode, 'a version 6 is never answered the 88');
        self::assertSame(25, $partition->offset, 'it reads the last stable offset and says nothing about the rest');

        $manager->abortTransaction();
        $this->openTransaction = null;
    }

    /**
     * A read-committed consumer starts from a stable offset; the 88 is waited out inside `poll()`
     */
    public function testAReadCommittedConsumerWaitsForTheTransactionToEnd(): void
    {
        $groupId     = self::uniqueGroupName();
        $coordinator = $this->client->getGroupCoordinator($groupId);

        $this->client->commitGroupOffsets($coordinator, $groupId, '', -1, [$this->topic => [0 => 0]], -1);

        $manager = $this->beginTransactionWithOffsets($groupId, 7);
        $manager->commitTransaction();
        $this->openTransaction = null;

        self::assertSame(7, $this->awaitStableOffset($coordinator, $groupId));

        $consumer = new KafkaConsumer([
            ConsumerConfig::GROUP_ID        => $groupId,
            ConsumerConfig::ISOLATION_LEVEL => ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED,
        ] + $this->configuration());

        $consumer->assign([$this->topic => [0]]);

        self::assertSame(
            7,
            $consumer->position($this->topic, 0),
            'the position of a read-committed consumer is the offset of the committed transaction'
        );
        self::assertSame([$this->topic => [0 => 7]], $consumer->committed([$this->topic => [0]]));
    }

    /**
     * Opens a transaction that commits the given offset of the topic under test for the group, without ending it
     */
    private function beginTransactionWithOffsets(string $groupId, int $offset): TransactionManager
    {
        $manager = new TransactionManager(
            $this->client,
            self::uniqueTopicName('t3-25-stable-tx'),
            60000,
            $this->configuration()
        );

        $manager->initTransactions();
        $manager->beginTransaction();
        $manager->sendOffsetsToTransaction([$this->topic => [0 => $offset]], $groupId);

        return $this->openTransaction = $manager;
    }

    /**
     * Reads the committed offset with `require_stable` until the coordinator has stopped answering the 88
     */
    private function awaitStableOffset(Node $coordinator, string $groupId): int
    {
        $deadline = microtime(true) + 30.0;
        do {
            try {
                return $this->client->fetchGroupOffsets($coordinator, $groupId, [$this->topic => [0]], true)
                    [$this->topic][0];
            } catch (UnstableOffsetCommitException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
            }
        } while (true);
    }

    /**
     * Waits until the fresh partition has a leader that answers, which a brand new topic needs a moment for
     */
    private function awaitTopic(string $topic): void
    {
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            try {
                $this->cluster()->reload();
                $this->admin->listOffsets([$topic => [0]], OffsetsRequest::LATEST);

                return;
            } catch (KafkaException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
            }
        } while (true);
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

            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * Builds a consumer group name that is unique for this test run
     */
    private static function uniqueGroupName(): string
    {
        return 't3-25-stable-group-' . bin2hex(random_bytes(6));
    }
}
