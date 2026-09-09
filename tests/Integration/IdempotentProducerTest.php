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
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\InvalidTxnTimeoutException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\OutOfOrderSequenceException;
use Protocol\Kafka\Common\Errors\ProducerFencedException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\InitProducerIdRequest;
use Protocol\Kafka\Protocol\Request\InitProducerIdResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * Exercises the idempotent producer of KIP-98 against a real Kafka 0.11.0.3 broker.
 *
 * Two things are verified here and nowhere else: that `InitProducerId` (key 22) really hands out what the protocol
 * document says it does - a fresh id with the epoch 0 for a null transactional id, the same id with a bumped epoch
 * for a real one - and that the **broker** deduplicates what this client stamps onto its batches. The second half
 * is the whole point of the guarantee, and it can only be seen against a log: a batch that is sent twice has to
 * come back with the offset of the first append and must not appear twice in the partition.
 *
 * @see docs/protocol/0.11.0.md, sections "InitProducerId API (key 22, v0)" and "The idempotent producer"
 */
#[CoversClass(Client::class)]
#[CoversClass(TransactionManager::class)]
#[CoversClass(ProducerIdAndEpoch::class)]
#[CoversClass(InitProducerIdRequest::class)]
#[CoversClass(InitProducerIdResponse::class)]
#[CoversClass(KafkaProducer::class)]
#[CoversClass(ProducerConfig::class)]
final class IdempotentProducerTest extends IntegrationTestCase
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

    public function testEveryRequestWithoutATransactionalIdAnswersANewProducerIdWithTheEpochZero(): void
    {
        $first  = $this->client->initProducerId();
        $second = $this->client->initProducerId();

        self::assertTrue($first->isValid());
        self::assertSame(0, $first->epoch, 'a producer id without a transactional id always has the epoch 0');
        self::assertSame(0, $second->epoch);
        self::assertNotSame(
            $first->producerId,
            $second->producerId,
            'there is no way to ask for the producer id of a previous session again'
        );
    }

    public function testATransactionTimeoutIsNotEvenLookedAtWithoutATransactionalId(): void
    {
        // `handleInitProducerId` returns from its null branch before it validates the timeout, so a value that a
        // transactional producer is refused for is answered with a perfectly normal producer id here
        $producerIdAndEpoch = $this->client->initProducerId(null, 999999999);

        self::assertTrue($producerIdAndEpoch->isValid());
        self::assertSame(0, $producerIdAndEpoch->epoch);
    }

    public function testATransactionalIdKeepsItsProducerIdAndBumpsItsEpoch(): void
    {
        $transactionalId = self::uniqueTopicName('t7-idempotent-txn');

        $first  = $this->client->initProducerId($transactionalId);
        $second = $this->client->initProducerId($transactionalId);

        self::assertSame(
            $first->producerId,
            $second->producerId,
            '`__transaction_state` holds one producer id per transactional id'
        );
        self::assertSame($first->epoch + 1, $second->epoch, 'the epoch is bumped, which fences the producer before');
    }

    public function testATransactionTimeoutAboveTheBrokerMaximumIsRefused(): void
    {
        $transactionalId = self::uniqueTopicName('t7-idempotent-timeout');

        $this->expectException(InvalidTxnTimeoutException::class);

        // `transaction.max.timeout.ms` is 900000 by default
        $this->client->initProducerId($transactionalId, 999999999);
    }

    public function testTheEmptyStringIsNotATransactionalId(): void
    {
        $this->expectException(InvalidRequestException::class);

        $this->client->initProducerId('');
    }

    public function testAnIdempotentProducerNumbersTheBatchesOfEveryPartitionOnItsOwn(): void
    {
        $topic     = $this->topic('sequences');
        $manager   = new TransactionManager($this->client);
        $partition = new TopicPartition($topic, 0);

        $first  = $this->produce($topic, $manager, ['one', 'two']);
        $second = $this->produce($topic, $manager, ['three']);

        self::assertSame(0, $first[$topic][0]->baseOffset);
        self::assertSame(2, $second[$topic][0]->baseOffset);
        self::assertSame(3, $manager->sequenceNumber($partition), 'three records were acknowledged');
        self::assertTrue($manager->getProducerIdAndEpoch()->isValid());
        self::assertSame(3, $this->latestOffset($topic), 'and three records are in the log');
    }

    public function testABatchThatIsSentTwiceIsAppendedOnceAndAnsweredWithTheOriginalOffset(): void
    {
        $topic   = $this->topic('duplicate');
        $manager = new TransactionManager($this->client);
        $records = $this->records(['exactly', 'once']);

        $first = $this->client->produce([$topic => [0 => $records]], $manager);

        // The very batch again, as a producer whose acknowledgement got lost would send it: the same producer id,
        // the same epoch and the same sequence numbers, because nothing told this producer that it was appended
        $retry = new TransactionManager($this->client);
        $retry->setProducerIdAndEpoch($manager->getProducerIdAndEpoch());

        $second = $this->client->produce([$topic => [0 => $records]], $retry);

        self::assertSame(0, $first[$topic][0]->baseOffset);
        self::assertSame(
            $first[$topic][0]->baseOffset,
            $second[$topic][0]->baseOffset,
            'the broker answers a duplicate with the offset of the original append'
        );
        self::assertSame(2, $this->latestOffset($topic), 'and the records are in the log exactly once');
        self::assertNotSame(
            -1,
            $second[$topic][0]->logAppendTime,
            'the LogAppendTime of a CreateTime topic is -1 for a real append and the stored timestamp for a duplicate'
        );
    }

    public function testAGapInTheSequenceIsRefusedAndThrowsTheProducerIdAway(): void
    {
        $topic   = $this->topic('gap');
        $manager = new TransactionManager($this->client);
        $this->produce($topic, $manager, ['first']);

        $producerId = $manager->getProducerIdAndEpoch()->producerId;
        // Nothing a producer would ever do - it is what a lost batch of a producer with more than one request in
        // flight looks like to the broker, and the reason the guarantee needs a single request at a time
        $manager->incrementSequenceNumber(new TopicPartition($topic, 0), 8);

        try {
            $this->produce($topic, $manager, ['out of order']);
            self::fail('a sequence with a gap in it has to be reported');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(OutOfOrderSequenceException::class, $exception->getExceptions()[$topic][0]);
        }

        self::assertFalse($manager->hasProducerId(), 'the idempotent producer starts over');
        self::assertFalse($manager->hasFatalError(), 'an out of order sequence is not fatal without a transaction');
        self::assertSame(1, $this->latestOffset($topic), 'the refused batch was not appended');

        // The next batch asks for a new producer id, starts at the sequence 0 again and is accepted
        $this->produce($topic, $manager, ['after the gap']);

        self::assertNotSame($producerId, $manager->getProducerIdAndEpoch()->producerId);
        self::assertSame(2, $this->latestOffset($topic));
    }

    public function testADuplicateOfABatchThatIsNoLongerTheLastOneIsAnOutOfOrderSequence(): void
    {
        // A 0.11.0.3 broker remembers exactly one batch per producer id and partition, so its duplicate check only
        // ever recognises the batch it appended last; an older one is indistinguishable from a gap for it
        $topic   = $this->topic('old-duplicate');
        $manager = new TransactionManager($this->client);
        $records = $this->records(['first']);

        $this->client->produce([$topic => [0 => $records]], $manager);
        $this->produce($topic, $manager, ['second']);

        $retry = new TransactionManager($this->client);
        $retry->setProducerIdAndEpoch($manager->getProducerIdAndEpoch());

        try {
            $this->client->produce([$topic => [0 => $records]], $retry);
            self::fail('a duplicate of a batch the broker no longer remembers has to be reported');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(OutOfOrderSequenceException::class, $exception->getExceptions()[$topic][0]);
        }

        self::assertSame(2, $this->latestOffset($topic));
    }

    public function testAnOldEpochIsFencedAndFinishesTheProducerForGood(): void
    {
        $topic           = $this->topic('fenced');
        $transactionalId = self::uniqueTopicName('t7-idempotent-fence');

        // Two epochs of one producer id: the produce path never asks the transaction coordinator anything, so the
        // fencing that is observed here is the one the LOG does, with plain idempotent batches
        $old = $this->client->initProducerId($transactionalId);
        $new = $this->client->initProducerId($transactionalId);

        $current = new TransactionManager($this->client);
        $current->setProducerIdAndEpoch($new);
        $this->client->produce([$topic => [0 => $this->records(['the current epoch'])]], $current);

        $fenced = new TransactionManager($this->client);
        $fenced->setProducerIdAndEpoch($old);

        try {
            $this->client->produce([$topic => [0 => $this->records(['the old epoch'])]], $fenced);
            self::fail('a batch of an old epoch has to be reported');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(ProducerFencedException::class, $exception->getExceptions()[$topic][0]);
        }

        self::assertTrue($fenced->hasFatalError(), 'a fenced producer never recovers');
        self::assertSame(1, $this->latestOffset($topic), 'and nothing of it was appended');

        $this->expectException(ProducerFencedException::class);

        $this->client->produce([$topic => [0 => $this->records(['never sent'])]], $fenced);
    }

    public function testAProducerWithIdempotenceWritesEveryRecordExactlyOnce(): void
    {
        $topic    = $this->topic('producer');
        // Deliberately without the defaults of the producer: `enable.idempotence` only overrides `acks` and
        // `retries` when the *caller* left them alone, exactly as the Java producer reads its originals
        $producer = new KafkaProducer([
            ProducerConfig::ENABLE_IDEMPOTENCE      => true,
            ProducerConfig::BATCH_SIZE              => 1024 * 1024,
            ProducerConfig::CLIENT_ID               => 't7-idempotent',
            ProducerConfig::BOOTSTRAP_SERVERS       => ['tcp://' . self::firstBootstrapServer()],
            ProducerConfig::REQUEST_TIMEOUT_MS      => 40000,
            ProducerConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ]);

        for ($index = 0; $index < 10; $index++) {
            $producer->send($topic, Record::fromValue("t7-idempotent-{$index}"), 0);
        }
        $producer->flush();

        self::assertSame(10, $this->latestOffset($topic));
        self::assertSame(
            array_map(static fn(int $index): string => "t7-idempotent-{$index}", range(0, 9)),
            array_map(
                static fn(Record $record): ?string => $record->value,
                $this->client->fetch([$topic => [0 => 0]], 5000)[$topic][0]
            )
        );
    }

    /**
     * Produces the given values as one batch of the first partition, with the producer state of the manager
     *
     * @param list<string> $values
     *
     * @return array<string, array<int, \Protocol\Kafka\Protocol\Data\ProduceResponsePartition>>
     */
    private function produce(string $topic, TransactionManager $manager, array $values): array
    {
        return $this->client->produce([$topic => [0 => $this->records($values)]], $manager);
    }

    /**
     * Builds the records of a batch, all of them stamped with the same `CreateTime`.
     *
     * The timestamp is the clock of the test rather than a fixed one, because a time-based retention deletes a
     * segment by the largest timestamp it holds; and it is the *same* for every call with the same values, so that
     * a batch that is built twice really is the same batch, byte for byte.
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
        $topic                 = self::uniqueTopicName("t7-idempotent-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));

        // A fresh topic answers 3, 5 or 6 for a moment, until its leader is elected and known to this client; the
        // probe has to be a read, because every write of it would end up in the offsets the tests assert on
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            try {
                $this->cluster->reload();
                $this->latestOffset($topic);

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
     * Returns the offset the next produced record will get
     */
    private function latestOffset(string $topic): int
    {
        return $this->admin->listOffsets([$topic => [0]], OffsetsRequest::LATEST)[$topic][0];
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
            ClientConfig::CLIENT_ID                 => 't7-idempotent',
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }
}
