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
use Protocol\Kafka\Common\Errors\UnknownProducerIdException;
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
 * Exercises the idempotent producer of KIP-98 against a real Kafka 2.8.2 broker.
 *
 * Three things are verified here and nowhere else: that `InitProducerId` (key 22, **version 1** since Kafka 2.0)
 * really hands out what the protocol document says it does - a fresh id with the epoch 0 for a null transactional
 * id, the same id with a bumped epoch for a real one - that the **broker** deduplicates what this client stamps
 * onto its batches, and how a **2.x** broker differs from the 1.1.1 one of the line below: it still remembers the
 * last **five** batches of a producer and partition, but the error code **59** `UnknownProducerId` of Kafka 1.0 is
 * gone from its produce path. `ProducerAppendInfo.checkSequence` @ 2.8.2 accepts any sequence of a producer it
 * holds no state of, so a batch whose records were deleted under it is simply appended and the repair path of this
 * client - which a 1.1.1 broker still needs - is never entered here.
 *
 * The deduplication is the whole point of the guarantee, and it can only be seen against a log: a batch that is
 * sent twice has to come back with the offset of the first append and must not appear twice in the partition.
 *
 * @see docs/protocol/2.8.md, sections "InitProducerId API (key 22, v0 to v4)" and "The idempotent producer"
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

    /**
     * A Kafka 1.x broker remembers the last **five** batches of a producer id, not the one 0.11 remembered
     *
     * `ProducerStateEntry.NumBatchesToRetain = 5` @ 1.0.2 and 1.1.1, where the same constant of 0.11.0.3 kept a
     * single batch. The consequence is visible to a client: a duplicate of a batch that is no longer the last one
     * is answered as the **original append** - the same base offset, the stored timestamp - as long as it is one of
     * the last five, where a 0.11 broker answered 45 (`OutOfOrderSequence`) for anything but the very last batch.
     * The 0.11 quirk "a duplicate of an older batch is 45" is therefore gone, and a producer whose acknowledgement
     * of several batches in a row was lost is no longer fatally out of sequence.
     *
     * Every one of the five is re-sent here, not only the oldest of them, because "five" is the number the whole
     * decision of {@see TransactionManager::canRetryBatch()} rests on.
     */
    public function testADuplicateOfAnyOfTheLastFiveBatchesIsAnsweredAsTheOriginalAppend(): void
    {
        $topic     = $this->topic('old-duplicate');
        $manager   = new TransactionManager($this->client);
        $partition = new TopicPartition($topic, 0);

        /** @var list<array{list<Record>, int}> $batches */
        $batches = [];
        foreach (['first', 'second', 'third', 'fourth', 'fifth'] as $value) {
            $records   = $this->records([$value]);
            $answer    = $this->client->produce([$topic => [0 => $records]], $manager);
            $batches[] = [$records, $answer[$topic][0]->baseOffset];
        }

        self::assertSame([0, 1, 2, 3, 4], array_column($batches, 1), 'one record per batch, five batches');
        self::assertSame(5, $manager->sequenceNumber($partition));
        self::assertSame(4, $manager->lastAckedOffset($partition));

        foreach ($batches as $index => [$records, $baseOffset]) {
            // The batch as the producer that lost the acknowledgement would send it again: the same producer id,
            // the same epoch and the same sequence numbers, because nothing told that producer it was appended
            $retry = new TransactionManager($this->client);
            $retry->setProducerIdAndEpoch($manager->getProducerIdAndEpoch());
            $retry->incrementSequenceNumber($partition, $index);

            $duplicate = $this->client->produce([$topic => [0 => $records]], $retry);

            self::assertSame(
                $baseOffset,
                $duplicate[$topic][0]->baseOffset,
                "the broker still has the entry of the batch {$index} batches from the start of the window"
            );
            self::assertNotSame(
                -1,
                $duplicate[$topic][0]->logAppendTime,
                'and gives the duplicate away with the stored timestamp of the original append'
            );
            self::assertSame($index + 1, $retry->sequenceNumber($partition), 'the duplicate counts as an append');
            self::assertSame(
                $baseOffset,
                $retry->lastAckedOffset($partition),
                'and the offset of that append is what the producer remembers for the partition'
            );
        }

        self::assertSame(5, $this->latestOffset($topic), 'and nothing at all was appended');
    }

    /**
     * The sixth batch pushes the first one out of the window, and only then is its duplicate an out of order sequence
     */
    public function testADuplicateOfABatchBelowTheWindowIsAnOutOfOrderSequence(): void
    {
        $topic   = $this->topic('evicted-duplicate');
        $manager = new TransactionManager($this->client);
        $records = $this->records(['first']);

        $this->client->produce([$topic => [0 => $records]], $manager);
        foreach (['second', 'third', 'fourth', 'fifth', 'sixth'] as $value) {
            $this->produce($topic, $manager, [$value]);
        }

        $retry = new TransactionManager($this->client);
        $retry->setProducerIdAndEpoch($manager->getProducerIdAndEpoch());

        try {
            $this->client->produce([$topic => [0 => $records]], $retry);
            self::fail('a duplicate of a batch the broker no longer remembers has to be reported');
        } catch (TopicPartitionRequestException $exception) {
            $error = $exception->getExceptions()[$topic][0];

            self::assertInstanceOf(OutOfOrderSequenceException::class, $error);
            self::assertNotInstanceOf(
                UnknownProducerIdException::class,
                $error,
                'the producer id is perfectly well known, only the batch is too old'
            );
            self::assertSame(0, $error->getContext()['logStartOffset'], 'and nothing of the log was deleted');
        }

        self::assertSame(6, $this->latestOffset($topic));
    }

    /**
     * A 2.8.2 broker accepts the next batch of a producer whose records were all deleted, sequence and all
     *
     * On a 1.1.1 broker this was the **59** `UnknownProducerId` of Kafka 1.0: `ProducerStateManager.truncateHead()`
     * dropped the entry of every producer whose last record fell below the new log start offset, so the next batch
     * met a broker without any state of that producer, and a first sequence other than 0 was refused - which this
     * client repaired by numbering the partition from 0 again.
     *
     * `ProducerAppendInfo.checkSequence()` @ 2.8.2 does not throw that exception any more (the class does not even
     * mention it): "If there is no current producer epoch (possibly because all producer records have been deleted
     * due to retention or the DeleteRecords API) accept writes with any sequence number". The batch is therefore
     * appended as it was sent, the producer keeps counting where it stood, and the repair path of this client -
     * which is still needed for a broker of the lines below - is never entered on a 2.8.2 broker.
     */
    public function testAProducerWhoseRecordsWereAllDeletedKeepsCountingOnATwoEightBroker(): void
    {
        $topic     = $this->topic('deleted-records');
        $manager   = new TransactionManager($this->client);
        $partition = new TopicPartition($topic, 0);

        $first = $this->produce($topic, $manager, ['one', 'two']);

        self::assertSame(0, $first[$topic][0]->baseOffset);
        self::assertSame(0, $first[$topic][0]->logStartOffset, 'an untouched log starts at 0');
        self::assertSame(1, $manager->lastAckedOffset($partition));

        $producerId = $manager->getProducerIdAndEpoch()->producerId;

        // Everything the producer wrote, and with the last of those records the state the broker held it by
        self::assertSame(2, $this->admin->deleteRecords([$topic => [0 => 2]])[$topic][0]->lowWatermark);

        $second = $this->produce($topic, $manager, ['after the deletion']);

        self::assertSame(2, $second[$topic][0]->baseOffset, 'the batch was appended at the new start of the log');
        self::assertSame(2, $second[$topic][0]->logStartOffset, 'and the answer reports that new start');
        self::assertSame(
            $producerId,
            $manager->getProducerIdAndEpoch()->producerId,
            'the producer id survives, and so does the numbering of the partition'
        );
        self::assertSame(
            3,
            $manager->sequenceNumber($partition),
            'the batch carried the sequence 2 and was accepted, where a 1.1.1 broker answered 59 and this client '
            . 'started the partition over at 0'
        );
        self::assertSame(2, $manager->lastAckedOffset($partition));
        self::assertSame(3, $this->latestOffset($topic));
    }

    /**
     * A first batch of an unknown producer id that does not start at 0 is **accepted** by a 2.8.2 broker
     *
     * `ProducerAppendInfo.checkSequence` @ 1.1.1 threw `UnknownProducerIdException` - the error code **59** - when
     * the log held no entry of the producer id at all (`NO_PRODUCER_EPOCH`) and the batch did not start at the
     * sequence 0; 0.11.0.3, which had no such error code, answered those with `OutOfOrderSequenceException` (45).
     *
     * A 2.8.2 broker answers neither: the very same branch now reads "If there is no current producer epoch
     * (possibly because all producer records have been deleted due to retention or the DeleteRecords API) accept
     * writes with any sequence number", so the batch is appended with the sequence it carries and the producer
     * simply carries on from there. The error code 59 is gone from the produce path of this release - what is left
     * of the idempotent guarantee is the 45 of a producer whose state the broker DOES hold (see the test above)
     * and the 47 of a fenced epoch.
     */
    public function testAFirstBatchOfAnUnknownProducerIdThatDoesNotStartAtZeroIsAccepted(): void
    {
        $topic     = $this->topic('unknown-id');
        $manager   = new TransactionManager($this->client);
        $partition = new TopicPartition($topic, 0);
        $manager->maybeInitProducerId();

        // Nothing a producer would ever do - it is what the first batch of a producer whose acknowledgement of an
        // earlier one was lost looks like to a broker that never saw that earlier batch
        $manager->incrementSequenceNumber($partition, 5);
        $manager->updateLastAckedOffset($partition, 0, 1);

        $appended = $this->produce($topic, $manager, ['not the first sequence']);

        self::assertSame(0, $appended[$topic][0]->baseOffset, 'the batch is the first record of the partition');
        self::assertSame(0, $appended[$topic][0]->logStartOffset);
        self::assertSame(6, $manager->sequenceNumber($partition), 'and the producer counts on from the 5 it claimed');
        self::assertTrue($manager->hasProducerId(), 'nothing was refused, so the producer id stays');
        self::assertFalse($manager->hasFatalError());
        self::assertSame(1, $this->latestOffset($topic));
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
