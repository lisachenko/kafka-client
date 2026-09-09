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

namespace Protocol\Kafka\Tests\Unit\Producer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\MessageTooLargeException;
use Protocol\Kafka\Common\Errors\NotLeaderForPartitionException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\ClusterFixture;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\FakeClient;
use Protocol\Kafka\Tests\Unit\Producer\Fixture\TestKafkaProducer;

/**
 * Verifies the buffering, the batching and the promise handling of the producer against a scripted client.
 *
 * The cluster of these tests is read out of a metadata cache file, {@see ClusterFixture}, and every produce request
 * is answered by a {@see FakeClient}, so that the behaviour of the producer can be observed without a broker.
 */
#[CoversClass(KafkaProducer::class)]
#[CoversClass(ProducerConfig::class)]
final class KafkaProducerTest extends TestCase
{
    /**
     * Topic that every test of this class produces to
     */
    private const string TOPIC = 'producer-topic';

    /**
     * Cluster of one broker that leads the three partitions of the topic
     */
    private Cluster $cluster;

    /**
     * Configuration that points the producer at that cluster
     *
     * @var array<string, mixed>
     */
    private array $clusterConfiguration;

    /**
     * Client of the producer that is currently under test, for the behaviours that answer with a partial result
     */
    private FakeClient $fakeClient;

    protected function setUp(): void
    {
        $this->cluster              = ClusterFixture::withPartitions([self::TOPIC => [0 => 1, 1 => 1, 2 => 1]]);
        $this->clusterConfiguration = ClusterFixture::configuration(ClusterFixture::lastCacheFile());
    }

    protected function tearDown(): void
    {
        ClusterFixture::cleanUp();
    }

    public function testEveryRecordIsSentOnItsOwnWhenBatchingIsDisabled(): void
    {
        [$producer, $client] = $this->producer();

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'first'));
        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'second'));

        // batch.size defaults to 0, which sends every record as soon as it was handed over
        self::assertCount(2, $client->produceCalls);
    }

    public function testRecordsAreBufferedUntilTheBatchIsFull(): void
    {
        [$producer, $client] = $this->producer([ProducerConfig::BATCH_SIZE => 3 * $this->recordSize('key-0', 'x')]);

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'x'));
        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'x'));
        self::assertSame([], $client->produceCalls, 'A batch that is not full yet is not sent');

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'x'));
        self::assertCount(1, $client->produceCalls, 'A full batch is sent in a single request');
        self::assertCount(3, $client->receivedRecords());
    }

    public function testABatchHoldsTheRecordsOfEveryTopicPartitionItSpans(): void
    {
        [$producer, $client] = $this->producer([ProducerConfig::BATCH_SIZE => 1024 * 1024]);

        // 'key-0' hashes to partition 1, 'key-2' to partition 2 in a topic of three partitions
        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'a'));
        $producer->send(self::TOPIC, Record::fromKeyValue('key-2', 'b'));
        $producer->send(self::TOPIC, Record::fromValue('c'), 0);
        $producer->flush();

        self::assertCount(1, $client->produceCalls);
        $request = $client->produceCalls[0];
        self::assertSame([self::TOPIC], array_keys($request));
        self::assertSame([1, 2, 0], array_keys($request[self::TOPIC]));
        self::assertCount(1, $request[self::TOPIC][1]);
        self::assertSame('a', $request[self::TOPIC][1][0]->value);
    }

    public function testABatchIsHeldBackUntilTheLingerTimeExpired(): void
    {
        [$producer, $client] = $this->producer([
            ProducerConfig::BATCH_SIZE => 1024 * 1024,
            ProducerConfig::LINGER_MS  => 40,
        ]);

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'first'));
        self::assertSame([], $client->produceCalls, 'The batch waits for the records that may still come');

        usleep(60000);
        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'second'));

        self::assertCount(1, $client->produceCalls, 'A batch that lingered long enough is sent');
        self::assertCount(2, $client->receivedRecords(), 'Both records of the batch are sent together');
    }

    public function testAFullBatchIsSentBeforeTheLingerTimeExpires(): void
    {
        [$producer, $client] = $this->producer([
            ProducerConfig::BATCH_SIZE => 2 * $this->recordSize('key-0', 'value'),
            ProducerConfig::LINGER_MS  => 60000,
        ]);

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'));
        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'));

        self::assertCount(1, $client->produceCalls);
    }

    public function testFlushSendsWhateverIsBuffered(): void
    {
        [$producer, $client] = $this->producer([
            ProducerConfig::BATCH_SIZE => 1024 * 1024,
            ProducerConfig::LINGER_MS  => 60000,
        ]);

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'));
        self::assertSame([], $client->produceCalls);

        $producer->flush();
        self::assertCount(1, $client->produceCalls);

        // A second flush has nothing left to send
        $producer->flush();
        self::assertCount(1, $client->produceCalls);
    }

    public function testThePromiseIsResolvedWithTheMetadataOfTheAcknowledgedBatch(): void
    {
        [$producer] = $this->producer([ProducerConfig::BATCH_SIZE => 1024 * 1024]);

        $metadata = null;
        $producer
            ->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'))
            ->then(static function (RecordMetadata $recordMetadata) use (&$metadata): void {
                $metadata = $recordMetadata;
            });

        self::assertNull($metadata, 'The promise is settled by the request, not by the buffering');

        $producer->flush();

        self::assertInstanceOf(RecordMetadata::class, $metadata);
        self::assertSame(self::TOPIC, $metadata->topic);
        self::assertSame(1, $metadata->partition);
        self::assertSame(0, $metadata->offset);
        self::assertSame(self::TOPIC . '-1@0', (string) $metadata);
    }

    public function testThePromiseCarriesTheThrottleTimeOfTheAnswer(): void
    {
        // A broker with a `producer_byte_rate` quota for this client id appends the batch and delays its answer
        [$producer] = $this->producer(
            [ProducerConfig::BATCH_SIZE => 1024 * 1024],
            [fn(array $topicPartitionMessages): array => $this->fakeClient->acknowledge($topicPartitionMessages, 793)]
        );

        $metadata = null;
        $producer
            ->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'))
            ->then(static function (RecordMetadata $recordMetadata) use (&$metadata): void {
                $metadata = $recordMetadata;
            });
        $producer->flush();

        self::assertInstanceOf(RecordMetadata::class, $metadata);
        self::assertSame(793, $metadata->throttleTimeMs, 'the delay of the answer reaches the caller of send()');
        self::assertNotNull($metadata->timestamp, 'the CreateTime the producer stamped on the batch');
    }

    public function testAnUnthrottledAnswerReportsNoDelay(): void
    {
        [$producer] = $this->producer([ProducerConfig::BATCH_SIZE => 1024 * 1024]);

        $metadata = null;
        $producer
            ->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'))
            ->then(static function (RecordMetadata $recordMetadata) use (&$metadata): void {
                $metadata = $recordMetadata;
            });
        $producer->flush();

        self::assertInstanceOf(RecordMetadata::class, $metadata);
        self::assertSame(0, $metadata->throttleTimeMs);
    }

    public function testEveryRecordOfABatchGetsTheMetadataOfThatBatch(): void
    {
        [$producer] = $this->producer([ProducerConfig::BATCH_SIZE => 1024 * 1024]);

        $offsets = [];
        $collect = static function (RecordMetadata $metadata) use (&$offsets): void {
            $offsets[] = $metadata->offset;
        };

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'a'))->then($collect);
        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'b'))->then($collect);
        $producer->flush();

        // The broker answers one base offset per topic-partition, which is the offset of the first record of it
        self::assertSame([0, 0], $offsets);
    }

    public function testThePromiseOfAFailedPartitionIsRejectedWithItsError(): void
    {
        $failure = new InvalidTopicException(['topic' => self::TOPIC]);

        [$producer, $client] = $this->producer(
            [ProducerConfig::BATCH_SIZE => 1024 * 1024],
            [
                function (array $topicPartitionMessages) use ($failure): array {
                    $result = $this->fakeClient->acknowledge([self::TOPIC => [1 => $topicPartitionMessages[self::TOPIC][1]]]);

                    throw new TopicPartitionRequestException($result, [self::TOPIC => [2 => $failure]]);
                },
            ]
        );

        $resolved = $rejected = null;
        $producer
            ->send(self::TOPIC, Record::fromKeyValue('key-0', 'a'))
            ->then(static function (RecordMetadata $metadata) use (&$resolved): void {
                $resolved = $metadata;
            });
        $producer
            ->send(self::TOPIC, Record::fromKeyValue('key-2', 'b'))
            ->then(null, static function (\Throwable $error) use (&$rejected): void {
                $rejected = $error;
            });

        $producer->flush();

        self::assertInstanceOf(RecordMetadata::class, $resolved, 'The partition that succeeded is resolved');
        self::assertSame(1, $resolved->partition);
        self::assertSame($failure, $rejected, 'The partition that failed is rejected with its own error');
        self::assertCount(1, $client->produceCalls, 'A non-retriable error is not retried');
    }

    public function testAWholeRequestErrorRejectsEveryPartitionOfTheBatch(): void
    {
        // The client of this branch reports the first error of a request instead of a partial result
        $failure = new InvalidTopicException(['topic' => self::TOPIC]);

        [$producer] = $this->producer(
            [ProducerConfig::BATCH_SIZE => 1024 * 1024],
            [static fn(): array => throw $failure]
        );

        $errors = [];
        $catch  = static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        };

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'a'))->then(null, $catch);
        $producer->send(self::TOPIC, Record::fromKeyValue('key-2', 'b'))->then(null, $catch);
        $producer->flush();

        self::assertSame([$failure, $failure], $errors);
    }

    public function testTheProducerLeavesTheRetryOfAFailedBatchToTheClient(): void
    {
        $failure = new NotLeaderForPartitionException(['topic' => self::TOPIC]);

        [$producer, $client] = $this->producer(
            [ProducerConfig::BATCH_SIZE => 1024 * 1024, ProducerConfig::RETRIES => 2],
            array_fill(0, 5, static fn(): array => throw new TopicPartitionRequestException(
                [],
                [self::TOPIC => [1 => $failure]]
            ))
        );

        $rejected = null;
        $producer
            ->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'))
            ->then(null, static function (\Throwable $error) use (&$rejected): void {
                $rejected = $error;
            });
        $producer->flush();

        // Client::produce() is the one that refreshes the metadata and sends the failed partitions again, up to
        // `retries` times; the producer does not add a second layer of retries on top of it
        self::assertCount(1, $client->produceCalls);
        self::assertSame($failure, $rejected, 'What the client gave up on fails the promise of its partition');

        // Nothing is left over for the next flush
        $producer->flush();
        self::assertCount(1, $client->produceCalls);
    }

    public function testTheRetryBudgetOfTheProducerIsTheOneOfItsClient(): void
    {
        // `retries` is a single option of the client configuration: the producer default of 0 replaces the default
        // of the general client configuration, and whatever is configured reaches the client that sends the batches
        self::assertSame(0, ProducerConfig::getDefaultConfiguration()[ClientConfig::RETRIES]);

        $producer = $this->configurationProbe([ProducerConfig::RETRIES => 4]);

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'));

        self::assertSame(4, $producer->clientConfiguration[ClientConfig::RETRIES]);
        self::assertSame(100, $producer->clientConfiguration[ClientConfig::RETRY_BACKOFF_MS]);
    }

    public function testAFireAndForgetSendIsAcknowledgedWithTheOffsetMinusOne(): void
    {
        [$producer, $client] = $this->producer(
            [ProducerConfig::ACKS => ProducerConfig::ACKS_NONE, ProducerConfig::BATCH_SIZE => 1024 * 1024],
            // A broker never answers a request with acks = 0, so the client hands back an empty result
            [static fn(): array => []]
        );

        $metadata = null;
        $producer
            ->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'))
            ->then(static function (RecordMetadata $recordMetadata) use (&$metadata): void {
                $metadata = $recordMetadata;
            });
        $producer->flush();

        self::assertCount(1, $client->produceCalls);
        self::assertInstanceOf(RecordMetadata::class, $metadata);
        self::assertSame(-1, $metadata->offset, 'The offset of a record that was never acknowledged is unknown');
        self::assertSame(0, $metadata->throttleTimeMs, 'A request without an answer reports no throttle time');
    }

    public function testARecordThatIsLargerThanTheRequestSizeIsRejected(): void
    {
        [$producer, $client] = $this->producer([ProducerConfig::MAX_REQUEST_SIZE => 64]);

        $this->expectException(MessageTooLargeException::class);

        try {
            $producer->send(self::TOPIC, Record::fromValue(str_repeat('a', 128)), 0);
        } finally {
            self::assertSame([], $client->produceCalls, 'The record never reaches the broker');
        }
    }

    public function testTheBufferIsSentBeforeItGrowsPastTheRequestSize(): void
    {
        $recordSize = $this->recordSize(null, str_repeat('a', 40));

        [$producer, $client] = $this->producer([
            ProducerConfig::BATCH_SIZE       => 1024 * 1024,
            ProducerConfig::MAX_REQUEST_SIZE => 2 * $recordSize + 1,
        ]);

        $producer->send(self::TOPIC, Record::fromValue(str_repeat('a', 40)), 0);
        $producer->send(self::TOPIC, Record::fromValue(str_repeat('a', 40)), 0);
        self::assertSame([], $client->produceCalls);

        // The third record does not fit into the request any more, so the two buffered ones are sent first
        $producer->send(self::TOPIC, Record::fromValue(str_repeat('a', 40)), 0);

        self::assertCount(1, $client->produceCalls);
        self::assertCount(2, $client->receivedRecords());
    }

    public function testTheDestructorSendsWhatIsStillBuffered(): void
    {
        [$producer, $client] = $this->producer([
            ProducerConfig::BATCH_SIZE => 1024 * 1024,
            ProducerConfig::LINGER_MS  => 60000,
        ]);

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'));
        self::assertSame([], $client->produceCalls);

        unset($producer);

        self::assertCount(1, $client->produceCalls, 'A producer that goes out of scope does not lose records');
    }

    public function testAnExplicitPartitionOverridesThePartitioner(): void
    {
        [$producer, $client] = $this->producer([ProducerConfig::BATCH_SIZE => 1024 * 1024]);

        // 'key-0' would be hashed to the partition 1
        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'), 2);
        $producer->flush();

        self::assertSame([2], array_keys($client->produceCalls[0][self::TOPIC]));
    }

    public function testThePartitionsOfATopicAreReadFromTheClusterMetadata(): void
    {
        [$producer] = $this->producer();

        $partitions = $producer->partitionsFor(self::TOPIC);

        self::assertCount(3, $partitions);
        self::assertSame([0, 1, 2], array_keys($partitions));
        self::assertSame(1, $partitions[0]->leader);
    }

    public function testAnUnusablePartitionerIsRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        new KafkaProducer([ProducerConfig::PARTITIONER_CLASS => \stdClass::class]);
    }

    public function testAnUnknownCompressionTypeIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('zstd');

        new KafkaProducer([ProducerConfig::COMPRESSION_TYPE => 'zstd']);
    }

    public function testEveryRecordIsStampedWithItsCreateTime(): void
    {
        [$producer] = $this->producer([ProducerConfig::BATCH_SIZE => 1024 * 1024]);
        $before     = (int) round(microtime(true) * 1000);

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'));
        $producer->send(self::TOPIC, Record::fromValue('another'));
        $producer->flush();

        $after   = (int) round(microtime(true) * 1000);
        $records = $this->fakeClient->receivedRecords();

        self::assertCount(2, $records);
        foreach ($records as $record) {
            self::assertNotNull($record->timestamp, 'the producer stamps the CreateTime of every record');
            self::assertGreaterThanOrEqual($before, $record->timestamp);
            self::assertLessThanOrEqual($after, $record->timestamp);
            self::assertSame(TimestampType::CREATE_TIME, $record->timestampType);
        }
    }

    public function testARecordThatAlreadyCarriesATimestampKeepsIt(): void
    {
        [$producer] = $this->producer([ProducerConfig::BATCH_SIZE => 1024 * 1024]);

        $producer->send(self::TOPIC, new Record('value', 'key-0', 0, null, 1489324800000));
        $producer->flush();

        $records = $this->fakeClient->receivedRecords();

        self::assertCount(1, $records);
        self::assertSame(1489324800000, $records[0]->timestamp);
    }

    public function testTheRecordMetadataCarriesTheCreateTimeOfTheBatch(): void
    {
        [$producer] = $this->producer([ProducerConfig::BATCH_SIZE => 1024 * 1024]);

        $metadata = null;
        $producer
            ->send(self::TOPIC, new Record('value', 'key-0', 0, null, 1489324800000))
            ->then(static function (RecordMetadata $recordMetadata) use (&$metadata): void {
                $metadata = $recordMetadata;
            });
        $producer->flush();

        self::assertInstanceOf(RecordMetadata::class, $metadata);
        self::assertSame(1489324800000, $metadata->timestamp, 'the CreateTime of the first record of the batch');
    }

    public function testTheRecordSizeOfMessageFormatV1CountsTheTimestampAsWell(): void
    {
        [$producer] = $this->producer([
            ProducerConfig::BATCH_SIZE       => 1024 * 1024,
            ProducerConfig::MAX_REQUEST_SIZE => MessageSet::ENTRY_OVERHEAD + Message::MIN_SIZE_V1 + 4,
        ]);

        // A record of five bytes fits into message format v0 but not into v1, which adds the eight bytes of the
        // timestamp to every message
        $this->expectException(MessageTooLargeException::class);

        $producer->send(self::TOPIC, Record::fromValue('value'));
    }

    public function testAnUnknownMessageFormatVersionIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('0.11.0');

        new KafkaProducer([ProducerConfig::MESSAGE_FORMAT_VERSION => '0.11.0']);
    }

    public function testTheCompressionTypeIsHandedToTheClientThatSendsTheBatches(): void
    {
        // A batch is compressed as a whole by the client that writes the message set of a topic-partition, see
        // ClientTest::testTheConfiguredCompressionTypeIsAppliedToTheWholeBatch()
        $producer = $this->configurationProbe([ProducerConfig::COMPRESSION_TYPE => 'snappy']);

        $producer->send(self::TOPIC, Record::fromKeyValue('key-0', 'value'));

        self::assertSame('snappy', $producer->clientConfiguration[ProducerConfig::COMPRESSION_TYPE]);
    }

    /**
     * Builds a producer of the fixture cluster whose requests are answered by a fake client
     *
     * @param array<string, mixed> $configuration Producer options on top of the defaults
     * @param list<\Closure>       $behaviours    Answer of every produce request, in order
     *
     * @return array{0: KafkaProducer, 1: FakeClient}
     */
    private function producer(array $configuration = [], array $behaviours = []): array
    {
        $configuration += $this->clusterConfiguration;
        $client = new FakeClient($this->cluster, $configuration, $behaviours);

        // The behaviours are bound to the test, so that they can build their answer with the client itself
        $this->fakeClient = $client;

        return [new TestKafkaProducer($configuration, $client), $client];
    }

    /**
     * Builds a producer that keeps the configuration it handed to its client
     *
     * @param array<string, mixed> $configuration Producer options on top of the defaults
     */
    private function configurationProbe(array $configuration): KafkaProducer
    {
        return new class ($configuration + $this->clusterConfiguration) extends KafkaProducer {
            /**
             * Configuration that this producer built its client with
             *
             * @var array<string, mixed>
             */
            public array $clientConfiguration = [];

            /**
             * @inheritdoc
             */
            protected function createClient(Cluster $cluster, array $configuration): Client
            {
                $this->clientConfiguration = $configuration;

                return new FakeClient($cluster, $configuration);
            }
        };
    }

    /**
     * Returns the number of bytes that a record takes in a produce request
     */
    private function recordSize(?string $key, string $value): int
    {
        return MessageSet::ENTRY_OVERHEAD + new Message($value, $key)->sizeInBytes();
    }

}
