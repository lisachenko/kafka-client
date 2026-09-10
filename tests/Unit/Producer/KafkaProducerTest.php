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
use Protocol\Kafka\Common\Errors\UnknownProducerIdException;
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Record\RecordV2;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
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

    public function testTheLogAppendTimeOfTheBrokerReplacesTheCreateTimeInTheRecordMetadata(): void
    {
        // Version 2 of the Produce API answers with the time the broker stamped the batch with when the topic is
        // configured with `message.timestamp.type=LogAppendTime`; that value is the one the log holds, so it is
        // the one the metadata of the batch reports - as the Java `RecordMetadata` does
        $appendTime  = 1489324800000 + 4711;
        [$producer]  = $this->producer(
            [ProducerConfig::BATCH_SIZE => 1024 * 1024],
            [function (array $topicPartitionMessages) use ($appendTime): array {
                $result = $this->fakeClient->acknowledge($topicPartitionMessages);
                foreach ($result as $partitions) {
                    foreach ($partitions as $partition) {
                        $partition->logAppendTime = $appendTime;
                    }
                }

                return $result;
            }]
        );

        $metadata = null;
        $producer
            ->send(self::TOPIC, new Record('value', 'key-0', 0, null, 1489324800000))
            ->then(static function (RecordMetadata $recordMetadata) use (&$metadata): void {
                $metadata = $recordMetadata;
            });
        $producer->flush();

        self::assertInstanceOf(RecordMetadata::class, $metadata);
        self::assertSame($appendTime, $metadata->timestamp, 'the broker stamped the batch itself');
    }

    public function testTheCreateTimeIsKeptWhenTheBrokerReportsNoAppendTime(): void
    {
        // -1 is what a topic that keeps the CreateTime of the producer answers, and what the versions 0 and 1 of
        // the api leave the field at, because they do not carry it at all
        [$producer] = $this->producer([ProducerConfig::BATCH_SIZE => 1024 * 1024]);

        $metadata = null;
        $producer
            ->send(self::TOPIC, new Record('value', 'key-0', 0, null, 1489324800000))
            ->then(static function (RecordMetadata $recordMetadata) use (&$metadata): void {
                $metadata = $recordMetadata;
            });
        $producer->flush();

        self::assertSame(-1, ProduceResponsePartition::NO_LOG_APPEND_TIME);
        self::assertInstanceOf(RecordMetadata::class, $metadata);
        self::assertSame(1489324800000, $metadata->timestamp, 'the CreateTime the producer stamped on the batch');
    }

    public function testTheRecordSizeOfMessageFormatV1CountsTheTimestampAsWell(): void
    {
        [$producer] = $this->producer([
            ProducerConfig::BATCH_SIZE             => 1024 * 1024,
            ProducerConfig::MESSAGE_FORMAT_VERSION => ProducerConfig::MESSAGE_FORMAT_VERSION_0_10_0,
            ProducerConfig::MAX_REQUEST_SIZE       => MessageSet::ENTRY_OVERHEAD + Message::MIN_SIZE_V1 + 4,
        ]);

        // A record of five bytes fits into message format v0 but not into v1, which adds the eight bytes of the
        // timestamp to every message
        $this->expectException(MessageTooLargeException::class);

        $producer->send(self::TOPIC, Record::fromValue('value'));
    }

    public function testTheRecordSizeOfTheMessageFormatV2CountsItsHeaders(): void
    {
        $withoutHeaders = Record::fromValue('value');
        $withHeaders    = $withoutHeaders->withHeaders(new Header('trace-id', 'abc'));

        [$producer] = $this->producer([
            ProducerConfig::BATCH_SIZE       => 1024 * 1024,
            ProducerConfig::MAX_REQUEST_SIZE => new RecordV2('value')->sizeInBytes(),
        ]);

        // The bare record fits exactly into `max.request.size`, the very same record with a header does not: a
        // record of the message format v2 carries its headers next to its key and its value
        $producer->send(self::TOPIC, $withoutHeaders);

        $this->expectException(MessageTooLargeException::class);

        $producer->send(self::TOPIC, $withHeaders);
    }

    public function testAnUnknownMessageFormatVersionIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('1.0.0');

        new KafkaProducer([ProducerConfig::MESSAGE_FORMAT_VERSION => '1.0.0']);
    }

    public function testTheHeadersOfARecordReachTheClientThatWritesTheBatch(): void
    {
        [$producer, $client] = $this->producer();

        $producer->send(
            self::TOPIC,
            Record::fromKeyValue('key-0', 'value')->withHeaders(new Header('trace-id', 'abc'), new Header('n'))
        );

        $records = $client->receivedRecords();
        self::assertCount(1, $records);
        self::assertSame(['trace-id', 'n'], array_map(
            static fn(Header $header): string => $header->key,
            $records[0]->headers
        ));
        self::assertSame(['abc', null], array_map(
            static fn(Header $header): ?string => $header->value,
            $records[0]->headers
        ));
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
        // The real producer hands its *resolved* configuration to `createClient()`, so the double has to see the
        // `acks = all` that `enable.idempotence` implies as well
        $client = new FakeClient(
            $this->cluster,
            ProducerConfig::resolveIdempotence($configuration) + ProducerConfig::getDefaultConfiguration(),
            $behaviours
        );

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
        // `message.format.version` defaults to the record batch of the message format v2, whose records are
        // varint-encoded and cost far less per record than an entry of a message set
        return new RecordV2($value, $key)->sizeInBytes();
    }

    public function testAPlainProducerCarriesNoProducerStateAtAll(): void
    {
        [$producer, $client] = $this->producer();

        $producer->send(self::TOPIC, Record::fromValue('plain'));

        self::assertSame([], $client->initProducerIdCalls, 'Nothing asks for a producer id');
        self::assertSame(
            [
                'producerId'      => RecordBatch::NO_PRODUCER_ID,
                'producerEpoch'   => RecordBatch::NO_PRODUCER_EPOCH,
                'baseSequences'   => [],
                'transactionalId' => null,
            ],
            $client->producerStates[0]
        );
    }

    public function testAnIdempotentProducerStampsEveryBatchWithItsProducerState(): void
    {
        [$producer, $client] = $this->producer([ProducerConfig::ENABLE_IDEMPOTENCE => true]);
        $client->producerIds = [new ProducerIdAndEpoch(2000, 0)];

        $producer->send(self::TOPIC, Record::fromValue('one'), 0);
        $producer->send(self::TOPIC, Record::fromValue('two'), 0);
        $producer->send(self::TOPIC, Record::fromValue('three'), 1);

        self::assertCount(1, $client->initProducerIdCalls, 'The producer id is asked for once, with the first flush');
        self::assertSame(
            ['transactionalId' => null, 'transactionTimeoutMs' => 60000],
            $client->initProducerIdCalls[0],
            'An idempotent producer has no transactional id'
        );

        self::assertSame(2000, $client->producerStates[0]['producerId']);
        self::assertSame(0, $client->producerStates[0]['producerEpoch']);
        self::assertNull($client->producerStates[0]['transactionalId']);
        self::assertSame([self::TOPIC => [0 => 0]], $client->producerStates[0]['baseSequences']);
        self::assertSame(
            [self::TOPIC => [0 => 1]],
            $client->producerStates[1]['baseSequences'],
            'The second batch of the partition continues where the first one ended'
        );
        self::assertSame(
            [self::TOPIC => [1 => 0]],
            $client->producerStates[2]['baseSequences'],
            'Another partition starts at 0 - the broker deduplicates per producer and partition'
        );
    }

    public function testABatchThatWasNotAcknowledgedKeepsItsSequenceNumbers(): void
    {
        $failure = new NotLeaderForPartitionException(['topic' => self::TOPIC]);

        [$producer, $client] = $this->producer(
            [ProducerConfig::ENABLE_IDEMPOTENCE => true],
            [static fn(): array => throw new TopicPartitionRequestException([], [self::TOPIC => [0 => $failure]])]
        );
        $client->producerIds = [new ProducerIdAndEpoch(2000, 0)];

        $rejected = null;
        $producer
            ->send(self::TOPIC, Record::fromValue('one'), 0)
            ->then(null, static function (\Throwable $error) use (&$rejected): void {
                $rejected = $error;
            });
        $producer->send(self::TOPIC, Record::fromValue('one again'), 0);
        $producer->send(self::TOPIC, Record::fromValue('two'), 0);

        self::assertSame($failure, $rejected);
        self::assertSame([self::TOPIC => [0 => 0]], $client->producerStates[0]['baseSequences']);
        self::assertSame(
            [self::TOPIC => [0 => 0]],
            $client->producerStates[1]['baseSequences'],
            'Nothing was appended, so the next batch of the partition carries the very same sequence'
        );
        self::assertSame(
            [self::TOPIC => [0 => 1]],
            $client->producerStates[2]['baseSequences'],
            'Only an acknowledged batch moves the sequence on'
        );
    }

    public function testAnIdempotentProducerNumbersAPartitionFromZeroAgainWhenItsRecordsWereDeleted(): void
    {
        // Kafka 1.0: the broker has no state of this producer for the partition any more, and the log start offset
        // of the Produce v5 answer says why - everything this producer wrote there is below it
        $unknown = new UnknownProducerIdException(
            ['topic' => self::TOPIC, 'partitionId' => 0, 'logStartOffset' => 5]
        );

        [$producer, $client] = $this->producer(
            [ProducerConfig::ENABLE_IDEMPOTENCE => true],
            [
                fn(array $records): array => $this->fakeClient->acknowledge($records),
                static fn(): array => throw new TopicPartitionRequestException([], [self::TOPIC => [0 => $unknown]]),
            ]
        );
        $client->producerIds = [new ProducerIdAndEpoch(2000, 0)];

        $rejected = null;
        $producer->send(self::TOPIC, Record::fromValue('one'), 0);
        $producer
            ->send(self::TOPIC, Record::fromValue('after the deletion'), 0)
            ->then(null, static function (\Throwable $error) use (&$rejected): void {
                $rejected = $error;
            });
        $producer->flush();

        self::assertNull($rejected, 'the batch that was sent again was appended, so nothing is reported');
        self::assertCount(3, $client->producerStates, 'the refused batch went out a second time');
        self::assertSame([self::TOPIC => [0 => 0]], $client->producerStates[0]['baseSequences']);
        self::assertSame(
            [self::TOPIC => [0 => 1]],
            $client->producerStates[1]['baseSequences'],
            'the batch the broker refused continued the numbering of the partition'
        );
        self::assertSame(
            [self::TOPIC => [0 => 0]],
            $client->producerStates[2]['baseSequences'],
            'and the one that was sent again starts it over'
        );
        self::assertSame(
            2000,
            $client->producerStates[2]['producerId'],
            'under the very same producer id - only the numbering of that partition started over'
        );
        self::assertCount(1, $client->initProducerIdCalls, 'so no new producer id was asked for');
    }

    public function testIdempotenceOverridesTheAcksAndTheRetriesOfTheProducer(): void
    {
        $probe = $this->configurationProbe([ProducerConfig::ENABLE_IDEMPOTENCE => true]);
        $probe->send(self::TOPIC, Record::fromValue('x'));

        self::assertSame(ProducerConfig::ACKS_ALL, $probe->clientConfiguration[ProducerConfig::ACKS]);
        self::assertSame(
            ProducerConfig::DEFAULT_IDEMPOTENT_RETRIES,
            $probe->clientConfiguration[ProducerConfig::RETRIES]
        );
    }

    public function testAProducerThatWantsIdempotenceWithoutAcksAllIsRefused(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Must set acks to all in order to use the idempotent producer');

        new TestKafkaProducer(
            [
                ProducerConfig::ENABLE_IDEMPOTENCE => true,
                ProducerConfig::ACKS               => ProducerConfig::ACKS_LEADER,
            ] + $this->clusterConfiguration,
            new FakeClient($this->cluster)
        );
    }

    public function testAProducerThatWantsIdempotenceWithoutRetriesIsRefused(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Must set retries to non-zero when using the idempotent producer');

        new TestKafkaProducer(
            [
                ProducerConfig::ENABLE_IDEMPOTENCE => true,
                ProducerConfig::RETRIES            => 0,
            ] + $this->clusterConfiguration,
            new FakeClient($this->cluster)
        );
    }

    public function testATransactionalIdImpliesIdempotenceAndItsConstraints(): void
    {
        $probe = $this->configurationProbe([ProducerConfig::TRANSACTIONAL_ID => 'tx-1']);
        // The client is built by the first transactional call, which is the first one a transactional producer makes
        $probe->initTransactions();

        self::assertTrue($probe->clientConfiguration[ProducerConfig::ENABLE_IDEMPOTENCE]);
        self::assertSame(ProducerConfig::ACKS_ALL, $probe->clientConfiguration[ProducerConfig::ACKS]);
        self::assertSame(
            ProducerConfig::DEFAULT_IDEMPOTENT_RETRIES,
            $probe->clientConfiguration[ProducerConfig::RETRIES]
        );
    }

    public function testATransactionalIdNextToDisabledIdempotenceIsRefused(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Cannot set enable.idempotence to false while a transactional.id');

        new TestKafkaProducer(
            [
                ProducerConfig::TRANSACTIONAL_ID   => 'tx-1',
                ProducerConfig::ENABLE_IDEMPOTENCE => false,
            ] + $this->clusterConfiguration,
            new FakeClient($this->cluster)
        );
    }

    public function testTheEmptyStringIsNotATransactionalId(): void
    {
        // A broker answers the empty id with the error code 42, so the producer refuses it before it sends anything
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('transactional.id must be a non-empty string');

        new TestKafkaProducer(
            [ProducerConfig::TRANSACTIONAL_ID => ''] + $this->clusterConfiguration,
            new FakeClient($this->cluster)
        );
    }

    public function testTheTransactionalApiIsRefusedWithoutATransactionalId(): void
    {
        [$producer] = $this->producer();

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The transactional API of the producer needs a transactional.id');

        $producer->initTransactions();
    }

    public function testASendOutsideATransactionIsRefused(): void
    {
        [$producer] = $this->producer([ProducerConfig::TRANSACTIONAL_ID => 'tx-1']);
        $producer->initTransactions();

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('Can not send in the state READY');

        $producer->send(self::TOPIC, Record::fromValue('outside'), 0);
    }

    public function testAWholeTransactionSendsItsRequestsInTheOrderOfTheProtocol(): void
    {
        [$producer, $client] = $this->producer([ProducerConfig::TRANSACTIONAL_ID => 'tx-1']);

        $producer->initTransactions();
        $producer->beginTransaction();
        $producer->send(self::TOPIC, Record::fromValue('one'), 0);
        $producer->flush();
        $producer->sendOffsetsToTransaction([self::TOPIC => [0 => 5]], 'my-group');
        $producer->commitTransaction();

        self::assertSame(
            ['addPartitionsToTxn', 'addOffsetsToTxn', 'txnOffsetCommit', 'endTxn'],
            array_column($client->transactionCalls, 0)
        );
        self::assertSame(
            'tx-1',
            $client->producerStates[0]['transactionalId'],
            'the batch of a transaction carries the transactional id into the Produce request'
        );
        self::assertTrue($client->transactionCalls[3][2], 'the last request commits');
    }

    public function testAnAbortThrowsTheBufferedRecordsAwayInsteadOfSendingThem(): void
    {
        // A batch size that the record does not fill, so that `send()` does not flush it right away
        [$producer, $client] = $this->producer([
            ProducerConfig::TRANSACTIONAL_ID => 'tx-1',
            ProducerConfig::BATCH_SIZE       => 65536,
        ]);

        $producer->initTransactions();
        $producer->beginTransaction();
        $rejected = null;
        $producer->send(self::TOPIC, Record::fromValue('never written'), 0)
            ->then(null, static function (\Throwable $error) use (&$rejected): void {
                $rejected = $error;
            });

        $producer->abortTransaction();

        self::assertSame([], $client->produceCalls, 'nothing of an aborted transaction is sent');
        self::assertSame([['endTxn', 'tx-1', false]], $client->transactionCalls);
        self::assertInstanceOf(\RuntimeException::class, $rejected);
        self::assertStringContainsString('The transaction was aborted', $rejected->getMessage());
    }
}
