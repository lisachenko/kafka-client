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
use PHPUnit\Framework\Attributes\DataProvider;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\RecordTooLargeException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Serialization\StringDeserializer;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Consumer\Internals\SubscriptionState;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Drives the consumer against a real Kafka 0.8.2.2 broker.
 *
 * Kafka 0.8 has no broker-side group membership, so the partitions are always assigned explicitly; what the broker
 * does provide is the offset storage of a group, in the `__consumer_offsets` topic (the version 1 of the offset
 * APIs, `offsets.storage` = `kafka`) or in ZooKeeper (the version 0, `offsets.storage` = `zookeeper`). Both are
 * exercised here, because a consumer resumes from what it committed there.
 *
 * @see docs/protocol/0.8.2.md, sections "Fetch API (key 1, v0)", "Offsets API (key 2, v0), a.k.a. ListOffset" and
 *      "OffsetFetch API (key 9, v0 and v1)"
 */
#[CoversClass(KafkaConsumer::class)]
#[CoversClass(SubscriptionState::class)]
#[CoversClass(ConsumerConfig::class)]
#[CoversClass(ConsumerRecord::class)]
#[CoversClass(RecordTooLargeException::class)]
final class KafkaConsumerTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t9';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * How long a poll loop keeps asking for the expected records, in seconds
     */
    private const float POLL_TIMEOUT = 30.0;

    /**
     * Topic of the current test, created and given a leader by {@see self::setUp()}
     */
    private string $topic;

    /**
     * Consumers that were built by the current test and have to release their assignment afterwards
     *
     * @var list<KafkaConsumer>
     */
    private array $consumers = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t9-consumer');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    protected function tearDown(): void
    {
        foreach ($this->consumers as $consumer) {
            $consumer->unsubscribe();
        }
        $this->consumers = [];

        parent::tearDown();
    }

    public function testPartitionsForReportsTheMetadataOfTheTopic(): void
    {
        $consumer   = $this->consumer(self::uniqueGroupName());
        $partitions = $consumer->partitionsFor($this->topic);

        self::assertGreaterThanOrEqual(2, count($partitions), 'the broker creates the topic with num.partitions');
        foreach ($partitions as $partitionId => $metadata) {
            self::assertSame($partitionId, $metadata->partitionId);
            self::assertGreaterThanOrEqual(0, $metadata->leader);
        }
    }

    public function testTwoAssignedPartitionsAreConsumedFromTheBeginning(): void
    {
        $this->produce(0, ['p0-a', 'p0-b', 'p0-c']);
        $this->produce(1, ['p1-a', 'p1-b']);

        $consumer = $this->consumer(self::uniqueGroupName(), [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([$this->topic => [0, 1]]);

        self::assertSame([$this->topic => [0 => 0, 1 => 1]], $consumer->assignment());
        self::assertSame([], $consumer->subscription(), 'there is no topic subscription in 0.8');
        self::assertSame(0, $consumer->position($this->topic, 0));

        $received = $this->pollUntil($consumer, 5);

        self::assertSame(['p0-a', 'p0-b', 'p0-c'], $this->valuesOf($received, 0));
        self::assertSame(['p1-a', 'p1-b'], $this->valuesOf($received, 1));
        self::assertSame([0, 1, 2], $this->offsetsOf($received, 0));
        self::assertSame(3, $consumer->position($this->topic, 0));
        self::assertSame(2, $consumer->position($this->topic, 1));
    }

    /**
     * @return iterable<string, array{0: string}>
     */
    public static function offsetStorages(): iterable
    {
        yield 'kafka'     => [ClientConfig::OFFSETS_STORAGE_KAFKA];
        yield 'zookeeper' => [ClientConfig::OFFSETS_STORAGE_ZOOKEEPER];
    }

    #[DataProvider('offsetStorages')]
    public function testANewConsumerResumesFromTheCommittedOffset(string $storage): void
    {
        $groupId = self::uniqueGroupName();
        $this->produce(0, ['one', 'two', 'three']);

        $first = $this->consumer($groupId, [
            ClientConfig::OFFSETS_STORAGE      => $storage,
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $first->assign([$this->topic => [0]]);
        $this->pollUntil($first, 3);
        $first->commitSync();

        self::assertSame([$this->topic => [0 => 3]], $first->committed([$this->topic => [0]]));

        $this->produce(0, ['four']);

        // A second consumer of the same group starts where the first one stopped, although it would otherwise
        // jump to the end of the log
        $second = $this->consumer($groupId, [
            ClientConfig::OFFSETS_STORAGE      => $storage,
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::LATEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $second->assign([$this->topic => [0]]);

        self::assertSame(3, $second->position($this->topic, 0));

        $received = $this->pollUntil($second, 1);

        self::assertSame(['four'], $this->valuesOf($received, 0));
        self::assertSame([3], $this->offsetsOf($received, 0));
    }

    public function testTheTwoOffsetStoragesAreIndependent(): void
    {
        $groupId = self::uniqueGroupName();
        $this->produce(0, ['one', 'two']);

        $inKafka = $this->consumer($groupId, [
            ClientConfig::OFFSETS_STORAGE      => ClientConfig::OFFSETS_STORAGE_KAFKA,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $inKafka->assign([$this->topic => [0]]);
        $inKafka->commitSync([$this->topic => [0 => 2]]);

        $inZooKeeper = $this->consumer($groupId, [
            ClientConfig::OFFSETS_STORAGE      => ClientConfig::OFFSETS_STORAGE_ZOOKEEPER,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $inZooKeeper->assign([$this->topic => [0]]);

        self::assertSame([$this->topic => [0 => 2]], $inKafka->committed([$this->topic => [0]]));
        self::assertSame(
            [$this->topic => [0 => -1]],
            $inZooKeeper->committed([$this->topic => [0]]),
            'the same group has one position per storage'
        );
    }

    public function testAutomaticCommitStoresThePositionsOfThePoll(): void
    {
        $groupId = self::uniqueGroupName();
        $this->produce(0, ['auto-a', 'auto-b']);

        $consumer = $this->consumer($groupId, [
            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => true,
            ConsumerConfig::AUTO_COMMIT_INTERVAL_MS => 0,
        ]);
        $consumer->assign([$this->topic => [0]]);
        $this->pollUntil($consumer, 2);

        self::assertSame(
            [$this->topic => [0 => 2]],
            $consumer->committed([$this->topic => [0]]),
            'poll() committed the position without an explicit commitSync()'
        );
    }

    public function testSeekToBeginningAndSeekToEndMoveThePosition(): void
    {
        $this->produce(0, ['first', 'second', 'third']);

        $consumer = $this->consumer(self::uniqueGroupName(), [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([$this->topic => [0]]);

        $consumer->seekToEnd([$this->topic => [0]]);

        self::assertSame(3, $consumer->position($this->topic, 0));
        self::assertSame([], $this->pollOnce($consumer, 0), 'nothing is left at the end of the log');

        $consumer->seekToBeginning([$this->topic => [0]]);

        self::assertSame(0, $consumer->position($this->topic, 0));
        self::assertSame(['first', 'second', 'third'], $this->valuesOf($this->pollUntil($consumer, 3), 0));
    }

    public function testAPositionBeyondTheLogIsResetToTheBeginning(): void
    {
        $this->produce(0, ['alpha', 'beta']);

        $consumer = $this->consumer(self::uniqueGroupName(), [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([$this->topic => [0]]);
        $consumer->seek($this->topic, 0, 1000000);

        $received = $this->pollUntil($consumer, 2);

        self::assertSame(['alpha', 'beta'], $this->valuesOf($received, 0));
        self::assertSame(2, $consumer->position($this->topic, 0));
    }

    public function testAPositionBeyondTheLogIsResetToTheEnd(): void
    {
        $this->produce(0, ['alpha', 'beta']);

        $consumer = $this->consumer(self::uniqueGroupName(), [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::LATEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([$this->topic => [0]]);
        $consumer->seek($this->topic, 0, 1000000);

        self::assertSame([], $this->pollOnce($consumer, 0));
        self::assertSame(2, $consumer->position($this->topic, 0), 'the position was reset to the end of the log');

        $this->produce(0, ['gamma']);

        self::assertSame(['gamma'], $this->valuesOf($this->pollUntil($consumer, 1), 0));
    }

    public function testAGzipCompressedMessageSetIsUnwrapped(): void
    {
        $values = ['gzip-a', 'gzip-b', 'gzip-c'];
        $this->produce(0, $values, CompressionCodec::GZIP);

        $consumer = $this->consumer(self::uniqueGroupName(), [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([$this->topic => [0]]);

        $received = $this->pollUntil($consumer, 3);

        self::assertSame($values, $this->valuesOf($received, 0));
        self::assertSame([0, 1, 2], $this->offsetsOf($received, 0), 'the broker assigns the inner offsets');
        self::assertSame(3, $consumer->position($this->topic, 0));
    }

    public function testASecondPollOfAGzipSetDoesNotReturnTheRecordsAgain(): void
    {
        // A compressed set is stored as one message, so a fetch in the middle of it returns the whole set back
        $this->produce(0, ['gzip-a', 'gzip-b', 'gzip-c'], CompressionCodec::GZIP);

        $consumer = $this->consumer(self::uniqueGroupName(), [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([$this->topic => [0]]);
        $consumer->seek($this->topic, 0, 1);

        $received = $this->pollUntil($consumer, 2);

        self::assertSame(['gzip-b', 'gzip-c'], $this->valuesOf($received, 0));
        self::assertSame([1, 2], $this->offsetsOf($received, 0));
    }

    public function testDeserializersAreAppliedToTheConsumedRecords(): void
    {
        $this->produce(0, ['payload']);

        $consumer = $this->consumer(self::uniqueGroupName(), [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
            ConsumerConfig::VALUE_DESERIALIZER => StringDeserializer::class,
        ]);
        $consumer->assign([$this->topic => [0]]);

        $records = $this->recordsOf($this->pollUntil($consumer, 1), 0);
        $record  = $records[0];

        self::assertInstanceOf(ConsumerRecord::class, $record);
        self::assertSame($this->topic, $record->topic);
        self::assertSame(0, $record->partition);
        self::assertSame('payload', $record->deserializedValue);
    }

    public function testAPausedPartitionIsNotConsumedUntilItIsResumed(): void
    {
        $this->produce(0, ['p0']);
        $this->produce(1, ['p1']);

        $consumer = $this->consumer(self::uniqueGroupName(), [
            ConsumerConfig::AUTO_OFFSET_RESET  => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $consumer->assign([$this->topic => [0, 1]]);
        $consumer->pause([$this->topic => [1]]);

        $received = $this->pollUntil($consumer, 1);

        self::assertSame(['p0'], $this->valuesOf($received, 0));
        self::assertSame([], $this->recordsOf($received, 1));
        self::assertSame(0, $consumer->position($this->topic, 1));

        $consumer->resume([$this->topic => [1]]);

        self::assertSame(['p1'], $this->valuesOf($this->pollUntil($consumer, 1), 1));
    }

    public function testAMessageBiggerThanTheFetchSizeIsReported(): void
    {
        $this->produce(0, [str_repeat('x', 4096)]);

        $consumer = $this->consumer(self::uniqueGroupName(), [
            ConsumerConfig::AUTO_OFFSET_RESET         => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT        => false,
            ConsumerConfig::MAX_PARTITION_FETCH_BYTES => 64,
        ]);
        $consumer->assign([$this->topic => [0]]);

        // The broker answers with a message it cut short, which the record layer drops: the first empty answer is
        // indistinguishable from an idle partition, the second one is not
        self::assertSame([], $this->pollOnce($consumer, 0));

        $this->expectException(RecordTooLargeException::class);
        $this->expectExceptionMessageMatches('/max\.partition\.fetch\.bytes/');
        $consumer->poll(1000);
    }

    /**
     * Runs a single poll and returns the records that came back for one partition of the topic under test
     *
     * @return list<Record>
     */
    private function pollOnce(KafkaConsumer $consumer, int $partition): array
    {
        return $this->recordsOf($consumer->poll(1000)[$this->topic] ?? [], $partition);
    }

    /**
     * Keeps polling until the expected number of records arrived, or the poll timeout elapsed
     *
     * @return array<int, list<Record>> Partition => records received for it, in offset order
     */
    private function pollUntil(KafkaConsumer $consumer, int $expectedRecords): array
    {
        $received = [];
        $total    = 0;
        $deadline = microtime(true) + self::POLL_TIMEOUT;

        do {
            foreach ($consumer->poll(1000) as $partitions) {
                foreach ($partitions as $partition => $records) {
                    foreach ($records as $record) {
                        $received[$partition][] = $record;
                        $total++;
                    }
                }
            }
        } while ($total < $expectedRecords && microtime(true) < $deadline);

        self::assertGreaterThanOrEqual(
            $expectedRecords,
            $total,
            sprintf('only %d of the %d expected records arrived within the poll timeout', $total, $expectedRecords)
        );

        return $received;
    }

    /**
     * Builds a consumer for the broker under test and registers it for the clean-up
     *
     * @param array<string, mixed> $configuration Options that override the defaults of this test class
     */
    private function consumer(string $groupId, array $configuration = []): KafkaConsumer
    {
        $consumer = new KafkaConsumer($configuration + [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ConsumerConfig::GROUP_ID                => $groupId,
            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
        ]);

        $this->consumers[] = $consumer;

        return $consumer;
    }

    /**
     * Produces the given values into one partition of the topic under test
     *
     * @param list<string> $values Values of the records to append
     */
    private function produce(int $partition, array $values, int $codec = CompressionCodec::NONE): void
    {
        $records    = array_map(static fn(string $value): Record => new Record($value), $values);
        $messageSet = MessageSet::fromRecords($records, $codec);

        $stream = $this->connect();
        new ProduceRequest(
            [$this->topic => [$partition => $messageSet]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            1
        )->writeTo($stream);

        $response = ProduceResponse::unpack($stream)->topics[$this->topic]->partitions[$partition];
        if ($response->errorCode !== 0) {
            throw KafkaException::fromCode(
                $response->errorCode,
                ['topic' => $this->topic, 'partitionId' => $partition]
            );
        }
    }

    /**
     * @param array<int, list<Record>> $received Records of a poll loop
     *
     * @return list<Record>
     */
    private function recordsOf(array $received, int $partition): array
    {
        $records = $received[$partition] ?? [];

        return is_array($records) ? array_values($records) : [];
    }

    /**
     * @param array<int, list<Record>> $received Records of a poll loop
     *
     * @return list<string|null>
     */
    private function valuesOf(array $received, int $partition): array
    {
        return array_map(
            static fn(Record $record): ?string => $record->value,
            $this->recordsOf($received, $partition)
        );
    }

    /**
     * @param array<int, list<Record>> $received Records of a poll loop
     *
     * @return list<int>
     */
    private function offsetsOf(array $received, int $partition): array
    {
        return array_map(
            static fn(Record $record): int => (int) $record->offset,
            $this->recordsOf($received, $partition)
        );
    }

    /**
     * Builds a consumer group name that is unique for this test run
     */
    private static function uniqueGroupName(): string
    {
        return 't9-group-' . bin2hex(random_bytes(6));
    }
}
