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
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\ControlRecordType;
use Protocol\Kafka\Common\Record\EndTransactionMarker;
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Record\RecordV2;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Tests\Fixture\RawRecordBatchProbe;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * The record batch of the message format v2 against a real Kafka 0.11.0.3 broker.
 *
 * The broker is the authority on the format: it validates the CRC-32C of every batch it appends, it assigns the base
 * offset and the partition leader epoch, it converts a batch into the `message.format.version` of the topic on write
 * and it converts the log back down for every client that fetches with a request below version 4. Everything this
 * suite asserts was read out of a log a 0.11.0.3 broker wrote.
 *
 * The apis that carry a batch - Produce v3 and Fetch v4/v5 - are not part of this ticket, so the frames are built by
 * {@see RawRecordBatchProbe}; that is also the only way to make the broker write a **control batch** without a
 * transactional producer.
 *
 * @see docs/protocol/1.1.md, section "RecordBatch (message format v2)"
 */
#[CoversClass(RecordBatch::class)]
#[CoversClass(RecordV2::class)]
#[CoversClass(MemoryRecords::class)]
#[CoversClass(Header::class)]
#[CoversClass(EndTransactionMarker::class)]
final class RecordBatchV2Test extends IntegrationTestCase
{
    /**
     * Client id that identifies the frames of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t2-rbv2';

    /**
     * Partition that every test of this class produces to and fetches from
     */
    private const int PARTITION = 0;

    /**
     * Error code the broker answers for a batch whose checksum does not match its contents
     */
    private const int CORRUPT_MESSAGE = 2;

    /**
     * Topic of the current test
     */
    private string $topic;

    /**
     * Probe that builds the raw Produce and Fetch frames of this suite
     */
    private RawRecordBatchProbe $probe;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t2-rbv2');
        $this->awaitTopic($this->topic);
        $this->probe = new RawRecordBatchProbe(self::firstBootstrapServer(), self::CLIENT_ID);
    }

    protected function tearDown(): void
    {
        // The whole suite is skipped before the probe exists when no broker is configured
        if (isset($this->probe)) {
            $this->probe->close();
        }
    }

    /**
     * @return iterable<string, array{0: int}>
     */
    public static function compressionCodecs(): iterable
    {
        yield 'uncompressed' => [CompressionCodec::NONE];
        yield 'gzip'         => [CompressionCodec::GZIP];
        yield 'snappy'       => [CompressionCodec::SNAPPY];
        yield 'lz4'          => [CompressionCodec::LZ4];
    }

    #[DataProvider('compressionCodecs')]
    public function testABatchOfEveryCodecSurvivesTheRoundTripThroughTheBroker(int $codec): void
    {
        $createTime = self::now();
        $records    = [
            new Record('alpha', null, 0, null, $createTime, TimestampType::CREATE_TIME),
            new Record('bravo', 'key', 0, null, $createTime + 10, TimestampType::CREATE_TIME),
            new Record('charlie', null, 0, null, $createTime + 5, TimestampType::CREATE_TIME),
        ];

        $written = $this->produce(RecordBatch::fromRecords($records, $codec));
        self::assertSame(0, $written['errorCode'], 'the broker accepted the batch');
        self::assertSame(0, $written['baseOffset']);
        self::assertSame(-1, $written['logAppendTime'], 'a CreateTime topic stamps nothing');

        $region = $this->fetch();
        $read   = $region->getRecords();

        self::assertSame(RecordBatch::MAGIC, $region->getMagic(), 'the log of a 0.11 topic is the message format v2');
        self::assertCount(1, $region->getBatches());
        self::assertCount(3, $read);
        self::assertSame([0, 1, 2], array_map(static fn(Record $r): ?int => $r->offset, $read));
        self::assertSame(['alpha', 'bravo', 'charlie'], array_map(static fn(Record $r): ?string => $r->value, $read));
        self::assertSame([null, 'key', null], array_map(static fn(Record $r): ?string => $r->key, $read));
        self::assertSame(
            [$createTime, $createTime + 10, $createTime + 5],
            array_map(static fn(Record $r): ?int => $r->timestamp, $read)
        );

        $batch = $region->getBatches()[0];
        self::assertInstanceOf(RecordBatch::class, $batch);
        self::assertSame($codec, $batch->getCompressionCodec());
        self::assertSame(TimestampType::CREATE_TIME, $batch->getTimestampType());
        self::assertSame($createTime, $batch->getFirstTimestamp());
        self::assertSame($createTime + 10, $batch->getMaxTimestamp(), 'the largest timestamp of the batch');
        self::assertSame(2, $batch->lastOffsetDelta);
        self::assertSame(0, $batch->getPartitionLeaderEpoch(), 'the broker assigned the leader epoch');
        self::assertSame(RecordBatch::NO_PRODUCER_ID, $batch->getProducerId());
        self::assertFalse($batch->isTransactional());
        self::assertFalse($batch->isControlBatch());
    }

    public function testTheBytesTheBrokerAnswersAreExactlyTheOnesItStoresAndThisClientWrites(): void
    {
        $createTime = self::now();
        $batch      = RecordBatch::fromRecords(
            [new Record('alpha', null, 0, null, $createTime, TimestampType::CREATE_TIME)]
        );

        $this->produce($batch);
        $answered = $this->fetch();

        // The broker only fills in the base offset, the leader epoch and the checksum that follows from them; the
        // records part is the one this client wrote, byte for byte
        self::assertSame(
            bin2hex(substr($batch->toBuffer(), RecordBatch::OVERHEAD)),
            bin2hex(substr($answered->toBuffer(), RecordBatch::OVERHEAD))
        );
        self::assertSame(
            bin2hex($answered->toBuffer()),
            bin2hex($answered->getBatches()[0]->toBuffer()),
            'and the batch re-encodes into the answer of the broker'
        );
    }

    public function testTheHeadersOfARecordSurviveTheBroker(): void
    {
        $createTime = self::now();
        $records    = [
            new Record('alpha', null, 0, null, $createTime, TimestampType::CREATE_TIME, [
                new Header('content-type', 'application/json'),
                new Header('trace-id', "\x00\x01\x02"),
            ]),
            new Record('bravo', 'key', 0, null, $createTime, TimestampType::CREATE_TIME, [
                new Header('empty-value', ''),
                new Header('null-value', null),
            ]),
        ];

        $this->produce(RecordBatch::fromRecords($records));
        $read = $this->fetch()->getRecords();

        self::assertCount(2, $read);
        self::assertSame('content-type', $read[0]->headers[0]->key);
        self::assertSame('application/json', $read[0]->headers[0]->value);
        self::assertSame("\x00\x01\x02", $read[0]->headers[1]->value);
        self::assertSame('', $read[1]->headers[0]->value, 'an empty header value stays empty');
        self::assertNull($read[1]->headers[1]->value, 'a null header value stays null');
    }

    public function testAnEmptyKeyAndANullValueAreTwoDifferentThings(): void
    {
        $createTime = self::now();
        $records    = [
            new Record(null, '', 0, null, $createTime, TimestampType::CREATE_TIME),
            new Record('', null, 0, null, $createTime, TimestampType::CREATE_TIME),
            new Record(null, null, 0, null, $createTime, TimestampType::CREATE_TIME),
        ];

        $this->produce(RecordBatch::fromRecords($records));
        $read = $this->fetch()->getRecords();

        self::assertSame(['', null, null], array_map(static fn(Record $r): ?string => $r->key, $read));
        self::assertSame([null, '', null], array_map(static fn(Record $r): ?string => $r->value, $read));
    }

    public function testALogAppendTimeTopicRewritesOnlyTheMaxTimestampOfTheBatch(): void
    {
        $topic = self::uniqueTopicName('t2-rbv2-lat');
        $this->createTopic($topic, ['message.timestamp.type=LogAppendTime']);

        $createTime = self::now();
        $before     = self::now();
        $this->produce(
            RecordBatch::fromRecords([
                new Record('alpha', null, 0, null, $createTime, TimestampType::CREATE_TIME),
                new Record('bravo', null, 0, null, $createTime + 10, TimestampType::CREATE_TIME),
            ]),
            $topic
        );
        $after = self::now();

        $region = $this->fetch(0, 4, 0, $topic);
        $batch  = $region->getBatches()[0];
        self::assertInstanceOf(RecordBatch::class, $batch);

        self::assertSame(TimestampType::LOG_APPEND_TIME, $batch->getTimestampType());
        self::assertSame(
            $createTime,
            $batch->getFirstTimestamp(),
            'the broker leaves the CreateTime of the producer in the first timestamp'
        );
        self::assertGreaterThanOrEqual($before, $batch->getMaxTimestamp());
        self::assertLessThanOrEqual($after, $batch->getMaxTimestamp());

        foreach ($region->getRecords() as $record) {
            self::assertSame(TimestampType::LOG_APPEND_TIME, $record->timestampType);
            self::assertSame($batch->getMaxTimestamp(), $record->timestamp, 'every record answers the append time');
        }

        // Converted down for a Fetch v3, the batch is rebuilt with a magic 1 builder, which stamps the append time
        // and bit 3 of the attributes on every single message
        foreach ($this->fetch(0, 3, 0, $topic)->getRecords() as $record) {
            self::assertSame(TimestampType::LOG_APPEND_TIME, $record->timestampType);
            self::assertSame($batch->getMaxTimestamp(), $record->timestamp);
        }
    }

    public function testTheBrokerRefusesABatchWhoseChecksumDoesNotMatchItsContents(): void
    {
        $batch  = RecordBatch::fromRecords([new Record('alpha', null, 0, null, self::now(), TimestampType::CREATE_TIME)]);
        $buffer = $batch->toBuffer();
        // Flip a byte of the value, which the CRC-32C covers, without recomputing the checksum
        $buffer[RecordBatch::OVERHEAD + 6] = 'A';

        $answer = $this->probe->produce($this->topic, self::PARTITION, $buffer);

        self::assertSame(self::CORRUPT_MESSAGE, $answer['errorCode'], 'the broker validates the CRC-32C on append');
    }

    public function testTheBrokerConvertsTheLogDownForEveryFetchBelowVersion4(): void
    {
        $createTime = self::now();
        $this->produce(RecordBatch::fromRecords([
            new Record('alpha', null, 0, null, $createTime, TimestampType::CREATE_TIME),
            new Record('bravo', 'key', 0, null, $createTime + 10, TimestampType::CREATE_TIME),
        ]));

        $asV2 = $this->fetch(0, 4);
        $asV1 = $this->fetch(0, 3);
        $asV0 = $this->fetch(0, 1);

        self::assertSame(RecordBatch::MAGIC, $asV2->getMagic());
        self::assertSame(Message::MAGIC_V1, $asV1->getMagic(), 'a Fetch v3 is answered in the message format v1');
        self::assertSame(Message::MAGIC_V0, $asV0->getMagic(), 'a Fetch v1 is answered in the message format v0');

        foreach ([$asV2, $asV1, $asV0] as $region) {
            self::assertSame(
                ['alpha', 'bravo'],
                array_map(static fn(Record $r): ?string => $r->value, $region->getRecords())
            );
            self::assertSame([0, 1], array_map(static fn(Record $r): ?int => $r->offset, $region->getRecords()));
        }
        self::assertSame(
            [$createTime, $createTime + 10],
            array_map(static fn(Record $r): ?int => $r->timestamp, $asV1->getRecords())
        );
        self::assertSame(
            [null, null],
            array_map(static fn(Record $r): ?int => $r->timestamp, $asV0->getRecords()),
            'the message format v0 has no timestamp field at all'
        );
        self::assertCount(2, $asV1->getBatches(), 'a converted batch becomes one message per record');
    }

    public function testTheLogOfAnOlderTopicFormatIsAnsweredInThatFormat(): void
    {
        $topic = self::uniqueTopicName('t2-rbv2-v010');
        $this->createTopic($topic, ['message.format.version=0.10.0']);

        $createTime = self::now();
        $answer     = $this->produce(
            RecordBatch::fromRecords([new Record('alpha', null, 0, null, $createTime, TimestampType::CREATE_TIME)]),
            $topic
        );

        self::assertSame(0, $answer['errorCode'], 'a Produce v3 with a record batch is accepted anyway');

        $region = $this->fetch(0, 4, 0, $topic);

        self::assertSame(
            Message::MAGIC_V1,
            $region->getMagic(),
            'the broker converted the batch down to the message format of the topic on write'
        );
        self::assertSame('alpha', $region->getRecords()[0]->value);
        self::assertSame($createTime, $region->getRecords()[0]->timestamp);
    }

    public function testACommittedTransactionLeavesADataBatchAndAControlBatchInTheLog(): void
    {
        $transactionalId = self::uniqueTopicName('t2-rbv2-txn');
        $producer        = $this->initTransactionalProducer($transactionalId);

        $added = $this->probe->addPartitionsToTxn(
            $transactionalId,
            $producer['producerId'],
            $producer['producerEpoch'],
            $this->topic,
            self::PARTITION
        );
        self::assertSame(0, $added['errorCode']);

        $createTime = self::now();
        $batch      = RecordBatch::fromRecords(
            [
                new Record('committed-1', null, 0, null, $createTime, TimestampType::CREATE_TIME),
                new Record('committed-2', null, 0, null, $createTime + 5, TimestampType::CREATE_TIME),
            ],
            CompressionCodec::NONE,
            0,
            $producer['producerId'],
            $producer['producerEpoch'],
            0,
            true
        );
        $written = $this->produce($batch, $this->topic, $transactionalId);
        self::assertSame(0, $written['errorCode']);

        $ended = $this->probe->endTxn(
            $transactionalId,
            $producer['producerId'],
            $producer['producerEpoch'],
            true
        );
        self::assertSame(0, $ended['errorCode']);

        $data = $this->fetch()->getBatches()[0];
        self::assertInstanceOf(RecordBatch::class, $data);
        self::assertTrue($data->isTransactional());
        self::assertFalse($data->isControlBatch());
        self::assertSame($producer['producerId'], $data->getProducerId());
        self::assertSame($producer['producerEpoch'], $data->getProducerEpoch());
        self::assertSame(0, $data->getBaseSequence());
        self::assertSame(1, $data->getLastSequence());

        // The coordinator appends the marker asynchronously, right behind the data batch
        $control = $this->awaitControlBatch(2);

        self::assertTrue($control->isControlBatch());
        self::assertTrue($control->isTransactional());
        self::assertSame($producer['producerId'], $control->getProducerId());
        self::assertSame(RecordBatch::NO_SEQUENCE, $control->getBaseSequence(), 'a marker has no sequence number');
        self::assertSame(1, $control->count());

        $marker = EndTransactionMarker::fromRecord($control->getRecords()[0]);
        self::assertSame(ControlRecordType::COMMIT, $marker->controlType);
        self::assertSame(0, $marker->coordinatorEpoch);

        // What a client hands to an application is the data alone
        $region = MemoryRecords::fromBuffer($this->fetchBytes(0, 4));
        self::assertSame(
            ['committed-1', 'committed-2'],
            array_map(static fn(Record $r): ?string => $r->value, $region->getRecords())
        );
    }

    public function testTheLastStableOffsetIsOnlyAnsweredForReadCommitted(): void
    {
        $this->produce(
            RecordBatch::fromRecords([new Record('alpha', null, 0, null, self::now(), TimestampType::CREATE_TIME)])
        );

        $uncommitted = $this->probe->fetch($this->topic, self::PARTITION, 0, 4, 0);
        $committed   = $this->probe->fetch($this->topic, self::PARTITION, 0, 4, 1);

        self::assertSame(0, $uncommitted['errorCode']);
        self::assertSame(1, $uncommitted['highWaterMarkOffset']);
        self::assertSame(-1, $uncommitted['lastStableOffset'], 'read_uncommitted answers no last stable offset');
        self::assertNull($uncommitted['abortedTransactions'], 'and a null aborted transaction array');

        self::assertSame(1, $committed['lastStableOffset']);
        self::assertSame([], $committed['abortedTransactions'], 'read_committed answers an array, empty or not');
    }

    /**
     * Waits until the transaction coordinator has appended the control batch at the given offset
     */
    private function awaitControlBatch(int $offset): RecordBatch
    {
        $deadline = microtime(true) + 20.0;
        while (microtime(true) < $deadline) {
            $region = MemoryRecords::fromBuffer($this->fetchBytes($offset, 4));
            foreach ($region->getBatches() as $batch) {
                if ($batch instanceof RecordBatch && $batch->isControlBatch()) {
                    return $batch;
                }
            }
            usleep(250000);
        }

        self::fail('The transaction coordinator did not append a control batch');
    }

    /**
     * Produces one record batch with a raw Produce v3 frame
     *
     * @return array{throttleTimeMs: int, errorCode: int, baseOffset: int, logAppendTime: int}
     */
    private function produce(RecordBatch $batch, ?string $topic = null, ?string $transactionalId = null): array
    {
        return $this->probe->produce(
            $topic ?? $this->topic,
            self::PARTITION,
            $batch->toBuffer(),
            $transactionalId
        );
    }

    /**
     * Reads the partition back with a raw Fetch frame of the given version
     */
    private function fetch(
        int $offset = 0,
        int $version = 4,
        int $isolationLevel = 0,
        ?string $topic = null,
    ): MemoryRecords {
        return MemoryRecords::fromBuffer($this->fetchBytes($offset, $version, $isolationLevel, $topic));
    }

    /**
     * Reads the raw byte region of the partition, the way it goes over the wire
     */
    private function fetchBytes(
        int $offset = 0,
        int $version = 4,
        int $isolationLevel = 0,
        ?string $topic = null,
    ): string {
        $answer = $this->probe->fetch($topic ?? $this->topic, self::PARTITION, $offset, $version, $isolationLevel);
        self::assertSame(0, $answer['errorCode'], 'the broker answered an error code for the fetch');

        return $answer['recordSet'];
    }

    /**
     * Asks the coordinator for a producer id, retrying while the internal transaction topic is still being created
     *
     * @return array{throttleTimeMs: int, errorCode: int, producerId: int, producerEpoch: int}
     */
    private function initTransactionalProducer(string $transactionalId): array
    {
        $deadline = microtime(true) + 30.0;
        $answer   = ['errorCode' => -1, 'producerId' => -1, 'producerEpoch' => -1, 'throttleTimeMs' => 0];
        while (microtime(true) < $deadline) {
            // The lookup is what creates __transaction_state and elects the leaders of its partitions; until it has
            // happened, InitProducerId answers 16 NotCoordinatorForGroup even on a one-broker cluster
            if ($this->probe->findTransactionCoordinator($transactionalId)['errorCode'] === 0) {
                $answer = $this->probe->initProducerId($transactionalId);
                if ($answer['errorCode'] === 0) {
                    return $answer;
                }
            }
            usleep(500000);
        }

        self::fail("The broker did not hand out a producer id for {$transactionalId}: error {$answer['errorCode']}");
    }

    /**
     * Creates a topic with the given configuration through the tools of the broker container, and waits for it.
     *
     * `message.format.version` and `message.timestamp.type` are topic-level configuration entries that this line
     * cannot set through CreateTopics until the admin apis of the ticket that owns them are there, so the topic is
     * created the way the other message format suites create theirs.
     *
     * @param list<string> $configuration `key=value` entries of the topic configuration
     */
    private function createTopic(string $topic, array $configuration = []): void
    {
        $container = getenv('KAFKA_CONTAINER');
        $command   = [
            'docker', 'exec', $container === false || $container === '' ? 'kafka-1-1-1' : $container,
            '/opt/kafka/bin/kafka-topics.sh', '--zookeeper', 'localhost:2181',
            '--create', '--topic', $topic, '--partitions', '1', '--replication-factor', '1',
        ];
        foreach ($configuration as $entry) {
            $command[] = '--config';
            $command[] = $entry;
        }

        $output   = [];
        $exitCode = 0;
        exec(implode(' ', array_map(escapeshellarg(...), $command)) . ' 2>&1', $output, $exitCode);

        if ($exitCode !== 0) {
            self::markTestSkipped("The topic {$topic} can not be created: " . implode("\n", $output));
        }

        $this->awaitTopic($topic);
    }

    /**
     * Waits until the topic exists and its partitions have a leader
     */
    private function awaitTopic(string $topic): void
    {
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($topic);
    }

    /**
     * The current time in milliseconds, which is what a producer stamps a record with
     *
     * A fixed timestamp far in the past cannot be used here: the broker deletes a segment whose largest timestamp is
     * older than `log.retention.hours` at the very next retention check, however recently it was appended.
     */
    private static function now(): int
    {
        return (int) round(microtime(true) * 1000);
    }
}
