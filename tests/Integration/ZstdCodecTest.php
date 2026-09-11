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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnsupportedCompressionTypeException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV9;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV9;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV6;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV6;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * **KIP-110** (Kafka 2.1): the zstd codec, and the two versions that are allowed to speak it.
 *
 * The compression type 4 exists in the message format v2 alone, and Kafka guards it with two version rules that
 * this class measures against the container: a **fetch** below version 10 of a topic that is configured
 * `compression.type=zstd` is refused with the error code 76, and a **produce** below version 7 whose record set
 * is compressed with zstd is refused with the same code. The client-side half of the code is the
 * {@see UnsupportedCompressionTypeException} that this package raises without `ext-zstd`.
 *
 * @see docs/protocol/2.8.md, sections "The zstd codec (Kafka 2.1, KIP-110)" and "Version 10 and the zstd codec (KIP-110)"
 */
#[CoversClass(CompressionCodec::class)]
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchResponsePartition::class)]
#[CoversClass(ProduceRequest::class)]
#[CoversClass(UnsupportedCompressionTypeException::class)]
final class ZstdCodecTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t2-zstd';

    private const int PRODUCE_TIMEOUT_MS = 5000;

    private const int FETCH_MAX_WAIT_MS = 500;

    /**
     * A topic created with `compression.type=zstd`, so that the broker recompresses everything it appends
     */
    private string $zstdTopic;

    /**
     * A topic on the default `compression.type=producer`, which keeps the codec of the producer
     */
    private string $plainTopic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->zstdTopic  = self::uniqueTopicName('t2-21-zstd');
        $this->plainTopic = self::uniqueTopicName('t2-21-zstd-plain');
        self::createTopic($this->zstdTopic, ['compression.type' => 'zstd']);
        self::createTopic($this->plainTopic);

        $probe = new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID);
        $probe->awaitTopicWithLeaders($this->zstdTopic);
        $probe->awaitTopicWithLeaders($this->plainTopic);
    }

    protected function tearDown(): void
    {
        self::deleteTopic($this->zstdTopic);
        self::deleteTopic($this->plainTopic);

        parent::tearDown();
    }

    public function testAZstdTopicIsRefusedToAFetchBelowVersionTenWithSeventySix(): void
    {
        // The records are produced uncompressed; the topic configuration is what turns them into a zstd batch,
        // because a `compression.type` other than `producer` makes the broker recompress on append
        $this->produce($this->zstdTopic, 'zstd-value', ProduceRequest::class, 1000);

        $refused = $this->fetch($this->zstdTopic, FetchRequestV9::class, FetchResponseV9::class, 1001);

        self::assertSame(KafkaException::UNSUPPORTED_COMPRESSION_TYPE, $refused->errorCode);
        self::assertSame(76, KafkaException::UNSUPPORTED_COMPRESSION_TYPE);
        self::assertInstanceOf(
            UnsupportedCompressionTypeException::class,
            KafkaException::fromCode($refused->errorCode, ['topic' => $this->zstdTopic])
        );
        self::assertSame(-1, $refused->highWaterMarkOffset, 'the refused partition carries no high water mark');
        self::assertSame('', $refused->messageSet, 'and no records at all');
    }

    public function testTheSamePartitionIsServedToAFetchOfVersionTen(): void
    {
        $this->produce($this->zstdTopic, 'zstd-value', ProduceRequest::class, 1010);

        $served = $this->fetch($this->zstdTopic, FetchRequest::class, FetchResponse::class, 1011);

        self::assertSame(KafkaException::NO_ERROR, $served->errorCode);

        // The header of a record batch is always plain, so the codec of the answer can be read without the
        // extension: `RecordBatch::fromBuffer()` decodes the 61 header bytes and leaves the records block alone
        $batch = RecordBatch::fromBuffer($served->messageSet);

        self::assertSame(
            CompressionCodec::ZSTD,
            $batch->getCompressionCodec(),
            'the broker answers the batch as it lies, with the compression type 4 in its attributes'
        );
        self::assertSame(1, $batch->count(), 'and the record count of a compressed batch is in that plain header');

        // Reading the records out of it is what needs `ext-zstd`; without it the client says so instead of
        // handing half-decoded bytes to the application - the client-side half of the code 76
        if (CompressionCodec::isZstdAvailable()) {
            self::assertSame(
                ['zstd-value'],
                array_map(static fn(Record $record): ?string => $record->value, $batch->getRecords())
            );
        } else {
            $this->expectException(UnsupportedCompressionTypeException::class);
            $batch->getRecords();
        }
    }

    public function testAZstdRecordSetIsRefusedToAProduceBelowVersionSevenWithSeventySix(): void
    {
        $zstdBatch = $this->zstdBatchOfTheBroker();

        $refused = $this->produceBatch($this->plainTopic, $zstdBatch, ProduceRequestV6::class, ProduceResponseV6::class, 1020);

        // `ProduceRequest.validateRecords` @ 2.8.2 refuses the codec below version 7 while it parses the request,
        // and the partition carries the code instead of the connection being closed
        self::assertSame(KafkaException::UNSUPPORTED_COMPRESSION_TYPE, $refused->errorCode);
        self::assertSame(-1, $refused->baseOffset);
        self::assertSame(-1, $refused->logStartOffset);

        $accepted = $this->produceBatch($this->plainTopic, $zstdBatch, ProduceRequest::class, ProduceResponse::class, 1021);

        self::assertSame(KafkaException::NO_ERROR, $accepted->errorCode, 'version 7 is the version that may');
        self::assertGreaterThanOrEqual(0, $accepted->baseOffset);
    }

    public function testAZstdBatchInATopicThatIsNotConfiguredForItIsServedToAFetchVersionNine(): void
    {
        // The refusal of a fetch is decided by the CONFIGURATION of the topic, not by the records: the very same
        // batch that a `compression.type=zstd` topic is refused for is served from a default topic unchecked
        $this->produceBatch(
            $this->plainTopic,
            $this->zstdBatchOfTheBroker(),
            ProduceRequest::class,
            ProduceResponse::class,
            1030
        );

        $served = $this->fetch($this->plainTopic, FetchRequestV9::class, FetchResponseV9::class, 1031);

        self::assertSame(KafkaException::NO_ERROR, $served->errorCode);
        self::assertSame(
            CompressionCodec::ZSTD,
            RecordBatch::fromBuffer($served->messageSet)->getCompressionCodec()
        );
    }

    /**
     * Returns a zstd-compressed record batch, compressed by the broker itself
     *
     * A PHP process without `ext-zstd` can not build one, and it does not have to: a topic configured
     * `compression.type=zstd` recompresses whatever it is given, so the bytes of its log are a zstd batch that
     * can be produced again elsewhere.
     */
    private function zstdBatchOfTheBroker(): string
    {
        $this->produce($this->zstdTopic, 'zstd-value', ProduceRequest::class, 1040);

        $partition = $this->fetch($this->zstdTopic, FetchRequest::class, FetchResponse::class, 1041);
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);

        $batch = RecordBatch::fromBuffer($partition->messageSet);
        self::assertSame(CompressionCodec::ZSTD, $batch->getCompressionCodec());

        return $batch->toBuffer();
    }

    /**
     * Produces one uncompressed record of the message format v2 to the partition 0 of a topic
     *
     * @param class-string<ProduceRequest> $requestClass
     */
    private function produce(string $topic, string $value, string $requestClass, int $correlationId): void
    {
        $batch = RecordBatch::fromRecords(
            [new Record($value, null, 0, null, (int) round(microtime(true) * 1000))]
        )->toBuffer();

        $partition = $this->produceBatch($topic, $batch, $requestClass, ProduceResponse::class, $correlationId);
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode, "the record was not appended to {$topic}");
    }

    /**
     * Produces a ready-made record set to the partition 0 of a topic and returns the partition of the answer
     *
     * @param class-string<ProduceRequest>  $requestClass
     * @param class-string<ProduceResponse> $responseClass
     */
    private function produceBatch(
        string $topic,
        string $recordSet,
        string $requestClass,
        string $responseClass,
        int $correlationId
    ): \Protocol\Kafka\Protocol\Data\ProduceResponsePartition {
        $stream = $this->connect();
        new $requestClass(
            [$topic => [0 => $recordSet]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            $correlationId
        )->writeTo($stream);

        return $responseClass::unpack($stream)->topics[$topic]->partitions[0];
    }

    /**
     * Fetches the partition 0 of a topic from the offset 0 with the given request and response classes
     *
     * @param class-string<FetchRequest>  $requestClass
     * @param class-string<FetchResponse> $responseClass
     */
    private function fetch(
        string $topic,
        string $requestClass,
        string $responseClass,
        int $correlationId
    ): FetchResponsePartition {
        $stream = $this->connect();
        new $requestClass(
            [$topic => [0 => 0]],
            self::FETCH_MAX_WAIT_MS,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            $correlationId
        )->writeTo($stream);

        return $responseClass::unpack($stream)->topics[$topic]->partitions[0];
    }


    /**
     * Deletes a topic of this test through the `kafka-topics.sh` of the container, so that the shared broker does
     * not accumulate the topics of every run
     */
    private static function deleteTopic(string $topic): void
    {
        $container = getenv('KAFKA_CONTAINER');
        $container = $container === false || trim($container) === '' ? 'kafka-2-8-2' : trim($container);

        $output   = [];
        $exitCode = 0;
        exec(
            sprintf(
                'docker exec %s /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete'
                . ' --topic %s 2>&1',
                escapeshellarg($container),
                escapeshellarg($topic)
            ),
            $output,
            $exitCode
        );
    }

    /**
     * Creates a topic with the given topic-level options through the `kafka-topics.sh` of the container
     *
     * @param array<string, string> $configuration Topic-level options, as `name => value`
     */
    private static function createTopic(string $topic, array $configuration = []): void
    {
        $options = '';
        foreach ($configuration as $name => $value) {
            $options .= ' --config ' . escapeshellarg("{$name}={$value}");
        }

        $container = getenv('KAFKA_CONTAINER');
        $container = $container === false || trim($container) === '' ? 'kafka-2-8-2' : trim($container);
        $command   = sprintf(
            'docker exec %s /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create'
            . ' --if-not-exists --topic %s --partitions 1 --replication-factor 1%s 2>&1',
            escapeshellarg($container),
            escapeshellarg($topic),
            $options
        );

        $output   = [];
        $exitCode = 0;
        exec($command, $output, $exitCode);

        if ($exitCode !== 0) {
            self::fail("Can not create the topic {$topic}: " . implode("\n", $output));
        }
    }
}
