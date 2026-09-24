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
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Lz4;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\MessageV0;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\FetchRequestV1;
use Protocol\Kafka\Protocol\Request\FetchRequestV2;
use Protocol\Kafka\Protocol\Request\FetchRequestV3;
use Protocol\Kafka\Protocol\Request\FetchRequestV4;
use Protocol\Kafka\Protocol\Request\FetchResponseV4;
use Protocol\Kafka\Protocol\Request\ProduceRequestV12;
use Protocol\Kafka\Protocol\Request\ProduceRequestV2;
use Protocol\Kafka\Protocol\Request\ProduceRequestV3;
use Protocol\Kafka\Protocol\Request\ProduceResponseV12;
use Protocol\Kafka\Protocol\Request\ProduceResponseV3;
use Protocol\Kafka\Tests\Fixture\RemovedVersionProbe;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * The message format v1 of Kafka 0.10 against the Kafka 4.3.1 node of this line: what is left of it is the refusal.
 *
 * On the lines up to 3.x this class measured the format on a real broker: the timestamps and the relative offsets
 * of a Produce v2 in the message format v1, the up-conversion of a magic 0 batch on append, and the down-conversion
 * of the log to the magic 0 for a Fetch v1 and to the magic 1 for a Fetch v2. **Kafka 4.0 removed all of it**
 * (KIP-896): Produce v0 to v2 and Fetch v0 to v3 - the only versions that carry a message set - close the connection,
 * a version 3 or higher Produce refuses a magic 0 or 1 batch per partition with the 87 `INVALID_RECORD`, and the
 * topic configuration `message.format.version`, which KIP-724 had already reduced to a no-op in Kafka 3.0, is not a
 * topic configuration any more: `TopicConfig` @ 4.0.0 has no such key and the node answers it with the **40**
 * `InvalidConfiguration`, "Unknown topic config name: message.format.version". Every log of a 4.x node is a log of
 * record batches v2, and every version of Fetch it serves hands those out unconverted.
 *
 * What this class measures now is exactly that, and what is left of the codecs of the old format in the new one:
 * the lz4 frame of this client, which the KAFKA-3160 checksum of the message format v0 no longer touches. The
 * message sets of the formats v0 and v1 stay a format of this package - their classes, the vectors the lines below
 * captured on real brokers and the unit tests of the codecs - for a peer of Kafka 3.x.
 *
 * @see docs/protocol/4.3.md, sections "MessageSet and Message" and "What the broker converts, and when"
 */
#[CoversClass(MessageSet::class)]
#[CoversClass(Message::class)]
#[CoversClass(MessageV0::class)]
#[CoversClass(TimestampType::class)]
#[CoversClass(CompressionCodec::class)]
#[CoversClass(Lz4::class)]
final class MessageFormatV1Test extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t2-40-format';

    /**
     * Partition that every test of this class produces to and fetches from
     */
    private const int PARTITION = 0;

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * Topic of the current test, created and given a leader by {@see MessageFormatV1Test::setUp()}
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t2-40-message-format');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
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
    public function testAMessageSetOfTheFormatV1CanNotReachTheLogAnyMore(int $codec): void
    {
        $messageSet = MessageSet::fromRecords(
            [
                new Record('alpha', 'first', 0, null, self::currentTimestampMs()),
                new Record('bravo', null, 0, null, self::currentTimestampMs()),
            ],
            $codec,
            Message::MAGIC_V1
        );

        // The Produce v2 that carried it costs the connection ...
        self::assertSame(
            RemovedVersionProbe::CLOSED,
            new RemovedVersionProbe(self::firstBootstrapServer())->send(new ProduceRequestV2(
                [$this->topic => [self::PARTITION => $messageSet]],
                1,
                self::PRODUCE_TIMEOUT_MS,
                self::CLIENT_ID,
                1
            ))
        );

        // ... and every version the node serves refuses the magic 1 with the 87 of KIP-467, the lowest and the
        // highest alike
        $stream = $this->connect();
        new ProduceRequestV3([$this->topic => [self::PARTITION => $messageSet]], 1, self::PRODUCE_TIMEOUT_MS, self::CLIENT_ID, 2)
            ->writeTo($stream);
        $three = ProduceResponseV3::unpack($stream)->topics[$this->topic]->partitions[self::PARTITION];
        new ProduceRequestV12([$this->topic => [self::PARTITION => $messageSet]], 1, self::PRODUCE_TIMEOUT_MS, self::CLIENT_ID, 3)
            ->writeTo($stream);
        $twelve = ProduceResponseV12::unpack($stream)->topics[$this->topic]->partitions[self::PARTITION];

        self::assertSame(KafkaException::INVALID_RECORD, $three->errorCode);
        self::assertSame(KafkaException::INVALID_RECORD, $twelve->errorCode);
        self::assertSame(-1, $twelve->baseOffset, 'nothing was appended');
    }

    public function testTheTopicConfigurationMessageFormatVersionIsGone(): void
    {
        // KIP-724 made it a no-op in Kafka 3.0 ("This configuration will be ignored if the inter.broker.protocol.version
        // is 3.0 or newer"), and Kafka 4.0 removed the key: `TopicConfig` @ 4.0.0 does not declare it any more
        $topic  = self::uniqueTopicName('t2-40-message-format-version');
        $errors = $this->admin()->createTopics([
            new NewTopic($topic, 1, 1, [], ['message.format.version' => '0.10.0']),
        ]);

        self::assertInstanceOf(InvalidConfigException::class, $errors[$topic]);
        self::assertSame(KafkaException::INVALID_CONFIG, $errors[$topic]->getCode());
        self::assertStringContainsString(
            'Unknown topic config name: message.format.version',
            $errors[$topic]->getMessage()
        );
    }

    public function testTheFetchVersionsThatConvertedTheLogDownCloseTheConnection(): void
    {
        $baseOffset = $this->produce(RecordBatch::fromRecords(
            [new Record('magic two', 'k')->withCreateTime(self::currentTimestampMs())]
        )->toBuffer());
        $probe      = new RemovedVersionProbe(self::firstBootstrapServer());

        // A 3.9.2 node converted its v2 log down to the magic 0 for a Fetch v1 and to the magic 1 for a Fetch v2
        // and v3; `FetchRequest.json` @ 4.0.0 starts at version 4, the first that reads a record batch v2
        foreach ([1 => FetchRequestV1::class, 2 => FetchRequestV2::class, 3 => FetchRequestV3::class] as $version => $class) {
            self::assertSame(
                RemovedVersionProbe::CLOSED,
                $probe->send(new $class([$this->topic => [self::PARTITION => $baseOffset]], 100, 1, 65536, -1, self::CLIENT_ID, 4)),
                "a Fetch v{$version}"
            );
        }

        $stream = $this->connect();
        new FetchRequestV4([$this->topic => [self::PARTITION => $baseOffset]], 100, 1, 65536, -1, self::CLIENT_ID, 5)
            ->writeTo($stream);
        $partition = FetchResponseV4::unpack($stream)->topics[$this->topic]->partitions[self::PARTITION];

        self::assertSame(RecordBatch::MAGIC, $partition->getRecords()->getMagic(), 'the log, as it lies');
        self::assertSame('magic two', $partition->getRecords()->getRecords()[0]->value);
    }

    public function testTheLz4FrameOfThisClientSurvivesTheBrokerUntouched(): void
    {
        // The topic keeps the codec of the producer (`compression.type=producer`), so the node validates the frame
        // - it decompresses the batch to check its records - and stores the very bytes it was given
        $records    = [new Record(str_repeat('a repetitive payload that compresses well. ', 50), 'lz4')
            ->withCreateTime(self::currentTimestampMs())];
        $baseOffset = $this->produce(RecordBatch::fromRecords($records, CompressionCodec::LZ4)->toBuffer());

        $stream = $this->connect();
        new FetchRequestV4([$this->topic => [self::PARTITION => $baseOffset]], 100, 1, 65536, -1, self::CLIENT_ID, 6)
            ->writeTo($stream);
        $raw = (string) FetchResponseV4::unpack($stream)->topics[$this->topic]->partitions[self::PARTITION]->messageSet;

        // The 61 bytes of the batch header, then the lz4 frame of the records: with the CORRECT descriptor checksum,
        // because the KAFKA-3160 quirk belonged to the message format v0, which no log of a 4.x node holds
        self::assertSame(
            bin2hex(Lz4::frameDescriptor(false)),
            bin2hex(substr($raw, 61, strlen(Lz4::frameDescriptor(false)))),
            'the frame of a record batch v2 carries the correct descriptor checksum'
        );
        $batches = RecordBatch::fromBuffer($raw);
        self::assertSame($records[0]->value, $batches->getRecords()[0]->value);
    }

    /**
     * Produces a record batch into the partition under test with a Produce **v3** and returns its base offset
     */
    private function produce(string $recordBatch): int
    {
        $stream = $this->connect();
        new ProduceRequestV3(
            [$this->topic => [self::PARTITION => $recordBatch]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            7
        )->writeTo($stream);

        $partition = ProduceResponseV3::unpack($stream)->topics[$this->topic]->partitions[self::PARTITION];
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);

        return $partition->baseOffset;
    }

    private function admin(): AdminClient
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * The current time in milliseconds: a record stamped with a timestamp of the past falls to the retention
     */
    private static function currentTimestampMs(): int
    {
        return (int) round(microtime(true) * 1000);
    }
}
