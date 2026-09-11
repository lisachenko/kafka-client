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
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV1;
use Protocol\Kafka\Protocol\Request\FetchRequestV3;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV1;
use Protocol\Kafka\Protocol\Request\FetchResponseV3;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * **KIP-283** (Kafka 2.0): the down-conversion of a log for an old client, and the switch that refuses it.
 *
 * Down-conversion is what a broker does when the message format of the log is newer than the Fetch version that
 * asks for it: a record batch v2 is rewritten as a message set v1 for a Fetch v2 or v3, and as a message set v0
 * for a Fetch v0 or v1. It is the most expensive thing a broker does for a client - before Kafka 2.0 the whole
 * converted region was built in the heap before the answer was written - so KIP-283 made it lazy and chunked and
 * added a switch that refuses it outright: `message.downconversion.enable` per topic,
 * `log.message.downconversion.enable` for the broker.
 *
 * This class measures both halves against the container: what the conversion does to a magic 2 log, and what a
 * topic with the switch off answers instead.
 *
 * @see docs/protocol/2.8.md, sections "What the broker converts, and when" and "Fetch API (key 1, v0 to v10)"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchResponsePartition::class)]
#[CoversClass(MemoryRecords::class)]
final class DownConversionTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t2-downconversion';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * How long a fetch waits for its answer, in milliseconds
     */
    private const int FETCH_MAX_WAIT_MS = 500;

    /**
     * Topic of the current test, on the default `message.format.version` of the broker (`2.8-IV1`, magic 2)
     */
    private string $topic;

    /**
     * A topic of the same format created with `message.downconversion.enable=false`
     */
    private string $refusingTopic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic         = self::uniqueTopicName('t2-downconv');
        $this->refusingTopic = self::uniqueTopicName('t2-downconv-off');

        self::createTopic($this->topic);
        self::createTopic($this->refusingTopic, ['message.downconversion.enable' => 'false']);

        $probe = new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID);
        $probe->awaitTopicWithLeaders($this->topic);
        $probe->awaitTopicWithLeaders($this->refusingTopic);
    }

    public function testAMagicTwoLogIsConvertedDownToTheFormatTheFetchVersionUnderstands(): void
    {
        $this->produce($this->topic, 'converted');

        $magic0 = $this->fetch($this->topic, FetchRequestV1::class, FetchResponseV1::class, 810);
        $magic1 = $this->fetch($this->topic, FetchRequestV3::class, FetchResponseV3::class, 811);
        $magic2 = $this->fetch($this->topic, FetchRequest::class, FetchResponse::class, 812);

        foreach (['v1' => $magic0, 'v3' => $magic1, 'v8' => $magic2] as $version => $partition) {
            self::assertSame(KafkaException::NO_ERROR, $partition->errorCode, "the {$version} fetch is served");
            self::assertSame(
                ['converted'],
                array_map(
                    static fn(Record $record): ?string => $record->value,
                    $partition->getRecords()->getRecords()
                ),
                "the record survives the conversion for a {$version} fetch"
            );
        }

        // `KafkaApis.handleFetchRequest` @ 2.8.2 converts on two conditions at once, the magic of the log and the
        // version of the request: `versionId <= 1` asks for magic 0, `versionId <= 3` for magic 1, nothing else
        // converts at all
        self::assertSame(Message::MAGIC_V0, $magic0->getRecords()->getMagic());
        self::assertSame(Message::MAGIC_V1, $magic1->getRecords()->getMagic());
        self::assertSame(RecordBatch::MAGIC, $magic2->getRecords()->getMagic());
    }

    public function testATopicWithTheDownConversionSwitchedOffRefusesTheFetchWithTheErrorCode35(): void
    {
        $this->produce($this->refusingTopic, 'never converted');

        $refused = $this->fetch($this->refusingTopic, FetchRequestV3::class, FetchResponseV3::class, 820);

        // **35 UNSUPPORTED_VERSION, not 43.** `KafkaApis.handleFetchRequest` @ 2.8.2: "if down-conversion is
        // disabled for the particular partition ... sending unsupported version response". 43
        // UNSUPPORTED_FOR_MESSAGE_FORMAT is the code of a timestamp lookup on a log below magic 1 and never
        // appears here.
        self::assertSame(KafkaException::UNSUPPORTED_VERSION, $refused->errorCode);
        self::assertSame(35, KafkaException::UNSUPPORTED_VERSION);
        self::assertInstanceOf(
            UnsupportedVersionException::class,
            KafkaException::fromCode($refused->errorCode, ['topic' => $this->refusingTopic])
        );
        self::assertSame(-1, $refused->highWaterMarkOffset, 'the error carries no high water mark either');
        self::assertSame('', $refused->messageSet, 'and an empty record set, not a converted one');
        self::assertSame([], $refused->getRecords()->getRecords());
    }

    public function testTheSamePartitionIsServedToAFetchThatNeedsNoConversion(): void
    {
        $this->produce($this->refusingTopic, 'served as it lies');

        $served = $this->fetch($this->refusingTopic, FetchRequest::class, FetchResponse::class, 821);

        // The switch refuses the conversion, not the partition: a client that asks with a version the log already
        // speaks is served exactly as it would be on any other topic
        self::assertSame(KafkaException::NO_ERROR, $served->errorCode);
        self::assertSame(RecordBatch::MAGIC, $served->getRecords()->getMagic());
        self::assertSame(
            ['served as it lies'],
            array_map(
                static fn(Record $record): ?string => $record->value,
                $served->getRecords()->getRecords()
            )
        );
    }

    public function testTheRefusalIsPerPartitionAndLeavesTheConnectionOpen(): void
    {
        $this->produce($this->topic, 'convertible');
        $this->produce($this->refusingTopic, 'not convertible');

        $stream = $this->connect();
        new FetchRequestV3(
            [$this->topic => [0 => 0], $this->refusingTopic => [0 => 0]],
            self::FETCH_MAX_WAIT_MS,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            830
        )->writeTo($stream);

        $response = FetchResponseV3::unpack($stream);

        self::assertSame(830, $response->getCorrelationId());
        self::assertSame(
            KafkaException::NO_ERROR,
            $response->topics[$this->topic]->partitions[0]->errorCode,
            'the convertible partition of the same request is served'
        );
        self::assertSame(
            KafkaException::UNSUPPORTED_VERSION,
            $response->topics[$this->refusingTopic]->partitions[0]->errorCode
        );

        // The connection survives it, which an unparsable frame would not
        new FetchRequestV3([$this->topic => [0 => 0]], self::FETCH_MAX_WAIT_MS, 1, 65536, -1, self::CLIENT_ID, 831)
            ->writeTo($stream);
        self::assertSame(831, FetchResponseV3::unpack($stream)->getCorrelationId());
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

    /**
     * Produces one record of the message format v2 to the partition 0 of the given topic
     */
    private function produce(string $topic, string $value): void
    {
        $stream = $this->connect();
        new ProduceRequest(
            [$topic => [0 => RecordBatch::fromRecords(
                [new Record($value, null, 0, null, (int) round(microtime(true) * 1000))]
            )]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            800
        )->writeTo($stream);

        $partition = ProduceResponse::unpack($stream)->topics[$topic]->partitions[0];
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode, "the record was not appended to {$topic}");
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

        $response = $responseClass::unpack($stream);
        self::assertSame($correlationId, $response->getCorrelationId());

        return $response->topics[$topic]->partitions[0];
    }
}
