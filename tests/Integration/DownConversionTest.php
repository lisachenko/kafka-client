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
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV1;
use Protocol\Kafka\Protocol\Request\FetchRequestV3;
use Protocol\Kafka\Protocol\Request\FetchRequestV4;
use Protocol\Kafka\Protocol\Request\FetchRequestV9;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV4;
use Protocol\Kafka\Protocol\Request\FetchResponseV9;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\RemovedVersionProbe;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * **KIP-283** (Kafka 2.0): the down-conversion of a log for an old client, and the switch that refused it - both gone.
 *
 * Down-conversion was what a broker did when the message format of the log was newer than the Fetch version that
 * asked for it: a record batch v2 was rewritten as a message set v1 for a Fetch v2 or v3, and as a message set v0
 * for a Fetch v0 or v1. It was the most expensive thing a broker did for a client, so KIP-283 made it lazy and
 * chunked and added a switch that refused it outright: `message.downconversion.enable` per topic,
 * `log.message.downconversion.enable` for the broker, which a 3.9.2 node answered per partition with the 35.
 *
 * **Kafka 4.0 removed the conversion together with the versions that needed it (KIP-896)**: `FetchRequest.json` @
 * 4.0.0 starts at version 4, the first that reads a record batch v2, a frame of Fetch v0 to v3 closes the connection,
 * and the switch went with them - `TopicConfig` @ 4.0.0 still names the constant, deprecated, but the node refuses
 * the topic configuration with the **40** `InvalidConfiguration`, "Unknown topic config name:
 * message.downconversion.enable". This class measures that, and that every version the node serves answers the log
 * as it lies.
 *
 * @see docs/protocol/4.3.md, sections "What the broker converts, and when" and "Fetch API (key 1, v0 to v17)"
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
    private const string CLIENT_ID = 'kafka-client-t2-40-downconversion';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * How long a fetch waits for its answer, in milliseconds
     */
    private const int FETCH_MAX_WAIT_MS = 500;

    /**
     * Topic of the current test, a log of record batches v2 like every log of a 4.x node
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t2-40-downconv');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    public function testAFetchThatNeededAConversionCostsTheConnection(): void
    {
        $this->produce($this->topic, 'never converted');

        // A 3.9.2 node converted the log to the magic 0 for a Fetch v1 and to the magic 1 for a Fetch v3
        $probe = new RemovedVersionProbe(self::firstBootstrapServer());
        foreach ([1 => FetchRequestV1::class, 3 => FetchRequestV3::class] as $version => $class) {
            self::assertSame(
                RemovedVersionProbe::CLOSED,
                $probe->send(new $class([$this->topic => [0 => 0]], self::FETCH_MAX_WAIT_MS, 1, 65536, -1, self::CLIENT_ID, 810)),
                "a Fetch v{$version} is not converted for any more, it is refused by the request parser"
            );
        }
    }

    public function testEveryServedVersionAnswersTheLogAsItLies(): void
    {
        $this->produce($this->topic, 'as it lies');

        $four    = $this->fetch(FetchRequestV4::class, FetchResponseV4::class, 811);
        $nine    = $this->fetch(FetchRequestV9::class, FetchResponseV9::class, 812);
        $highest = $this->fetch(FetchRequest::class, FetchResponse::class, 813);

        foreach (['v4' => $four, 'v9' => $nine, 'v' . FetchRequest::VERSION => $highest] as $version => $partition) {
            self::assertSame(KafkaException::NO_ERROR, $partition->errorCode, "the {$version} fetch is served");
            self::assertSame(RecordBatch::MAGIC, $partition->getRecords()->getMagic(), "{$version}: the magic 2");
            self::assertSame(
                ['as it lies'],
                array_map(static fn(Record $record): ?string => $record->value, $partition->getRecords()->getRecords())
            );
        }
        self::assertSame(
            bin2hex((string) $four->messageSet),
            bin2hex((string) $highest->messageSet),
            'the very same bytes for the lowest and the highest version'
        );
    }

    public function testTheSwitchOfKip283IsNotATopicConfigurationAnyMore(): void
    {
        $topic  = self::uniqueTopicName('t2-40-downconv-off');
        $errors = $this->admin()->createTopics([
            new NewTopic($topic, 1, 1, [], ['message.downconversion.enable' => 'false']),
        ]);

        self::assertInstanceOf(InvalidConfigException::class, $errors[$topic]);
        self::assertSame(KafkaException::INVALID_CONFIG, $errors[$topic]->getCode());
        self::assertStringContainsString(
            'Unknown topic config name: message.downconversion.enable',
            $errors[$topic]->getMessage()
        );
    }

    public function testTheRefusalOfARemovedVersionIsTheConnectionNotAPartition(): void
    {
        // On 3.9.2 the switch refused a partition with the 35 and left the connection open, so the convertible
        // partition of the same request was served. A 4.x node refuses the whole frame, before any partition
        $this->produce($this->topic, 'served on the next connection');

        self::assertSame(
            RemovedVersionProbe::CLOSED,
            new RemovedVersionProbe(self::firstBootstrapServer())->send(new FetchRequestV3(
                [$this->topic => [0 => 0]],
                self::FETCH_MAX_WAIT_MS,
                1,
                65536,
                -1,
                self::CLIENT_ID,
                830
            ))
        );

        // ... and the next connection is served as if nothing had happened
        self::assertSame(
            KafkaException::NO_ERROR,
            $this->fetch(FetchRequestV4::class, FetchResponseV4::class, 831)->errorCode
        );
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
     * Fetches the partition 0 of the topic under test from the offset 0 with the given request and response classes
     *
     * @param class-string<FetchRequest>  $requestClass
     * @param class-string<FetchResponse> $responseClass
     */
    private function fetch(string $requestClass, string $responseClass, int $correlationId): FetchResponsePartition
    {
        $stream = $this->connect();
        new $requestClass(
            [$this->topic => [0 => 0]],
            self::FETCH_MAX_WAIT_MS,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            $correlationId,
            // Version 13 names the topic by its id (KIP-516); every lower version ignores the map
            topicIds: [$this->topic => self::topicIdOf($this->topic)]
        )->writeTo($stream);

        $response = $responseClass::unpack($stream);
        self::assertSame($correlationId, $response->getCorrelationId());

        return self::fetchedTopic($response, $this->topic)->partitions[0];
    }
}
