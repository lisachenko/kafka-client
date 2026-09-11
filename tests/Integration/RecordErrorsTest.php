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
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidRecordException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponseRecordError;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV7;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV7;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * **KIP-467** (Kafka 2.4): which records of a refused batch the broker refused, and why.
 *
 * A batch is validated as a whole, so one bad record refuses all of it with the error code **87**
 * `INVALID_RECORD`; up to Produce v7 that is everything the producer learns. Version 8 names the offenders by
 * their **batch index** and adds a partition-wide message. The condition is produced here with the cheapest
 * validation a client can trigger: a record without a key on a `cleanup.policy=compact` topic.
 *
 * @see docs/protocol/2.8.md, section "The record errors of a refused batch (v8, KIP-467)"
 */
#[CoversClass(ProduceRequest::class)]
#[CoversClass(ProduceResponse::class)]
#[CoversClass(ProduceResponsePartition::class)]
#[CoversClass(ProduceResponseRecordError::class)]
final class RecordErrorsTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t2-record-errors';

    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * A topic created with `cleanup.policy=compact`, whose log is keyed by definition
     */
    private string $compacted;

    protected function setUp(): void
    {
        parent::setUp();

        $this->compacted = self::uniqueTopicName('t2-24-compacted');
        self::createTopic($this->compacted, ['cleanup.policy' => 'compact']);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->compacted);
    }

    protected function tearDown(): void
    {
        self::deleteTopic($this->compacted);

        parent::tearDown();
    }

    public function testARefusedBatchNamesItsBadRecordsByTheirBatchIndex(): void
    {
        $partition = $this->produce(ProduceRequest::class, ProduceResponse::class, 1100);

        self::assertSame(KafkaException::UNSUPPORTED_FOR_MESSAGE_FORMAT + 0, 43, 'sanity: the codes are stable');
        self::assertSame(87, KafkaException::INVALID_RECORD);
        self::assertSame(KafkaException::INVALID_RECORD, $partition->errorCode);
        self::assertSame(-1, $partition->baseOffset, 'not one record of the batch was appended');
        self::assertSame(-1, $partition->logAppendTime);

        // The two key-less records are named by their position in the SENT batch, counted from 0; the first
        // record, which has a key, is not in the array
        self::assertSame([1, 2], array_keys($partition->recordErrors));
        self::assertSame(1, $partition->recordErrors[1]->batchIndex);
        self::assertSame(2, $partition->recordErrors[2]->batchIndex);
        foreach ($partition->recordErrors as $recordError) {
            self::assertStringContainsString(
                'Compacted topic cannot accept message without key',
                (string) $recordError->batchIndexErrorMessage
            );
        }
        self::assertSame('One or more records have been rejected', $partition->errorMessage);
    }

    public function testAVersionSevenRequestIsAnsweredTheErrorCodeAndNothingElse(): void
    {
        $partition = $this->produce(ProduceRequestV7::class, ProduceResponseV7::class, 1101);

        self::assertSame(KafkaException::INVALID_RECORD, $partition->errorCode);
        self::assertSame(-1, $partition->baseOffset);
        self::assertSame([], $partition->recordErrors, 'the frame of version 7 has no room for them');
        self::assertNull($partition->errorMessage);
    }

    public function testTheRecordErrorsReachTheCallerOfProduceThroughTheException(): void
    {
        $records = [
            new Record('with-a-key', 'k', 0, null, self::timestampMs()),
            new Record('without-a-key', null, 0, null, self::timestampMs()),
        ];

        try {
            $this->client()->produce([$this->compacted => [0 => $records]]);
            self::fail('a batch a compacted topic refuses has to reach the caller');
        } catch (TopicPartitionRequestException $failure) {
            // A produce reports its per-partition errors through this wrapper, one exception per partition
            $exception = $failure->getExceptions()[$this->compacted][0];
            self::assertInstanceOf(InvalidRecordException::class, $exception);

            $context = $exception->getContext();

            self::assertSame($this->compacted, $context['topic']);
            self::assertSame(0, $context['partitionId']);
            self::assertSame('One or more records have been rejected', $context['errorMessage']);
            self::assertSame([1], array_keys($context['recordErrors']), 'the second record is the bad one');
            self::assertStringContainsString(
                'Compacted topic cannot accept message without key',
                (string) $context['recordErrors'][1]
            );
        }
    }

    /**
     * Sends a batch of three records - the second and the third without a key - to the compacted topic
     *
     * @param class-string<ProduceRequest>  $requestClass
     * @param class-string<ProduceResponse> $responseClass
     */
    private function produce(string $requestClass, string $responseClass, int $correlationId): ProduceResponsePartition
    {
        $batch = RecordBatch::fromRecords([
            new Record('with-a-key', 'k', 0, null, self::timestampMs()),
            new Record('without-a-key', null, 0, null, self::timestampMs()),
            new Record('also-without-a-key', null, 0, null, self::timestampMs()),
        ])->toBuffer();

        $stream = $this->connect();
        new $requestClass(
            [$this->compacted => [0 => $batch]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            $correlationId
        )->writeTo($stream);

        return $responseClass::unpack($stream)->topics[$this->compacted]->partitions[0];
    }

    /**
     * Returns a client that talks to the broker under test
     */
    private function client(): Client
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => 1,
        ] + ProducerConfig::getDefaultConfiguration();

        return new Client(Cluster::bootstrap($configuration), $configuration);
    }

    private static function timestampMs(): int
    {
        return (int) round(microtime(true) * 1000);
    }

    /**
     * Deletes a topic of this test through the `kafka-topics.sh` of the container
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
