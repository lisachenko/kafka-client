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

namespace Protocol\Kafka\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\CorrelationIdMismatchException;
use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Errors\GroupLoadInProgressException;
use Protocol\Kafka\Common\Errors\IllegalGenerationException;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\NotLeaderForPartitionException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\RebalanceInProgressException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Network\RetryPolicy;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Tests\Compliance\MessageFields;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * Tests the low-level client against scripted brokers: the fan-out to the partition leaders, the correlation of the
 * answers, the retries after a metadata refresh and the reporting of a partially failed request.
 *
 * @see docs/protocol/0.10.2.md
 */
#[CoversClass(Client::class)]
#[CoversClass(RetryPolicy::class)]
#[CoversClass(ResponseValidator::class)]
#[CoversClass(ConnectionFactory::class)]
#[CoversClass(CoordinatorLookup::class)]
final class ClientTest extends TestCase
{
    private const string TOPIC = 'orders';

    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    private const string FIRST_LEADER = 'tcp://kafka-1:9092';

    private const string SECOND_LEADER = 'tcp://kafka-2:9093';

    private ScriptedConnections $brokers;

    /**
     * Listening socket of the broker that never answers
     *
     * @var resource|null
     */
    private $silentServer;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
        if (is_resource($this->silentServer)) {
            fclose($this->silentServer);
        }
        $this->silentServer = null;
    }

    public function testEveryPartitionLeaderGetsItsOwnRequestAndTheAnswersAreMerged(): void
    {
        $first  = new BrokerConnection(ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 17]]]));
        $second = new BrokerConnection(ResponseFrame::produce(0, [self::TOPIC => [1 => [0, 42]]]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $first)
            ->on(self::SECOND_LEADER, $second)
            ->install();

        $result = $this->client()->produce([
            self::TOPIC => [
                0 => [new Record('to the first leader')],
                1 => [new Record('to the second leader')],
            ],
        ]);

        self::assertInstanceOf(ProduceResponsePartition::class, $result[self::TOPIC][0]);
        self::assertSame(17, $result[self::TOPIC][0]->baseOffset);
        self::assertSame(42, $result[self::TOPIC][1]->baseOffset);
        self::assertSame(1, $first->getRequestCount(), 'the partitions of one leader travel in a single request');
        self::assertSame(1, $second->getRequestCount());
    }

    public function testTheThrottleTimeOfAProduceAnswerReachesEveryPartitionOfIt(): void
    {
        // Produce v1 reports the throttle time once per answer, behind the topics, and a batch is split by the
        // partition leaders, so each of those answers carries the delay of its own broker
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::produce(
                0,
                [self::TOPIC => [0 => [0, 17]]],
                793
            )))
            ->on(self::SECOND_LEADER, new BrokerConnection(ResponseFrame::produce(
                0,
                [self::TOPIC => [1 => [0, 42]]]
            )))
            ->install();

        $result = $this->client()->produce([
            self::TOPIC => [
                0 => [new Record('over the quota')],
                1 => [new Record('inside the quota')],
            ],
        ]);

        self::assertSame(793, $result[self::TOPIC][0]->throttleTimeMs);
        self::assertSame(0, $result[self::TOPIC][1]->throttleTimeMs, 'the other leader did not throttle anything');
    }

    public function testEveryRequestCarriesItsOwnCorrelationId(): void
    {
        $leader = new BrokerConnection(
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 1]]]),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 2]]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $client = $this->client();
        $client->produce([self::TOPIC => [0 => [new Record('first')]]]);
        $client->produce([self::TOPIC => [0 => [new Record('second')]]]);

        $correlationIds = $leader->getReceivedCorrelationIds();

        self::assertCount(2, $correlationIds);
        self::assertNotSame($correlationIds[0], $correlationIds[1]);
        self::assertSame($correlationIds, array_unique($correlationIds));
    }

    public function testAnAnswerWithTheWrongCorrelationIdFailsTheRequestAndDropsTheConnection(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(
                self::FIRST_LEADER,
                new BrokerConnection(ResponseFrame::produce(999, [self::TOPIC => [0 => [0, 1]]]))
                    ->withoutCorrelationIdEcho(),
                new BrokerConnection(ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 5]]]))
            )
            ->install();

        $client = $this->client();

        try {
            $client->produce([self::TOPIC => [0 => [new Record('desynchronized')]]]);
            self::fail('An answer that belongs to another request is expected to fail the request');
        } catch (TopicPartitionRequestException $exception) {
            self::assertSame([], $exception->getPartialResult());
            self::assertInstanceOf(
                CorrelationIdMismatchException::class,
                $exception->getExceptions()[self::TOPIC][0]
            );
        }

        // The stream position of that connection is unknown, so the next request has to use a fresh one
        $result = $client->produce([self::TOPIC => [0 => [new Record('on a fresh connection')]]]);

        self::assertSame(5, $result[self::TOPIC][0]->baseOffset);
        self::assertSame(2, $this->brokers->getConnectionCount(self::FIRST_LEADER));
    }

    public function testARetriableErrorIsSentAgainToTheLeaderOfTheRefreshedMetadata(): void
    {
        $movedLeader = ResponseFrame::metadata(
            0,
            [[0, 'kafka-1', 9092], [1, 'kafka-2', 9093]],
            [self::TOPIC => [0 => 1, 1 => 1]]
        );
        $staleLeader = new BrokerConnection(
            ResponseFrame::produce(0, [self::TOPIC => [0 => [KafkaException::NOT_LEADER_FOR_PARTITION, -1]]])
        );
        $newLeader = new BrokerConnection(ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 8]]]));

        $this->brokers
            ->on(
                self::BOOTSTRAP_ADDRESS,
                new BrokerConnection($this->clusterMetadata()),
                new BrokerConnection($movedLeader)
            )
            ->on(self::FIRST_LEADER, $staleLeader)
            ->on(self::SECOND_LEADER, $newLeader)
            ->install();

        $result = $this->client([ClientConfig::RETRIES => 1])
            ->produce([self::TOPIC => [0 => [new Record('follows the leader')]]]);

        self::assertSame(8, $result[self::TOPIC][0]->baseOffset);
        self::assertSame(1, $staleLeader->getRequestCount());
        self::assertSame(1, $newLeader->getRequestCount());
        self::assertSame(
            2,
            $this->brokers->getConnectionCount(self::BOOTSTRAP_ADDRESS),
            'the metadata is refreshed before the request is sent again'
        );
    }

    public function testOnlyTheFailedPartitionsAreSentAgain(): void
    {
        $leader = new BrokerConnection(
            ResponseFrame::produce(0, [
                self::TOPIC => [
                    0 => [0, 3],
                    1 => [KafkaException::LEADER_NOT_AVAILABLE, -1],
                ],
            ]),
            ResponseFrame::produce(0, [self::TOPIC => [1 => [0, 9]]])
        );
        // Both partitions are led by the same broker here, so one request carries them both
        $metadata = ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0, 1 => 0]]);
        $this->brokers
            ->on(
                self::BOOTSTRAP_ADDRESS,
                new BrokerConnection($metadata),
                new BrokerConnection($metadata)
            )
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $result = $this->client([ClientConfig::RETRIES => 1])->produce([
            self::TOPIC => [
                0 => [new Record('accepted at once')],
                1 => [new Record('accepted on the second attempt')],
            ],
        ]);

        self::assertSame(3, $result[self::TOPIC][0]->baseOffset);
        self::assertSame(9, $result[self::TOPIC][1]->baseOffset);
        self::assertSame(2, $leader->getRequestCount(), 'the partition that succeeded is not produced twice');
    }

    public function testAPartiallyFailedRequestReportsBothTheResultAndTheErrors(): void
    {
        $metadata = ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0, 1 => 0]]);
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($metadata))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::produce(0, [
                self::TOPIC => [
                    0 => [0, 12],
                    1 => [KafkaException::MESSAGE_TOO_LARGE, -1],
                ],
            ])))
            ->install();

        try {
            $this->client()->produce([
                self::TOPIC => [
                    0 => [new Record('small enough')],
                    1 => [new Record('too large')],
                ],
            ]);
            self::fail('A request that failed on one partition is expected to be reported');
        } catch (TopicPartitionRequestException $exception) {
            $partialResult = $exception->getPartialResult();
            $errors        = $exception->getExceptions();

            self::assertSame(12, $partialResult[self::TOPIC][0]->baseOffset);
            self::assertArrayNotHasKey(1, $partialResult[self::TOPIC]);
            self::assertArrayNotHasKey(0, $errors[self::TOPIC]);
            self::assertSame(
                KafkaException::MESSAGE_TOO_LARGE,
                $errors[self::TOPIC][1]->getCode(),
                'a message that is too large is not going to fit on a retry either'
            );
        }
    }

    public function testAFireAndForgetRequestIsNeverWaitedFor(): void
    {
        // acks = 0 is the only request of the protocol that the broker does not answer at all
        $leader = new BrokerConnection();
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $result = $this->client([ProducerConfig::ACKS => 0])
            ->produce([self::TOPIC => [0 => [new Record('fire and forget')]]]);

        self::assertSame([], $result);
        self::assertSame(1, $leader->getRequestCount(), 'the request is written, only its answer is not awaited');
        self::assertSame(ApiKeys::PRODUCE, $this->apiKeyOf($leader->getReceivedFrames()[0]));
    }

    /**
     * Compression type of the client and the codec that the message set of a batch has to announce
     *
     * @return \Generator<string, array{0: string, 1: int}>
     */
    public static function compressionTypes(): \Generator
    {
        yield 'gzip'   => [ProducerConfig::COMPRESSION_TYPE_GZIP, CompressionCodec::GZIP];
        yield 'snappy' => [ProducerConfig::COMPRESSION_TYPE_SNAPPY, CompressionCodec::SNAPPY];
    }

    #[DataProvider('compressionTypes')]
    public function testTheConfiguredCompressionTypeIsAppliedToTheWholeBatch(
        string $compressionType,
        int $expectedCodec
    ): void {
        $leader = new BrokerConnection(ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 5]]]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $records = [
            new Record(str_repeat('a repetitive value ', 32), 'key-0'),
            new Record(str_repeat('a repetitive value ', 32), 'key-1'),
            new Record(str_repeat('a repetitive value ', 32)),
        ];

        $this->client([ProducerConfig::COMPRESSION_TYPE => $compressionType])
            ->produce([self::TOPIC => [0 => $records]]);

        // A compressed batch travels as a message set of exactly one message, whose value is the whole batch
        $messageSetBuffer = self::messageSetOf($leader->getReceivedFrames()[0]);
        $wrapper          = self::firstMessageOf($messageSetBuffer);

        self::assertTrue($wrapper->isCompressed());
        self::assertSame($expectedCodec, $wrapper->getCompressionCodec());
        self::assertLessThan(
            MessageSet::fromRecords($records)->sizeInBytes(),
            strlen($messageSetBuffer),
            'the batch that goes over the wire is smaller than the records it holds'
        );

        // ... and it holds exactly the records it was built from, which is what the broker unwraps on append
        $sentRecords = MessageSet::fromBuffer($messageSetBuffer)->getRecords();

        self::assertCount(3, $sentRecords);
        self::assertSame(['key-0', 'key-1', null], array_column($sentRecords, 'key'));
        self::assertSame($records[0]->value, $sentRecords[0]->value);
    }

    public function testABatchIsSentAsItIsWithoutACompressionType(): void
    {
        $leader = new BrokerConnection(ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 1]]]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $this->client()->produce([self::TOPIC => [0 => [new Record('as it is', 'a key')]]]);

        $wrapper = self::firstMessageOf(self::messageSetOf($leader->getReceivedFrames()[0]));

        self::assertFalse($wrapper->isCompressed(), 'compression.type defaults to none');
        self::assertSame('as it is', $wrapper->value);
        self::assertSame('a key', $wrapper->key);
    }

    public function testAnUnsupportedCompressionTypeIsRejectedBeforeAnythingIsSent(): void
    {
        $leader = new BrokerConnection();
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        try {
            // 0.9.0.1 knows the lz4 codec, but this client neither writes nor reads it
            $this->client([ProducerConfig::COMPRESSION_TYPE => 'lz4'])
                ->produce([self::TOPIC => [0 => [new Record('never sent')]]]);
            self::fail('An unsupported compression type has to be rejected');
        } catch (InvalidConfigurationException $exception) {
            self::assertStringContainsString('lz4', $exception->getMessage());
        }

        self::assertSame(0, $leader->getRequestCount());
    }

    public function testABrokerThatNeverAnswersIsReportedAsATimeout(): void
    {
        $silentAddress = $this->startSilentBroker();
        $metadata      = ResponseFrame::metadata(
            0,
            [[0, '127.0.0.1', $this->silentPort()]],
            [self::TOPIC => [0 => 0]]
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($metadata))
            ->passThrough($silentAddress)
            ->install();

        $startedAt = microtime(true);
        try {
            $this->client([ClientConfig::REQUEST_TIMEOUT_MS => 100])
                ->fetchTopicPartitionOffsets([self::TOPIC => [0 => -1]]);
            self::fail('A broker that never answers is expected to time out');
        } catch (TopicPartitionRequestException $exception) {
            $error = $exception->getExceptions()[self::TOPIC][0];

            self::assertInstanceOf(NetworkException::class, $error);
            self::assertSame('Timeout while waiting for the response', $error->getContext()['error']);
            self::assertSame(100, $error->getContext()['timeoutMs']);
        }
        self::assertGreaterThanOrEqual(0.1, microtime(true) - $startedAt, 'request.timeout.ms is waited for');
    }

    public function testTheOffsetsOfEveryPartitionAreCollected(): void
    {
        $metadata = ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0, 1 => 0]]);
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($metadata))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::offsets(0, [
                // v0 answers with the list of segment offsets, the newest one first
                self::TOPIC => [0 => [0, [64, 32, 0]], 1 => [0, []]],
            ])))
            ->install();

        $offsets = $this->client()->fetchTopicPartitionOffsets([self::TOPIC => [0 => -1, 1 => -1]]);

        self::assertSame([self::TOPIC => [0 => 64, 1 => 0]], $offsets);
    }

    public function testTheRecordsOfAFetchAreDecoded(): void
    {
        $messageSet = MessageSet::fromRecords([new Record('first'), new Record('second', 'key')])->toBuffer();
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::fetch(0, [
                self::TOPIC => [0 => [0, 2, $messageSet]],
            ])))
            ->install();

        $records = $this->client()->fetch([self::TOPIC => [0 => 0]], 200)[self::TOPIC][0];

        self::assertCount(2, $records);
        self::assertSame('first', $records[0]->value);
        self::assertSame('key', $records[1]->key);
        self::assertSame(1, $records[1]->offset);
    }

    public function testAFetchAlsoReportsTheStateOfEveryPartition(): void
    {
        $messageSet = MessageSet::fromRecords([new Record('first'), new Record('second')])->toBuffer();
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::fetch(0, [
                self::TOPIC => [0 => [0, 40, $messageSet]],
            ])))
            ->install();

        $partition = $this->client()->fetchPartitions([self::TOPIC => [0 => 12]], 200)[self::TOPIC][0];

        self::assertInstanceOf(FetchedPartition::class, $partition);
        self::assertSame(self::TOPIC, $partition->topicPartition->topic);
        self::assertSame(0, $partition->topicPartition->partition);
        self::assertSame(12, $partition->fetchOffset);
        self::assertSame(0, $partition->errorCode);
        self::assertSame(40, $partition->highWaterMarkOffset, 'the consumer sees how far behind the log it is');
        self::assertCount(2, $partition->getRecords());
        self::assertSame(2, $partition->count());
        self::assertFalse($partition->isEmpty());
        self::assertFalse($partition->hasPartialTrailingMessage());
        self::assertFalse($partition->isSingleMessageTooLarge());
        self::assertSame(2, $partition->getNextOffset(), 'the offsets of a produced set count from 0');
        self::assertSame(0, $partition->throttleTimeMs, 'a broker without quotas never throttles');
    }

    public function testTheThrottleTimeOfAFetchAnswerReachesEveryPartitionOfIt(): void
    {
        // Fetch v1 reports the throttle time once for the whole answer, so every partition of it carries the value
        $messageSet = MessageSet::fromRecords([new Record('throttled')])->toBuffer();
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::fetch(
                0,
                [self::TOPIC => [0 => [0, 1, $messageSet]]],
                250
            )))
            ->install();

        $partition = $this->client()->fetchPartitions([self::TOPIC => [0 => 0]], 200)[self::TOPIC][0];

        self::assertSame(250, $partition->throttleTimeMs);
        self::assertCount(1, $partition->getRecords());
    }

    public function testAMessageThatDoesNotFitIntoTheFetchSizeIsVisibleWithoutASecondRequest(): void
    {
        // A 0.9.0.1 broker cuts the set off at MaxBytes without guaranteeing progress: the answer carries no
        // complete message at all although the high water mark shows that there is something to read
        $truncated = substr(MessageSet::fromRecords([new Record(str_repeat('x', 512))])->toBuffer(), 0, 40);
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::fetch(0, [
                self::TOPIC => [0 => [0, 5, $truncated]],
            ])))
            ->install();

        $partition = $this->client()->fetchPartitions([self::TOPIC => [0 => 0]], 200)[self::TOPIC][0];

        self::assertTrue($partition->isSingleMessageTooLarge());
        self::assertTrue($partition->isEmpty());
        self::assertSame(0, $partition->getNextOffset(), 'a partition without a record keeps its fetch offset');
    }

    public function testTheChecksumOfEveryMessageIsVerifiedUnlessTheConsumerOptsOut(): void
    {
        $corrupted = MessageSet::fromRecords([new Record('tampered with')])->toBuffer();
        // The CRC32 of a message is the first field of its payload, right behind Offset int64 and MessageSize int32
        $corrupted = substr_replace($corrupted, pack('N', 0xDEADBEEF), 12, 4);

        $brokerAnswer = ResponseFrame::fetch(0, [self::TOPIC => [0 => [0, 1, $corrupted]]]);
        $this->brokers
            ->on(
                self::BOOTSTRAP_ADDRESS,
                new BrokerConnection($this->clusterMetadata()),
                new BrokerConnection($this->clusterMetadata())
            )
            // The connection to a broker is cached and reused, so both fetches travel over the very same one
            ->on(self::FIRST_LEADER, new BrokerConnection($brokerAnswer, $brokerAnswer))
            ->install();

        $unchecked = $this->client([ConsumerConfig::CHECK_CRCS => false])
            ->fetchPartitions([self::TOPIC => [0 => 0]], 200)[self::TOPIC][0];

        self::assertSame('tampered with', $unchecked->getRecords()[0]->value);

        try {
            $this->client([ConsumerConfig::CHECK_CRCS => true])->fetchPartitions([self::TOPIC => [0 => 0]], 200);
            self::fail('A message whose checksum does not match is expected to be reported');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(CorruptMessageException::class, $exception->getExceptions()[self::TOPIC][0]);
        }
    }

    public function testAFetchErrorIsReportedPerPartition(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::fetch(0, [
                self::TOPIC => [0 => [KafkaException::OFFSET_OUT_OF_RANGE, 0, '']],
            ])))
            ->install();

        try {
            $this->client()->fetch([self::TOPIC => [0 => 9999]], 200);
            self::fail('An offset outside the range of the log is expected to be reported');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(OffsetOutOfRangeException::class, $exception->getExceptions()[self::TOPIC][0]);
            self::assertSame([], $exception->getPartialResult());
        }
    }

    public function testAPartiallyFailedFetchStillReturnsTheRecordsOfTheOtherPartitions(): void
    {
        $messageSet = MessageSet::fromRecords([new Record('readable')])->toBuffer();
        $metadata   = ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0, 1 => 0]]);
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($metadata))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::fetch(0, [
                self::TOPIC => [
                    0 => [0, 1, $messageSet],
                    1 => [KafkaException::OFFSET_OUT_OF_RANGE, 0, ''],
                ],
            ])))
            ->install();

        try {
            $this->client()->fetch([self::TOPIC => [0 => 0, 1 => 9999]], 200);
            self::fail('The partition with an invalid offset is expected to be reported');
        } catch (TopicPartitionRequestException $exception) {
            // T9 relies on both shapes: the records of the partitions that answered and the error of the others
            self::assertSame(['readable'], array_column($exception->getPartialResult()[self::TOPIC][0], 'value'));
            self::assertInstanceOf(OffsetOutOfRangeException::class, $exception->getExceptions()[self::TOPIC][1]);
        }
    }

    public function testACommitIsRoutedToTheCoordinatorAsVersionTwo(): void
    {
        // The coordinator lookup itself is answered by the first node of the cluster, it points at the second one
        $coordinator = new BrokerConnection(
            ResponseFrame::offsetCommit(0, [self::TOPIC => [0 => 0]]),
            ResponseFrame::offsetFetch(0, [self::TOPIC => [0 => [0, 21, 'by the client']]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)))
            ->on(self::SECOND_LEADER, $coordinator)
            ->install();

        $client          = $this->client();
        $coordinatorNode = $client->getGroupCoordinator('t7-group');

        self::assertSame(1, $coordinatorNode->nodeId);

        $client->commitGroupOffsets(
            $coordinatorNode,
            't7-group',
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            [self::TOPIC => [0 => new OffsetAndMetadata(21, 'by the client')]],
            OffsetCommitRequest::DEFAULT_RETENTION_TIME
        );
        $offsets = $client->fetchGroupOffsets($coordinatorNode, 't7-group', [self::TOPIC => [0]]);

        self::assertSame([self::TOPIC => [0 => 21]], $offsets);

        $frames = $coordinator->getReceivedFrames();

        self::assertSame(ApiKeys::OFFSET_COMMIT, $this->apiKeyOf($frames[0]));
        self::assertSame(2, $this->apiVersionOf($frames[0]), 'kafka offset storage speaks OffsetCommit version 2');
        self::assertSame(ApiKeys::OFFSET_FETCH, $this->apiKeyOf($frames[1]));
        self::assertSame(1, $this->apiVersionOf($frames[1]), 'OffsetFetch did not change in Kafka 0.9');
    }

    public function testZookeeperOffsetStorageSpeaksVersionZero(): void
    {
        $anyNode = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::offsetCommit(0, [self::TOPIC => [0 => 0]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $anyNode)
            ->install();

        $client      = $this->client([ClientConfig::OFFSETS_STORAGE => ClientConfig::OFFSETS_STORAGE_ZOOKEEPER]);
        $coordinator = $client->getGroupCoordinator('t7-group');

        $client->commitGroupOffsets($coordinator, 't7-group', '', -1, [self::TOPIC => [0 => 21]], -1);

        self::assertSame(0, $this->apiVersionOf($anyNode->getReceivedFrames()[1]));
    }

    public function testACommitErrorOfAPartitionIsReported(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
                ResponseFrame::offsetCommit(0, [self::TOPIC => [0 => KafkaException::OFFSET_METADATA_TOO_LARGE]])
            ))
            ->install();

        $client      = $this->client();
        $coordinator = $client->getGroupCoordinator('t7-group');

        $this->expectException(KafkaException::class);
        $client->commitGroupOffsets($coordinator, 't7-group', '', -1, [self::TOPIC => [0 => 21]], -1);
    }

    public function testANeverCommittedPartitionComesBackWithTheOffsetMinusOne(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
                ResponseFrame::offsetFetch(0, [
                    self::TOPIC => [0 => [KafkaException::UNKNOWN_TOPIC_OR_PARTITION, -1, '']],
                ])
            ))
            ->install();

        $client = $this->client();

        self::assertSame(
            [self::TOPIC => [0 => -1]],
            $client->fetchGroupOffsets($client->getGroupCoordinator('t7-group'), 't7-group', [self::TOPIC => [0]])
        );
    }

    public function testAGroupIsJoinedThroughTheCoordinatorWithTheConfiguredSessionTimeout(): void
    {
        $coordinator = new BrokerConnection(
            ResponseFrame::joinGroup(0, 0, 1, 'range', 'one-1', 'one-1', ['one-1' => 'metadata'])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)))
            ->on(self::SECOND_LEADER, $coordinator)
            ->install();

        $client   = $this->client([ConsumerConfig::SESSION_TIMEOUT_MS => 12000]);
        $response = $client->joinGroup(
            $client->getGroupCoordinator('t3-group'),
            't3-group',
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            'consumer',
            ['range' => 'metadata']
        );

        self::assertSame(1, $response->generationId);
        self::assertSame('one-1', $response->memberId);
        self::assertSame('one-1', $response->leaderId, 'the only member of a fresh group is its leader');
        self::assertSame(['one-1' => 'metadata'], array_map(
            static fn($member): string => $member->metadata,
            $response->members
        ));

        $frame = $coordinator->getReceivedFrames()[0];

        self::assertSame(ApiKeys::JOIN_GROUP, $this->apiKeyOf($frame));
        self::assertSame(0, $this->apiVersionOf($frame), 'JoinGroup v1 with its rebalance timeout is Kafka 0.10.1');
        $sent = JoinGroupRequest::unpack(new StringStream(pack('N', strlen($frame)) . $frame));

        self::assertSame(
            [
                'consumerGroup'  => 't3-group',
                'sessionTimeout' => 12000,
                'memberId'       => '',
                'protocolType'   => 'consumer',
            ],
            array_intersect_key(MessageFields::of($sent), array_flip([
                'consumerGroup',
                'sessionTimeout',
                'memberId',
                'protocolType',
            ])),
            'the session timeout of the request is session.timeout.ms of the configuration'
        );
    }

    public function testTheLeaderPublishesItsAssignmentWithSyncGroupAndEveryMemberReadsItsOwn(): void
    {
        $coordinator = new BrokerConnection(ResponseFrame::syncGroup(0, 0, 'my-share'));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)
            ))
            ->on(self::SECOND_LEADER, $coordinator)
            ->install();

        $client   = $this->client();
        $response = $client->syncGroup(
            $client->getGroupCoordinator('t3-group'),
            't3-group',
            'one-1',
            4,
            ['one-1' => 'my-share', 'two-2' => 'other-share']
        );

        self::assertSame('my-share', $response->memberAssignment);
        self::assertSame(ApiKeys::SYNC_GROUP, $this->apiKeyOf($coordinator->getReceivedFrames()[0]));
        self::assertSame(0, $this->apiVersionOf($coordinator->getReceivedFrames()[0]));
    }

    public function testAHeartbeatAndALeaveAreSentToTheCoordinatorAndReportNothingWhenTheySucceed(): void
    {
        $coordinator = new BrokerConnection(ResponseFrame::heartbeat(0, 0), ResponseFrame::leaveGroup(0, 0));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)
            ))
            ->on(self::SECOND_LEADER, $coordinator)
            ->install();

        $client          = $this->client();
        $coordinatorNode = $client->getGroupCoordinator('t3-group');

        $client->heartbeat($coordinatorNode, 't3-group', 'one-1', 4);
        $client->leaveGroup($coordinatorNode, 't3-group', 'one-1');

        $frames = $coordinator->getReceivedFrames();

        self::assertSame(ApiKeys::HEARTBEAT, $this->apiKeyOf($frames[0]));
        self::assertSame(ApiKeys::LEAVE_GROUP, $this->apiKeyOf($frames[1]));
        self::assertNotSame(
            $coordinator->getReceivedCorrelationIds()[0],
            $coordinator->getReceivedCorrelationIds()[1],
            'every request of the group protocol carries a correlation id of its own'
        );
    }

    /**
     * The error codes of the membership protocol are statements about this member and are reported to the caller,
     * which is the only one that can react to them: rejoin, reset the member id or give up
     */
    public function testAnErrorCodeOfTheMembershipProtocolIsReportedAsItsException(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)
            ))
            ->on(self::SECOND_LEADER, new BrokerConnection(
                ResponseFrame::heartbeat(0, KafkaException::REBALANCE_IN_PROGRESS),
                ResponseFrame::syncGroup(0, KafkaException::ILLEGAL_GENERATION),
            ))
            ->install();

        $client          = $this->client();
        $coordinatorNode = $client->getGroupCoordinator('t3-group');

        try {
            $client->heartbeat($coordinatorNode, 't3-group', 'one-1', 4);
            self::fail('A heartbeat that answers 27 has to be reported');
        } catch (RebalanceInProgressException $exception) {
            self::assertSame('t3-group', $exception->getContext()['groupId']);
        }

        $this->expectException(IllegalGenerationException::class);
        $client->syncGroup($coordinatorNode, 't3-group', 'one-1', 4);
    }

    /**
     * A coordinator that is not ready yet says so with the same codes the coordinator lookup retries - they are
     * about the coordinator and not about the member, so the request itself is simply sent again
     */
    public function testACoordinatorThatIsNotReadyYetIsAskedAgain(): void
    {
        $coordinator = new BrokerConnection(
            ResponseFrame::heartbeat(0, KafkaException::GROUP_LOAD_IN_PROGRESS),
            ResponseFrame::heartbeat(0, 0)
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)
            ))
            ->on(self::SECOND_LEADER, $coordinator)
            ->install();

        $client = $this->client([ClientConfig::RETRIES => 1]);
        $client->heartbeat($client->getGroupCoordinator('t3-group'), 't3-group', 'one-1', 4);

        self::assertSame(2, $coordinator->getRequestCount(), 'the heartbeat was sent again after the code 14');
    }

    public function testACoordinatorThatStaysUnavailableIsGivenUpOnceTheRetriesAreExhausted(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)
            ))
            ->on(self::SECOND_LEADER, new BrokerConnection(
                ResponseFrame::leaveGroup(0, KafkaException::NOT_COORDINATOR_FOR_GROUP),
                ResponseFrame::leaveGroup(0, KafkaException::NOT_COORDINATOR_FOR_GROUP)
            ))
            ->install();

        $client = $this->client([ClientConfig::RETRIES => 1]);

        // The caller has to look the coordinator up again, which is why this is reported and not retried forever
        $this->expectException(NotCoordinatorForGroupException::class);
        $client->leaveGroup($client->getGroupCoordinator('t3-group'), 't3-group', 'one-1');
    }

    public function testAJoinThatIsRefusedWhileTheGroupIsLoadedIsNotReportedAsAJoinFailure(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)
            ))
            ->on(self::SECOND_LEADER, new BrokerConnection(
                ResponseFrame::joinGroup(0, KafkaException::GROUP_LOAD_IN_PROGRESS, -1, '', '', '')
            ))
            ->install();

        $client = $this->client([ConsumerConfig::SESSION_TIMEOUT_MS => 12000]);

        $this->expectException(GroupLoadInProgressException::class);
        $client->joinGroup(
            $client->getGroupCoordinator('t3-group'),
            't3-group',
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            'consumer',
            ['range' => 'metadata']
        );
    }

    public function testAnUnreachableLeaderIsReportedForItsPartitionsOnly(): void
    {
        // The second leader is not scripted at all, so the connection to it is refused
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 4]]])))
            ->install();

        try {
            $this->client()->produce([
                self::TOPIC => [0 => [new Record('reaches its leader')], 1 => [new Record('does not')]],
            ]);
            self::fail('An unreachable leader is expected to be reported');
        } catch (TopicPartitionRequestException $exception) {
            self::assertSame(4, $exception->getPartialResult()[self::TOPIC][0]->baseOffset);
            self::assertInstanceOf(NetworkException::class, $exception->getExceptions()[self::TOPIC][1]);
        }
    }

    public function testARetriableFailureIsGivenUpOnceTheRetriesAreExhausted(): void
    {
        $notLeader = ResponseFrame::produce(
            0,
            [self::TOPIC => [0 => [KafkaException::NOT_LEADER_FOR_PARTITION, -1]]]
        );
        $this->brokers
            ->on(
                self::BOOTSTRAP_ADDRESS,
                new BrokerConnection($this->clusterMetadata()),
                new BrokerConnection($this->clusterMetadata()),
                new BrokerConnection($this->clusterMetadata())
            )
            ->on(self::FIRST_LEADER, new BrokerConnection($notLeader, $notLeader, $notLeader))
            ->install();

        try {
            $this->client([ClientConfig::RETRIES => 2])
                ->produce([self::TOPIC => [0 => [new Record('never accepted')]]]);
            self::fail('An error that survives every retry is expected to be reported');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(
                NotLeaderForPartitionException::class,
                $exception->getExceptions()[self::TOPIC][0]
            );
        }
    }

    /**
     * Returns the message set of the only topic-partition of a produce request frame.
     *
     * <pre>
     *   ApiKey ApiVersion CorrelationId ClientId RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
     * </pre>
     */
    private static function messageSetOf(string $frame): string
    {
        /** @var array{clientIdLength: int} $header */
        $header = unpack('napiKey/napiVersion/NcorrelationId/nclientIdLength', $frame);
        $offset = 2 + 2 + 4 + 2 + $header['clientIdLength'];

        // requiredAcks, timeout and the number of topics of the request
        $offset += 2 + 4 + 4;

        /** @var array{topicLength: int} $topic */
        $topic  = unpack('ntopicLength', $frame, $offset);
        $offset += 2 + $topic['topicLength'];

        // the number of partitions of the topic and the id of the only one
        $offset += 4 + 4;

        /** @var array{messageSetSize: int} $partition */
        $partition = unpack('NmessageSetSize', $frame, $offset);

        return substr($frame, $offset + 4, $partition['messageSetSize']);
    }

    /**
     * Reads the first message of a serialized message set, without unwrapping a compressed one
     */
    private static function firstMessageOf(string $buffer): Message
    {
        /** @var array{messageSize: int} $header */
        $header = unpack('Joffset/NmessageSize', $buffer);

        return Message::fromBuffer(substr($buffer, MessageSet::ENTRY_OVERHEAD, $header['messageSize']));
    }

    /**
     * Builds a client on a cluster that the scripted brokers answer for
     *
     * @param array<string, mixed> $overrides
     */
    private function client(array $overrides = []): Client
    {
        $configuration = $overrides + [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't7-client',
            ClientConfig::REQUEST_TIMEOUT_MS        => 500,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::METADATA_MAX_AGE_MS       => 300000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
            ClientConfig::RETRIES                   => 0,
            ClientConfig::OFFSETS_STORAGE           => ClientConfig::OFFSETS_STORAGE_KAFKA,

            ProducerConfig::ACKS       => 1,
            ProducerConfig::TIMEOUT_MS => 100,

            ConsumerConfig::FETCH_MAX_WAIT_MS         => 200,
            ConsumerConfig::FETCH_MIN_BYTES           => 1,
            ConsumerConfig::MAX_PARTITION_FETCH_BYTES => 65536,
        ];

        return new Client(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * A cluster of two brokers, the partitions 0 and 1 of the topic are led by a different one each
     */
    private function clusterMetadata(): string
    {
        return ResponseFrame::metadata(
            0,
            [[0, 'kafka-1', 9092], [1, 'kafka-2', 9093]],
            [self::TOPIC => [0 => 0, 1 => 1]]
        );
    }

    /**
     * Starts a broker that accepts connections but never answers a request
     */
    private function startSilentBroker(): string
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errorNumber, $errorString);
        if ($server === false) {
            self::markTestSkipped("Can not listen on a local TCP port: {$errorString}");
        }
        $this->silentServer = $server;

        return 'tcp://' . stream_socket_get_name($server, false);
    }

    /**
     * Returns the port the silent broker listens on
     */
    private function silentPort(): int
    {
        $address = (string) stream_socket_get_name($this->silentServer, false);

        return (int) substr($address, (int) strrpos($address, ':') + 1);
    }

    /**
     * Reads the ApiKey out of a request frame: ApiKey int16, ApiVersion int16, CorrelationId int32, ClientId string
     */
    private function apiKeyOf(string $frame): int
    {
        return (int) unpack('napiKey', substr($frame, 0, 2))['apiKey'];
    }

    /**
     * Reads the ApiVersion out of a request frame
     */
    private function apiVersionOf(string $frame): int
    {
        return (int) unpack('nversion', substr($frame, 2, 2))['version'];
    }
}
