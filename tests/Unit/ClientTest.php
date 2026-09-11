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
use Protocol\Kafka\Common\Errors\GroupAuthorizationFailedException;
use Protocol\Kafka\Common\Errors\GroupLoadInProgressException;
use Protocol\Kafka\Common\Errors\IllegalGenerationException;
use Protocol\Kafka\Common\Errors\InvalidConfigurationException;
use Protocol\Kafka\Common\Errors\InvalidTxnTimeoutException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\NotLeaderForPartitionException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\OutOfOrderSequenceException;
use Protocol\Kafka\Common\Errors\ProducerFencedException;
use Protocol\Kafka\Common\Errors\RebalanceInProgressException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownProducerIdException;
use Protocol\Kafka\Common\Errors\UnstableOffsetCommitException;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\FetchSessionHandler;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Network\RetryPolicy;
use Protocol\Kafka\Producer\Internals\TransactionManager;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Tests\Compliance\MessageFields;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;
use Protocol\Kafka\Tests\Unit\Fixture\ThrottleAwareTestClient;
use Protocol\Kafka\Tests\Unit\Fixture\TransactionalTestClient;

/**
 * Tests the low-level client against scripted brokers: the fan-out to the partition leaders, the correlation of the
 * answers, the retries after a metadata refresh and the reporting of a partially failed request.
 *
 * @see docs/protocol/2.8.md
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
        // A leader change always raises the leader epoch of the partition, and a Metadata v7 answer that does not
        // raise it is ignored as stale (KIP-320), so the moved leader arrives with the epoch 1
        $movedLeader = ResponseFrame::metadata(
            0,
            [[0, 'kafka-1', 9092], [1, 'kafka-2', 9093]],
            [self::TOPIC => [0 => 1, 1 => 1]],
            leaderEpochs: [self::TOPIC => [0 => 1, 1 => 1]]
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
        $leader = new BrokerConnection(ResponseFrame::produceV2(0, [self::TOPIC => [0 => [0, 5]]]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $records = [
            new Record(str_repeat('a repetitive value ', 32), 'key-0'),
            new Record(str_repeat('a repetitive value ', 32), 'key-1'),
            new Record(str_repeat('a repetitive value ', 32)),
        ];

        $this->client([
            ProducerConfig::COMPRESSION_TYPE       => $compressionType,
            ProducerConfig::MESSAGE_FORMAT_VERSION => ProducerConfig::MESSAGE_FORMAT_VERSION_0_10_0,
        ])->produce([self::TOPIC => [0 => $records]]);

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
        $leader = new BrokerConnection(ResponseFrame::produceV2(0, [self::TOPIC => [0 => [0, 1]]]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $this->client([ProducerConfig::MESSAGE_FORMAT_VERSION => ProducerConfig::MESSAGE_FORMAT_VERSION_0_10_0])
            ->produce([self::TOPIC => [0 => [new Record('as it is', 'a key')]]]);

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
            // zstd is the codec of Kafka 2.1 and the message format v2, not of this protocol line
            $this->client([ProducerConfig::COMPRESSION_TYPE => 'zstd'])
                ->produce([self::TOPIC => [0 => [new Record('never sent')]]]);
            self::fail('An unsupported compression type has to be rejected');
        } catch (InvalidConfigurationException $exception) {
            self::assertStringContainsString('zstd', $exception->getMessage());
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
                // v1 answers one offset per partition; a request for the latest offset carries the timestamp -1
                self::TOPIC => [0 => [0, -1, 64], 1 => [0, -1, 0]],
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
        self::assertFalse($partition->hasPartialTrailingRecord());
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

    public function testAPartitionWithoutACompleteMessageIsNotReportedAsStuckByAVersionThreeFetch(): void
    {
        // Up to version 2 an answer without a single complete message meant "the next message does not fit into
        // MaxBytes"; the version 3 request that this client sends has no such state, because the broker returns
        // the first message of the answer whatever its size is. An empty partition below the high water mark now
        // means that the `fetch.max.bytes` of the answer were used up by the partitions in front of it, so the
        // client must not turn it into a RecordTooLargeException any more
        $truncated = substr(MessageSet::fromRecords([new Record(str_repeat('x', 512))])->toBuffer(), 0, 40);
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::fetch(0, [
                self::TOPIC => [0 => [0, 5, $truncated]],
            ])))
            ->install();

        $partition = $this->client()->fetchPartitions([self::TOPIC => [0 => 0]], 200)[self::TOPIC][0];

        self::assertFalse($partition->isSingleMessageTooLarge());
        self::assertTrue($partition->isEmpty());
        self::assertSame(0, $partition->getNextOffset(), 'a partition without a record keeps its fetch offset');
    }

    public function testTheFetchRequestCarriesTheConfiguredFetchMaxBytesAndTheOrderOfTheGivenPartitions(): void
    {
        // Both partitions are led by the same broker, so that they travel in one request
        $metadata   = ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0, 1 => 0]]);
        $connection = new BrokerConnection(ResponseFrame::fetch(0, [
            self::TOPIC => [1 => [0, 1, ''], 0 => [0, 1, '']],
        ]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($metadata))
            ->on(self::FIRST_LEADER, $connection)
            ->install();

        $this->client([ConsumerConfig::FETCH_MAX_BYTES => 1048576])
            ->fetchPartitions([self::TOPIC => [1 => 7, 0 => 3]], 200);

        $request = bin2hex($connection->getReceivedFrames()[0]);

        // ApiKey 1, ApiVersion 11, then - behind MinBytes - the request-level MaxBytes of `fetch.max.bytes`, the
        // isolation level `read_uncommitted` and the session id 0 with the epoch -1 of a session-less fetch
        self::assertStringStartsWith('0001000b', $request, 'the Fetch api is spoken in version 11');
        self::assertStringContainsString(
            '00100000' . '00' . '00000000' . 'ffffffff',
            $request,
            'fetch.max.bytes, read_uncommitted and the session-less metadata of KIP-227'
        );
        // The partitions travel in the order they were given, which is the order the broker fills the answer in;
        // the -1 in front of every MaxBytes is the LogStartOffset of v5, which only a follower fills in, and the
        // trailing empty array is the `forgotten_topics_data` of version 7
        // The -1 in front of every fetch offset is the `current_leader_epoch` of Fetch v9 (KIP-320): this client
        // sends "I do not know the epoch" unless the caller passed one
        self::assertStringEndsWith(
            '00000001' . 'ffffffff' . '0000000000000007' . 'ffffffffffffffff' . '00010000'
            . '00000000' . 'ffffffff' . '0000000000000003' . 'ffffffffffffffff' . '00010000'
            . '00000000'
            . '0000',
            $request
        );
    }

    public function testAReadCommittedFetchReportsTheLastStableOffsetAndTheAbortedTransactions(): void
    {
        $recordSet = MemoryRecords::fromRecordBatch(RecordBatch::fromRecords([new Record('committed')]))->toBuffer();
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::fetch(
                0,
                [self::TOPIC => [0 => [0, 40, $recordSet]]],
                0,
                [self::TOPIC => [0 => [37, 4, [[1000, 12]]]]]
            )))
            ->install();

        $client    = $this->client(['isolation.level' => 'read_committed']);
        $partition = $client->fetchPartitions([self::TOPIC => [0 => 12]], 200)[self::TOPIC][0];

        self::assertSame(37, $partition->lastStableOffset, 'a read_committed fetch stops at the LSO');
        self::assertSame(4, $partition->logStartOffset);
        self::assertCount(1, (array) $partition->abortedTransactions);
        self::assertSame(1000, $partition->abortedTransactions[0]->producerId);
        self::assertSame(12, $partition->abortedTransactions[0]->firstOffset);
        self::assertSame(['committed'], array_column($partition->getRecords(), 'value'));
    }

    /**
     * @param string|null $isolationLevel The `isolation.level` of the configuration, null for a client without one
     * @param string      $expectedByte   Hexadecimal of the `IsolationLevel` byte the request has to carry
     */
    #[DataProvider('isolationLevels')]
    public function testTheIsolationLevelOfTheConfigurationReachesTheFetchRequest(
        ?string $isolationLevel,
        string $expectedByte
    ): void {
        $connection = new BrokerConnection(ResponseFrame::fetch(0, [self::TOPIC => [0 => [0, 0, '']]]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $connection)
            ->install();

        $overrides = $isolationLevel === null ? [] : ['isolation.level' => $isolationLevel];
        $this->client($overrides)->fetchPartitions([self::TOPIC => [0 => 0]], 200);

        // The isolation level is the single byte behind the request-level MaxBytes, here the 50 MiB default
        self::assertStringContainsString(
            '03200000' . $expectedByte,
            bin2hex($connection->getReceivedFrames()[0])
        );
    }

    /**
     * @return iterable<string, array{0: string|null, 1: string}>
     */
    public static function isolationLevels(): iterable
    {
        yield 'not configured'   => [null, '00'];
        yield 'read_uncommitted' => ['read_uncommitted', '00'];
        yield 'read_committed'   => ['read_committed', '01'];
    }

    public function testTheProduceRequestOfTheDefaultMessageFormatIsAVersionEightRecordBatch(): void
    {
        $leader = new BrokerConnection(ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 5]]]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $record = new Record('with a header', 'a key')->withHeaders(new Header('trace-id', 'abc'));
        $this->client()->produce([self::TOPIC => [0 => [$record]]]);

        $frame = bin2hex($leader->getReceivedFrames()[0]);
        // ApiKey 0, ApiVersion 8, correlation id, client id, then the null transactional id of a plain producer
        self::assertStringStartsWith('00000008', $frame, 'the Produce api is spoken in version 8');
        self::assertStringContainsString('74372d636c69656e74' . 'ffff', $frame, 'no transactional id is sent');

        $records = MemoryRecords::fromBuffer(self::messageSetOf($leader->getReceivedFrames()[0]));
        self::assertSame(RecordBatch::MAGIC, $records->getMagic());
        self::assertSame('with a header', $records->getRecords()[0]->value);
        self::assertSame('trace-id', $records->getRecords()[0]->headers[0]->key);
        self::assertSame('abc', $records->getRecords()[0]->headers[0]->value);
    }

    public function testAMessageFormatBelowTheRecordBatchIsSentAsAProduceVersionTwo(): void
    {
        $leader = new BrokerConnection(ResponseFrame::produceV2(0, [self::TOPIC => [0 => [0, 5]]]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $record = new Record('dropped headers', 'a key')->withHeaders(new Header('trace-id', 'abc'));
        $this->client([ProducerConfig::MESSAGE_FORMAT_VERSION => ProducerConfig::MESSAGE_FORMAT_VERSION_0_10_0])
            ->produce([self::TOPIC => [0 => [$record]]]);

        $frame = bin2hex($leader->getReceivedFrames()[0]);
        self::assertStringStartsWith('00000002', $frame, 'a message set can only be sent below version 3');

        $records = MemoryRecords::fromBuffer(self::messageSetOf($leader->getReceivedFrames()[0]));
        self::assertSame(Message::MAGIC_V1, $records->getMagic());
        self::assertSame([], $records->getRecords()[0]->headers, 'a message set has no place for headers');
    }

    public function testTheProducerStateOfABatchTravelsInItsRecordBatch(): void
    {
        $leader = new BrokerConnection(ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 5]]]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        // This is the plug of T7 and T8: the producer id, the epoch, the sequence of every topic-partition and the
        // transactional id are the only things the bookkeeping of KIP-98 has to add to a produce call
        $this->transactionalClient()->produceRecordsWith(
            [self::TOPIC => [0 => [new Record('in a transaction')]]],
            1000,
            3,
            [self::TOPIC => [0 => 42]],
            'tx-1'
        );

        $frame = bin2hex($leader->getReceivedFrames()[0]);
        self::assertStringContainsString('0004' . '74782d31', $frame, 'the transactional id reached the frame');

        $batch = MemoryRecords::fromBuffer(self::messageSetOf($leader->getReceivedFrames()[0]))->getBatches()[0];
        self::assertInstanceOf(RecordBatch::class, $batch);
        self::assertSame(1000, $batch->producerId);
        self::assertSame(3, $batch->producerEpoch);
        self::assertSame(42, $batch->baseSequence);
        self::assertTrue($batch->isTransactional());
    }

    public function testATransactionalIdNeedsTheMessageFormatOfTheRecordBatch(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection())
            ->install();

        $client = $this->transactionalClient(
            [ProducerConfig::MESSAGE_FORMAT_VERSION => ProducerConfig::MESSAGE_FORMAT_VERSION_0_10_0]
        );

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('A transactional producer needs the message format 0.11.0');

        $client->produceRecordsWith([self::TOPIC => [0 => [new Record('never sent')]]], 1000, 3, [], 'tx-1');
    }

    public function testAProducerIdWithoutATransactionalIdIsAskedOfAnyBroker(): void
    {
        $anyBroker = new BrokerConnection(ResponseFrame::initProducerId(0, 0, 2000, 0));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $anyBroker)
            ->install();

        $producerIdAndEpoch = $this->client()->initProducerId();

        self::assertSame(2000, $producerIdAndEpoch->producerId);
        self::assertSame(0, $producerIdAndEpoch->epoch);
        self::assertTrue($producerIdAndEpoch->isValid());

        $frame = $anyBroker->getReceivedFrames()[0];

        self::assertSame(ApiKeys::INIT_PRODUCER_ID, $this->apiKeyOf($frame));
        self::assertSame(2, $this->apiVersionOf($frame), 'Kafka 2.4 raised the api to the flexible version 2');
        // The compact null of the transactional id, the default transaction timeout of one minute and the tag
        // buffer that closes the body of every flexible frame
        self::assertStringEndsWith('00' . '0000ea60' . '00', bin2hex($frame));
    }

    public function testAProducerIdOfATransactionalIdIsAskedOfItsTransactionCoordinator(): void
    {
        $lookupNode  = new BrokerConnection(ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093));
        $coordinator = new BrokerConnection(ResponseFrame::initProducerId(0, 0, 4711, 2));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $lookupNode)
            ->on(self::SECOND_LEADER, $coordinator)
            ->install();

        $producerIdAndEpoch = $this->client()->initProducerId('tx-1', 30000);

        self::assertSame(4711, $producerIdAndEpoch->producerId);
        self::assertSame(2, $producerIdAndEpoch->epoch);

        $lookupFrame = $lookupNode->getReceivedFrames()[0];

        self::assertSame(ApiKeys::GROUP_COORDINATOR, $this->apiKeyOf($lookupFrame));
        self::assertSame(3, $this->apiVersionOf($lookupFrame), 'the flexible FindCoordinator of KIP-482');
        // The key "tx-1" and the CoordinatorType 1 of a transactional id
        self::assertStringEndsWith(
            '05' . '74782d31' . '01' . '00',
            bin2hex($lookupFrame),
            'the compact key is the transactional id, the type 1, and the body ends in its tag buffer'
        );

        $initFrame = $coordinator->getReceivedFrames()[0];

        self::assertSame(ApiKeys::INIT_PRODUCER_ID, $this->apiKeyOf($initFrame));
        // The compact "tx-1" of the flexible v2, the timeout and the tag buffer of the body
        self::assertStringEndsWith('05' . '74782d31' . '00007530' . '00', bin2hex($initFrame));
    }

    public function testAnErrorOfTheProducerIdRequestIsReportedAsItsException(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
                ResponseFrame::initProducerId(0, KafkaException::INVALID_TRANSACTION_TIMEOUT)
            ))
            ->install();

        $this->expectException(InvalidTxnTimeoutException::class);

        $this->client()->initProducerId('tx-1', 999999999);
    }

    public function testAnIdempotentProduceStampsTheBatchesAndMovesTheSequencesOn(): void
    {
        $leader = new BrokerConnection(
            ResponseFrame::initProducerId(0, 0, 2000, 0),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 17]]]),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 19]]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $client  = $this->idempotentClient();
        $manager = new TransactionManager($client);

        $client->produce([self::TOPIC => [0 => [new Record('one'), new Record('two')]]], $manager);
        $client->produce([self::TOPIC => [0 => [new Record('three')]]], $manager);

        self::assertSame(2000, $manager->getProducerIdAndEpoch()->producerId);
        self::assertSame(3, $manager->sequenceNumber(new TopicPartition(self::TOPIC, 0)));

        $frames = $leader->getReceivedFrames();

        self::assertSame(ApiKeys::INIT_PRODUCER_ID, $this->apiKeyOf($frames[0]), 'The producer id comes first');

        $first = MemoryRecords::fromBuffer(self::messageSetOf($frames[1]))->getBatches()[0];
        self::assertInstanceOf(RecordBatch::class, $first);
        self::assertSame(2000, $first->producerId);
        self::assertSame(0, $first->producerEpoch);
        self::assertSame(0, $first->baseSequence);
        self::assertSame(1, $first->getLastSequence());
        self::assertFalse($first->isTransactional(), 'An idempotent batch is not a transactional one');

        $second = MemoryRecords::fromBuffer(self::messageSetOf($frames[2]))->getBatches()[0];
        self::assertInstanceOf(RecordBatch::class, $second);
        self::assertSame(2, $second->baseSequence, 'The second batch continues where the first one ended');
    }

    public function testProducerStateNextToAnAcksBelowAllIsRefused(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection())
            ->install();

        // The default of this fixture is acks = 1, which is exactly what the guarantee can not be built on: a
        // request that is not answered by the ISR is one whose sequence numbers this client can not move on
        $client = $this->client();

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The delivery guarantee of KIP-98 needs acks = all');

        $client->produce([self::TOPIC => [0 => [new Record('never sent')]]], new TransactionManager($client));
    }

    public function testTheRetryOfABatchIsTheVerySameFrameAgain(): void
    {
        // A leader change always raises the leader epoch of the partition, and a Metadata v7 answer that does not
        // raise it is ignored as stale (KIP-320), so the moved leader arrives with the epoch 1
        $movedLeader = ResponseFrame::metadata(
            0,
            [[0, 'kafka-1', 9092], [1, 'kafka-2', 9093]],
            [self::TOPIC => [0 => 1, 1 => 1]],
            leaderEpochs: [self::TOPIC => [0 => 1, 1 => 1]]
        );
        $staleLeader = new BrokerConnection(
            ResponseFrame::initProducerId(0, 0, 2000, 0),
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

        $client  = $this->idempotentClient([ClientConfig::RETRIES => 1]);
        $manager = new TransactionManager($client);
        $record  = new Record('exactly once')->withCreateTime(1600000000000);

        $result = $client->produce([self::TOPIC => [0 => [$record]]], $manager);

        self::assertSame(8, $result[self::TOPIC][0]->baseOffset);
        // The record set is built once and sent to whoever leads the partition, so the retry carries the very same
        // producer id, epoch and sequence - which is the only reason the broker can recognise it as a duplicate
        self::assertSame(
            bin2hex(self::messageSetOf($staleLeader->getReceivedFrames()[1])),
            bin2hex(self::messageSetOf($newLeader->getReceivedFrames()[0]))
        );
        self::assertSame(1, $manager->sequenceNumber(new TopicPartition(self::TOPIC, 0)));
    }

    public function testADuplicateSequenceIsReportedAsAnAcceptedPartition(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::initProducerId(0, 0, 2000, 0),
                ResponseFrame::produce(0, [self::TOPIC => [0 => [KafkaException::DUPLICATE_SEQUENCE_NUMBER, -1]]])
            ))
            ->install();

        $client  = $this->idempotentClient();
        $manager = new TransactionManager($client);

        $result = $client->produce([self::TOPIC => [0 => [new Record('already there')]]], $manager);

        self::assertSame(KafkaException::NO_ERROR, $result[self::TOPIC][0]->errorCode);
        self::assertSame(-1, $result[self::TOPIC][0]->baseOffset, 'The offset of the original append is not in it');
        self::assertSame(1, $manager->sequenceNumber(new TopicPartition(self::TOPIC, 0)));
        self::assertFalse($manager->hasFatalError());
    }

    public function testAnOutOfOrderSequenceThrowsTheProducerIdAwayAndIsReported(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::initProducerId(0, 0, 2000, 0),
                ResponseFrame::produce(0, [self::TOPIC => [0 => [KafkaException::OUT_OF_ORDER_SEQUENCE_NUMBER, -1]]])
            ))
            ->install();

        $client  = $this->idempotentClient();
        $manager = new TransactionManager($client);

        try {
            $client->produce([self::TOPIC => [0 => [new Record('a gap')]]], $manager);
            self::fail('An out of order sequence is expected to be reported to the caller');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(
                OutOfOrderSequenceException::class,
                $exception->getExceptions()[self::TOPIC][0]
            );
        }

        self::assertFalse($manager->hasProducerId(), 'The idempotent producer starts over with a new producer id');
        self::assertFalse($manager->hasFatalError());
    }

    public function testAnUnknownProducerIdAboveTheAcknowledgedOffsetNumbersThePartitionFromZeroAndSendsItAgain(): void
    {
        $leader = new BrokerConnection(
            ResponseFrame::initProducerId(0, 0, 2000, 0),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 0]]], 0, -1, [self::TOPIC => [0 => 0]]),
            // Every record of this producer was deleted in the meantime: the log now starts at 5 and the broker
            // has no state of the producer id left, which Kafka 1.0 answers with 59 instead of 45
            ResponseFrame::produce(
                0,
                [self::TOPIC => [0 => [KafkaException::UNKNOWN_PRODUCER_ID, -1]]],
                0,
                -1,
                [self::TOPIC => [0 => 5]]
            ),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 5]]], 0, -1, [self::TOPIC => [0 => 5]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $client  = $this->idempotentClient();
        $manager = new TransactionManager($client);
        $client->produce([self::TOPIC => [0 => [new Record('the first one')]]], $manager);

        $result = $client->produce([self::TOPIC => [0 => [new Record('after the deletion')]]], $manager);

        self::assertSame(5, $result[self::TOPIC][0]->baseOffset, 'the batch that was sent again was appended');
        self::assertSame(2000, $manager->getProducerIdAndEpoch()->producerId, 'and under the very same producer id');
        self::assertSame(1, $manager->sequenceNumber(new TopicPartition(self::TOPIC, 0)));

        $frames  = $leader->getReceivedFrames();
        $refused = MemoryRecords::fromBuffer(self::messageSetOf($frames[2]))->getBatches()[0];
        $again   = MemoryRecords::fromBuffer(self::messageSetOf($frames[3]))->getBatches()[0];

        self::assertInstanceOf(RecordBatch::class, $refused);
        self::assertInstanceOf(RecordBatch::class, $again);
        self::assertSame(1, $refused->baseSequence, 'the batch the broker refused continued the numbering');
        self::assertSame(0, $again->baseSequence, 'and the one that was sent again starts the partition over');
        self::assertSame(2000, $again->producerId);
    }

    public function testAnUnknownProducerIdWithoutALogStartOffsetIsSentAgainUnchanged(): void
    {
        $leader = new BrokerConnection(
            ResponseFrame::initProducerId(0, 0, 2000, 0),
            // "The partition may have moved away from the broker between the error and the answer", Sender.java
            // @ 1.1.1: nothing is decided, and the very same batch goes out once more
            ResponseFrame::produce(
                0,
                [self::TOPIC => [0 => [KafkaException::UNKNOWN_PRODUCER_ID, -1]]],
                0,
                -1,
                [self::TOPIC => [0 => -1]]
            ),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 3]]], 0, -1, [self::TOPIC => [0 => 0]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $client  = $this->idempotentClient();
        $manager = new TransactionManager($client);

        $result = $client->produce([self::TOPIC => [0 => [new Record('unchanged')]]], $manager);

        self::assertSame(3, $result[self::TOPIC][0]->baseOffset);

        $frames = $leader->getReceivedFrames();

        self::assertSame(
            bin2hex(self::messageSetOf($frames[1])),
            bin2hex(self::messageSetOf($frames[2])),
            'the second attempt is byte for byte the first one'
        );
    }

    public function testAnUnknownProducerIdWhoseRecordsAreStillInTheLogThrowsTheProducerIdAway(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::initProducerId(0, 0, 2000, 0),
                ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 0]]], 0, -1, [self::TOPIC => [0 => 0]]),
                ResponseFrame::produce(
                    0,
                    [self::TOPIC => [0 => [KafkaException::UNKNOWN_PRODUCER_ID, -1]]],
                    0,
                    -1,
                    [self::TOPIC => [0 => 0]]
                )
            ))
            ->install();

        $client  = $this->idempotentClient();
        $manager = new TransactionManager($client);
        $client->produce([self::TOPIC => [0 => [new Record('still in the log')]]], $manager);

        try {
            $client->produce([self::TOPIC => [0 => [new Record('and yet unknown')]]], $manager);
            self::fail('an unknown producer id that a retry can not fix has to be reported to the caller');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(
                UnknownProducerIdException::class,
                $exception->getExceptions()[self::TOPIC][0]
            );
            self::assertSame(0, $exception->getExceptions()[self::TOPIC][0]->getContext()['logStartOffset']);
        }

        self::assertFalse($manager->hasProducerId(), 'it is the out of order sequence it is a special case of');
        self::assertFalse($manager->hasFatalError());
    }

    public function testAFencedProducerRefusesToSendAnythingElse(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::initProducerId(0, 0, 2000, 0),
                ResponseFrame::produce(0, [self::TOPIC => [0 => [KafkaException::INVALID_PRODUCER_EPOCH, -1]]])
            ))
            ->install();

        $client  = $this->idempotentClient();
        $manager = new TransactionManager($client);

        try {
            $client->produce([self::TOPIC => [0 => [new Record('fenced')]]], $manager);
            self::fail('A fenced producer is expected to report the error of the partition');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(ProducerFencedException::class, $exception->getExceptions()[self::TOPIC][0]);
        }

        self::assertTrue($manager->hasFatalError());

        $this->expectException(ProducerFencedException::class);

        $client->produce([self::TOPIC => [0 => [new Record('never sent')]]], $manager);
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

    public function testTheFirstFetchWithSessionsOpensASessionAndTheNextOneIsIncremental(): void
    {
        $messageSet = MessageSet::fromRecords([new Record('first')])->toBuffer();
        $metadata   = ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0, 1 => 0]]);
        $connection = new BrokerConnection(
            // The full fetch is answered with both partitions and with the id of the session the broker opened
            ResponseFrame::fetch(0, [self::TOPIC => [0 => [0, 1, $messageSet], 1 => [0, 0, '']]], 0, [], 0, 4711),
            // ... the incremental one only with the partition that has news
            ResponseFrame::fetch(0, [self::TOPIC => [0 => [0, 2, $messageSet]]], 0, [], 0, 4711)
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($metadata))
            ->on(self::FIRST_LEADER, $connection)
            ->install();

        $client = $this->client();
        $client->fetchPartitionsWithSessions([self::TOPIC => [0 => 0, 1 => 0]], 200);
        $second = $client->fetchPartitionsWithSessions([self::TOPIC => [0 => 1, 1 => 0]], 200);

        [$full, $incremental] = array_map(bin2hex(...), $connection->getReceivedFrames());

        // The first request carries the session id 0 with the epoch 0 - "open a session" - and both partitions
        self::assertStringContainsString('00000000' . '00000000' . '00000001' . '00066f7264657273', $full);
        // The second one carries the session id of the answer, the epoch 1, the partition whose offset moved and
        // nothing else; the trailing empty array is the forgotten_topics_data
        self::assertStringContainsString('00001267' . '00000001', $incremental, 'the session id and the epoch 1');
        self::assertStringEndsWith(
            '00000001' . '00066f7264657273' . '00000001'
            . '00000000' . 'ffffffff' . '0000000000000001' . 'ffffffffffffffff' . '00010000'
            . '00000000'
            . '0000',
            $incremental,
            'only the partition whose fetch offset moved travels, and nothing is forgotten'
        );
        self::assertSame(
            [self::TOPIC => [0]],
            array_map(array_keys(...), $second),
            'an incremental answer carries only the partitions that have news'
        );
        self::assertSame([0 => 4711], array_map(
            static fn(FetchSessionHandler $handler): int => $handler->getSessionId(),
            $client->getFetchSessionHandlers()
        ));
    }

    public function testAPartitionThatIsNotFetchedAnyMoreIsForgottenByTheSession(): void
    {
        $metadata   = ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0, 1 => 0]]);
        $connection = new BrokerConnection(
            ResponseFrame::fetch(0, [self::TOPIC => [0 => [0, 0, ''], 1 => [0, 0, '']]], 0, [], 0, 4711),
            ResponseFrame::fetch(0, [], 0, [], 0, 4711)
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($metadata))
            ->on(self::FIRST_LEADER, $connection)
            ->install();

        $client = $this->client();
        $client->fetchPartitionsWithSessions([self::TOPIC => [0 => 0, 1 => 0]], 200);
        // The partition 1 left the assignment, so the next request tells the session to drop it
        $answer = $client->fetchPartitionsWithSessions([self::TOPIC => [0 => 0]], 200);

        $incremental = bin2hex($connection->getReceivedFrames()[1]);

        self::assertStringEndsWith(
            '00000000'                                     // topicPartitions: nothing moved
            . '00000001' . '00066f7264657273' . '00000001' // forgottenTopics: one topic ...
            . '00000001'                                   // ... with the partition 1
            . '0000',                                      // rackId: the empty rack of KIP-392
            $incremental
        );
        self::assertSame([], $answer, 'an answer with no topic at all is a legal answer of a session');
    }

    public function testASessionErrorIsAnsweredWithAFullFetchWithoutTheCallerNoticing(): void
    {
        $messageSet = MessageSet::fromRecords([new Record('after the session was lost')])->toBuffer();
        $metadata   = ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0]]);
        $connection = new BrokerConnection(
            ResponseFrame::fetch(0, [self::TOPIC => [0 => [0, 0, '']]], 0, [], 0, 4711),
            // The broker evicted the session: the error code 70, the session id 0 and no topic at all
            ResponseFrame::fetch(0, [], 0, [], KafkaException::FETCH_SESSION_ID_NOT_FOUND, 0),
            ResponseFrame::fetch(0, [self::TOPIC => [0 => [0, 1, $messageSet]]], 0, [], 0, 815)
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($metadata))
            ->on(self::FIRST_LEADER, $connection)
            ->install();

        $client = $this->client();
        $client->fetchPartitionsWithSessions([self::TOPIC => [0 => 0]], 200);
        $answer = $client->fetchPartitionsWithSessions([self::TOPIC => [0 => 0]], 200);

        self::assertCount(3, $connection->getReceivedFrames(), 'the 70 is answered with a full fetch right away');
        self::assertSame(
            ['after the session was lost'],
            array_column($answer[self::TOPIC][0]->getRecords(), 'value'),
            'the caller of the fetch sees the records of the new session, not the session error'
        );
        self::assertSame(815, $client->getFetchSessionHandlers()[0]->getSessionId());

        // The recovery is a full fetch with the epoch 0 and the session id 0, because a 70 says that the id is gone
        $recovery = bin2hex($connection->getReceivedFrames()[2]);

        self::assertStringContainsString('00000000' . '00000000' . '00000001' . '00066f7264657273', $recovery);
    }

    public function testEveryBrokerOfTheClusterGetsAFetchSessionOfItsOwn(): void
    {
        $first  = new BrokerConnection(ResponseFrame::fetch(0, [self::TOPIC => [0 => [0, 0, '']]], 0, [], 0, 11));
        $second = new BrokerConnection(ResponseFrame::fetch(0, [self::TOPIC => [1 => [0, 0, '']]], 0, [], 0, 22));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $first)
            ->on(self::SECOND_LEADER, $second)
            ->install();

        $client = $this->client();
        $client->fetchPartitionsWithSessions([self::TOPIC => [0 => 0, 1 => 0]], 200);

        self::assertSame([0 => 11, 1 => 22], array_map(
            static fn(FetchSessionHandler $handler): int => $handler->getSessionId(),
            $client->getFetchSessionHandlers()
        ), 'a fetch session belongs to one broker, and every leader answers with an id of its own');
    }

    public function testAFetchThatIsNotAnsweredAtAllStartsTheSessionOver(): void
    {
        $metadata = ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0]]);
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($metadata))
            ->on(
                self::FIRST_LEADER,
                new BrokerConnection(ResponseFrame::fetch(0, [self::TOPIC => [0 => [0, 0, '']]], 0, [], 0, 4711)),
                // The connection of the second fetch is opened and answers nothing at all
                new BrokerConnection(null)
            )
            ->install();

        $client = $this->client();
        $client->fetchPartitionsWithSessions([self::TOPIC => [0 => 0]], 200);

        try {
            $client->fetchPartitionsWithSessions([self::TOPIC => [0 => 0]], 200);
            self::fail('A broker that does not answer is expected to fail the fetch of its partitions');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(NetworkException::class, $exception->getExceptions()[self::TOPIC][0]);
        }

        // The session may or may not have seen that request, so the next one closes it and opens a new one
        $metadata = $client->getFetchSessionHandlers()[0]->getNextMetadata();

        self::assertSame(4711, $metadata->sessionId);
        self::assertSame(FetchMetadata::INITIAL_EPOCH, $metadata->epoch);
    }

    public function testACommitIsRoutedToTheCoordinatorAsVersionSix(): void
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
        self::assertSame(8, $this->apiVersionOf($frames[0]), 'kafka offset storage speaks OffsetCommit version 8');
        self::assertSame(ApiKeys::OFFSET_FETCH, $this->apiKeyOf($frames[1]));
        self::assertSame(7, $this->apiVersionOf($frames[1]), 'kafka offset storage speaks OffsetFetch version 7');
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

    public function testEveryCommittedOffsetOfAGroupIsFetchedWithTheNullTopicArray(): void
    {
        $coordinator = new BrokerConnection(
            ResponseFrame::offsetFetch(0, [self::TOPIC => [0 => [0, 21, '']]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)))
            ->on(self::SECOND_LEADER, $coordinator)
            ->install();

        $client = $this->client();
        $node   = $client->getGroupCoordinator('t7-group');

        self::assertSame([self::TOPIC => [0 => 21]], $client->fetchGroupOffsets($node, 't7-group', null));

        $frame = $coordinator->getReceivedFrames()[0];
        self::assertSame(7, $this->apiVersionOf($frame), 'the nullable topic array needs OffsetFetch v2 or above');
        self::assertStringEndsWith(
            '0000',
            bin2hex($frame),
            'the null topic array of a flexible version is the unsigned varint 0, then the tag buffer'
        );
    }

    /**
     * KIP-447 (Kafka 2.5): the `require_stable` flag of version 7 is the last byte before the tag buffer of the body
     */
    public function testStableOffsetsAreAskedForWithTheFlagOfVersionSeven(): void
    {
        $coordinator = new BrokerConnection(
            ResponseFrame::offsetFetch(0, [self::TOPIC => [0 => [0, 21, '']]]),
            ResponseFrame::offsetFetch(1, [self::TOPIC => [0 => [0, 21, '']]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)))
            ->on(self::SECOND_LEADER, $coordinator)
            ->install();

        $client = $this->client();
        $node   = $client->getGroupCoordinator('t7-group');

        $client->fetchGroupOffsets($node, 't7-group', [self::TOPIC => [0]]);
        $client->fetchGroupOffsets($node, 't7-group', [self::TOPIC => [0]], true);

        [$plain, $stable] = $coordinator->getReceivedFrames();

        self::assertSame(7, $this->apiVersionOf($plain));
        self::assertStringEndsWith('0000', bin2hex($plain), 'false, then the tag buffer of the body');
        self::assertStringEndsWith('0100', bin2hex($stable), 'true, then the tag buffer of the body');
        self::assertSame(
            strlen($plain),
            strlen($stable),
            'the flag is a byte of the frame in both cases, not an optional field'
        );
    }

    /**
     * And the 88 a coordinator answers a held-back partition with is reported as its exception
     */
    public function testAnUnstableCommittedOffsetIsReportedAsTheEightyEight(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)))
            ->on(self::SECOND_LEADER, new BrokerConnection(ResponseFrame::offsetFetch(
                0,
                [self::TOPIC => [0 => [KafkaException::UNSTABLE_OFFSET_COMMIT, -1, '']]]
            )))
            ->install();

        $client = $this->client();
        $node   = $client->getGroupCoordinator('t7-group');

        $this->expectException(UnstableOffsetCommitException::class);
        $client->fetchGroupOffsets($node, 't7-group', [self::TOPIC => [0]], true);
    }

    public function testAGroupLevelErrorOfOffsetFetchVersionTwoIsReported(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
                ResponseFrame::offsetFetch(0, [], KafkaException::GROUP_AUTHORIZATION_FAILED)
            ))
            ->install();

        $client = $this->client();

        $this->expectException(GroupAuthorizationFailedException::class);
        $client->fetchGroupOffsets($client->getGroupCoordinator('t7-group'), 't7-group', null);
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
        self::assertSame(7, $this->apiVersionOf($frame), 'JoinGroup v7 is the version of KIP-559');
        $sent = JoinGroupRequest::unpack(new StringStream(pack('N', strlen($frame)) . $frame));

        self::assertSame(
            [
                'consumerGroup'    => 't3-group',
                'sessionTimeout'   => 12000,
                'rebalanceTimeout' => ConsumerConfig::DEFAULT_MAX_POLL_INTERVAL_MS,
                'memberId'         => '',
                'protocolType'     => 'consumer',
            ],
            array_intersect_key(MessageFields::of($sent), array_flip([
                'consumerGroup',
                'sessionTimeout',
                'rebalanceTimeout',
                'memberId',
                'protocolType',
            ])),
            'the two timeouts come from session.timeout.ms and max.poll.interval.ms of the configuration'
        );
    }

    public function testTheRebalanceTimeoutOfAJoinFollowsTheConfigurationAndTheExplicitArgument(): void
    {
        $coordinator = new BrokerConnection(
            ResponseFrame::joinGroup(0, 0, 1, 'range', 'one-1', 'one-1', ['one-1' => 'metadata']),
            ResponseFrame::joinGroup(0, 0, 1, 'range', 'one-1', 'one-1', ['one-1' => 'metadata'])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(ResponseFrame::groupCoordinator(0, 0, 1, 'kafka-2', 9093)))
            ->on(self::SECOND_LEADER, $coordinator)
            ->install();

        $client = $this->client([
            ConsumerConfig::SESSION_TIMEOUT_MS   => 12000,
            ConsumerConfig::MAX_POLL_INTERVAL_MS => 45000,
        ]);
        $node   = $client->getGroupCoordinator('t3-group');

        $client->joinGroup($node, 't3-group', '', 'consumer', ['range' => 'metadata']);
        $client->joinGroup($node, 't3-group', '', 'consumer', ['range' => 'metadata'], 7000);

        $timeouts = [];
        foreach ($coordinator->getReceivedFrames() as $frame) {
            $sent       = JoinGroupRequest::unpack(new StringStream(pack('N', strlen($frame)) . $frame));
            $timeouts[] = MessageFields::of($sent)['rebalanceTimeout'];
        }

        self::assertSame(
            [45000, 7000],
            $timeouts,
            'null takes max.poll.interval.ms of the configuration, an explicit value wins over it'
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
            ['one-1' => 'my-share', 'two-2' => 'other-share'],
            null,
            'consumer',
            'range'
        );

        self::assertSame('my-share', $response->memberAssignment);
        self::assertSame(ApiKeys::SYNC_GROUP, $this->apiKeyOf($coordinator->getReceivedFrames()[0]));
        self::assertSame(5, $this->apiVersionOf($coordinator->getReceivedFrames()[0]));
    }

    /**
     * KIP-559 made the two protocol fields of a SyncGroup v5 mandatory, so a caller that names neither gets the v4
     */
    public function testASyncThatNamesNoProtocolIsSentAsTheVersionFourFrame(): void
    {
        $coordinator = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::syncGroupV4(0, 0, 'my-share')
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $coordinator)
            ->install();

        $client = $this->client();
        $client->syncGroup($client->getGroupCoordinator('t3-group'), 't3-group', 'one-1', 4);

        $frame = $coordinator->getReceivedFrames()[1];

        self::assertSame(ApiKeys::SYNC_GROUP, $this->apiKeyOf($frame));
        self::assertSame(
            4,
            $this->apiVersionOf($frame),
            'a version 5 without the protocol type and name would be refused with 23 before the group is read'
        );
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
                ResponseFrame::syncGroupV4(0, KafkaException::ILLEGAL_GENERATION),
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
     * Returns the record set of the only topic-partition of a produce request frame.
     *
     * <pre>
     *   ApiKey ApiVersion CorrelationId ClientId [TransactionalId] RequiredAcks Timeout
     *   [TopicName [Partition RecordSetSize RecordSet]]
     * </pre>
     */
    private static function messageSetOf(string $frame): string
    {
        /** @var array{apiVersion: int, clientIdLength: int} $header */
        $header = unpack('napiKey/napiVersion/NcorrelationId/nclientIdLength', $frame);
        $offset = 2 + 2 + 4 + 2 + $header['clientIdLength'];

        // The nullable TransactionalId that version 3 put in front of RequiredAcks: -1 is null and has no bytes
        if ($header['apiVersion'] >= 3) {
            /** @var array{transactionalIdLength: int} $transactionalId */
            $transactionalId = unpack('ntransactionalIdLength', $frame, $offset);
            $length          = $transactionalId['transactionalIdLength'];
            $offset          += 2 + ($length === 0xFFFF ? 0 : $length);
        }

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

    public function testAThrottledAnswerDelaysTheNextRequestToTheSameBroker(): void
    {
        // The first answer reports a throttle of 400 ms, the second one none: KIP-219 means that the broker has
        // muted the channel for those 400 ms, so the second request must not be written before they are over
        $leader = new BrokerConnection(
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 17]]], 400),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 18]]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $client = $this->throttleAwareClient();
        $first  = $client->produce([self::TOPIC => [0 => [new Record('first')]]]);
        self::assertSame([], $client->getSleeps(), 'the throttled answer itself is never waited for');
        self::assertSame(400, $first[self::TOPIC][0]->throttleTimeMs, 'the field is reported to the caller');

        $client->produce([self::TOPIC => [0 => [new Record('second')]]]);

        self::assertSame([400000], $client->getSleeps(), 'the next request waits the reported 400 ms out');
        self::assertSame(2, $leader->getRequestCount());
    }

    public function testTheClientWaitsTheThrottleDeadlineOutAndNotTheValueTwice(): void
    {
        // A caller that spent 300 of the 400 ms elsewhere only owes the remaining 100, and a caller that spent
        // more than the whole throttle owes nothing at all: what is remembered is the deadline, not the value
        $leader = new BrokerConnection(
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 17]]], 400),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 18]]], 400),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 19]]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $client = $this->throttleAwareClient();
        $client->produce([self::TOPIC => [0 => [new Record('first')]]]);
        $client->advanceBy(0.3);
        $client->produce([self::TOPIC => [0 => [new Record('second')]]]);
        $client->advanceBy(1.0);
        $client->produce([self::TOPIC => [0 => [new Record('third')]]]);

        self::assertSame(
            [100000],
            $client->getSleeps(),
            'the second request owes 100 ms of the first throttle, the third one nothing of the second'
        );
    }

    public function testASecondRequestAfterAThrottleThatWasWaitedOutDoesNotWaitAgain(): void
    {
        $leader = new BrokerConnection(
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 17]]], 250),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 18]]]),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 19]]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $client = $this->throttleAwareClient();
        $client->produce([self::TOPIC => [0 => [new Record('first')]]]);
        $client->produce([self::TOPIC => [0 => [new Record('second')]]]);
        $client->produce([self::TOPIC => [0 => [new Record('third')]]]);

        self::assertSame([250000], $client->getSleeps(), 'a throttle is spent by the request that waited it out');
    }

    public function testTheThrottleOfOneBrokerDoesNotDelayARequestToAnother(): void
    {
        // The two partitions have different leaders, so the second round sends one request to each: only the
        // broker that muted its channel is waited for
        $first  = new BrokerConnection(
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 17]]], 500),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 18]]])
        );
        $second = new BrokerConnection(
            ResponseFrame::produce(0, [self::TOPIC => [1 => [0, 42]]]),
            ResponseFrame::produce(0, [self::TOPIC => [1 => [0, 43]]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $first)
            ->on(self::SECOND_LEADER, $second)
            ->install();

        $client = $this->throttleAwareClient();
        $client->produce([
            self::TOPIC => [0 => [new Record('to the first')], 1 => [new Record('to the second')]],
        ]);
        $client->produce([
            self::TOPIC => [0 => [new Record('again')], 1 => [new Record('again')]],
        ]);

        self::assertSame([500000], $client->getSleeps(), 'only the throttled broker is waited for');
        self::assertSame(2, $first->getRequestCount());
        self::assertSame(2, $second->getRequestCount());
    }

    public function testTheThrottleWaitCanBeSwitchedOffAndTheFieldIsStillReported(): void
    {
        $leader = new BrokerConnection(
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 17]]], 400),
            ResponseFrame::produce(0, [self::TOPIC => [0 => [0, 18]]])
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, $leader)
            ->install();

        $client = $this->throttleAwareClient([ClientConfig::THROTTLE_WAIT => false]);
        $result = $client->produce([self::TOPIC => [0 => [new Record('first')]]]);
        $client->produce([self::TOPIC => [0 => [new Record('second')]]]);

        self::assertSame([], $client->getSleeps(), 'with the option off the stall happens on the broker side');
        self::assertSame(
            400,
            $result[self::TOPIC][0]->throttleTimeMs,
            'the throttle time is read and reported either way, only the waiting is switched off'
        );
        self::assertTrue(ClientConfig::getDefaultConfiguration()[ClientConfig::THROTTLE_WAIT]);
    }

    public function testAFetchAnswerReportsItsThrottleTimeFromTheHeadOfTheFrame(): void
    {
        // The Fetch api carries `throttle_time_ms` in front of its topics array, the Produce api behind them:
        // both are read, because the client looks the field up by name on the answer it just decoded
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_LEADER, new BrokerConnection(
                ResponseFrame::fetch(0, [self::TOPIC => [0 => [0, 1, '']]], 750),
                ResponseFrame::fetch(0, [self::TOPIC => [0 => [0, 1, '']]])
            ))
            ->install();

        $client = $this->throttleAwareClient();
        $client->fetchPartitions([self::TOPIC => [0 => 0]], 200);
        $client->fetchPartitions([self::TOPIC => [0 => 0]], 200);

        self::assertSame([750000], $client->getSleeps());
    }

    /**
     * Builds a client whose clock and whose sleep the test controls, on the same scripted brokers
     *
     * @param array<string, mixed> $overrides
     */
    private function throttleAwareClient(array $overrides = []): ThrottleAwareTestClient
    {
        $configuration = self::configuration($overrides);

        return new ThrottleAwareTestClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * Builds a client on a cluster that the scripted brokers answer for
     *
     * @param array<string, mixed> $overrides
     */
    private function client(array $overrides = []): Client
    {
        $configuration = self::configuration($overrides);

        return new Client(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * The configuration of the client above
     *
     * @param array<string, mixed> $overrides
     *
     * @return array<string, mixed>
     */
    private static function configuration(array $overrides = []): array
    {
        return $overrides + [
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
    }

    /**
     * A client that produces the way an idempotent producer configures it: every batch acknowledged by the ISR
     *
     * @param array<string, mixed> $overrides
     */
    private function idempotentClient(array $overrides = []): Client
    {
        return $this->client($overrides + [ProducerConfig::ACKS => ProducerConfig::ACKS_ALL]);
    }

    /**
     * A client that exposes the producer state of {@see Client::produceRecords()}, the way T7 and T8 will fill it
     *
     * @param array<string, mixed> $overrides
     */
    private function transactionalClient(array $overrides = []): TransactionalTestClient
    {
        $configuration = self::configuration($overrides);

        return new TransactionalTestClient(Cluster::bootstrap($configuration), $configuration);
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
