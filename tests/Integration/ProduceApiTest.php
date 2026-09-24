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
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidRecordException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ProduceRequestPartition;
use Protocol\Kafka\Protocol\Data\ProduceRequestTopic;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartitionV0;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopic;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopicV0;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\DeleteRecordsRequest;
use Protocol\Kafka\Protocol\Request\DeleteRecordsResponse;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV4;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV4;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV0;
use Protocol\Kafka\Protocol\Request\ProduceRequestV1;
use Protocol\Kafka\Protocol\Request\ProduceRequestV11;
use Protocol\Kafka\Protocol\Request\ProduceRequestV2;
use Protocol\Kafka\Protocol\Request\ProduceRequestV3;
use Protocol\Kafka\Protocol\Request\ProduceRequestV4;
use Protocol\Kafka\Protocol\Request\ProduceRequestV5;
use Protocol\Kafka\Protocol\Request\ProduceRequestV6;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV0;
use Protocol\Kafka\Protocol\Request\ProduceResponseV1;
use Protocol\Kafka\Protocol\Request\ProduceResponseV11;
use Protocol\Kafka\Protocol\Request\ProduceResponseV3;
use Protocol\Kafka\Protocol\Request\ProduceResponseV4;
use Protocol\Kafka\Protocol\Request\ProduceResponseV5;
use Protocol\Kafka\Protocol\Request\ProduceResponseV6;
use Protocol\Kafka\Tests\Fixture\RemovedVersionProbe;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the Produce API against the Kafka 4.3.1 node of this line.
 *
 * The node validates the CRC of every batch it appends and is the authority on what the api answers: the
 * `LogAppendTime` of a partition, which is -1 for a topic that keeps the `CreateTime` of the producer and the clock
 * of the broker for a topic with `message.timestamp.type=LogAppendTime`, and the `LogStartOffset` of version 5.
 *
 * **Kafka 4.0 removed the versions 0 to 2 (KIP-896)**, and with them the only versions that carry a message set of
 * the formats v0 and v1: a 4.x node still lists Produce from version 0 in its ApiVersions answer (KAFKA-18659) and
 * closes the connection on a frame of those versions, and a version 3 or higher request that carries a message set
 * is refused per partition with the 87 `INVALID_RECORD`. The test data of this class is therefore written as record
 * batches of the message format v2, through the lowest version the node serves, 3, and the refusal of the removed
 * versions is measured instead of sent as if they were served.
 *
 * @see docs/protocol/4.3.md, section "Produce API (key 0, v0 to v12)"
 */
#[CoversClass(ProduceRequest::class)]
#[CoversClass(ProduceRequestV11::class)]
#[CoversClass(ProduceResponseV11::class)]
#[CoversClass(ProduceRequestV4::class)]
#[CoversClass(ProduceRequestV3::class)]
#[CoversClass(ProduceRequestV1::class)]
#[CoversClass(ProduceRequestV0::class)]
#[CoversClass(ProduceResponse::class)]
#[CoversClass(ProduceResponseV4::class)]
#[CoversClass(ProduceResponseV3::class)]
#[CoversClass(ProduceResponseV1::class)]
#[CoversClass(ProduceResponseV0::class)]
#[CoversClass(ProduceRequestTopic::class)]
#[CoversClass(ProduceRequestPartition::class)]
#[CoversClass(ProduceResponseTopic::class)]
#[CoversClass(ProduceResponseTopicV0::class)]
#[CoversClass(ProduceResponsePartition::class)]
#[CoversClass(ProduceResponsePartitionV0::class)]
#[CoversClass(Client::class)]
#[CoversClass(KafkaProducer::class)]
final class ProduceApiTest extends IntegrationTestCase
{
    /**
     * Client id sent along with every request of this test class
     */
    private const string CLIENT_ID = 'kafka-client-t4-produce';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * A fixed CreateTime for the produced records, 2017-03-12T13:20:00Z
     */
    private const int CREATE_TIME = 1489324800000;

    /**
     * Topic of the current test, created and given a leader by {@see ProduceApiTest::setUp()}
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t4-produce');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    public function testProduceWithAcksOneReturnsTheBaseOffsetOfEveryBatch(): void
    {
        $stream = $this->connect();

        $first = $this->produce($stream, self::batchOf([[null, 'a'], [null, 'b']]), 1, 11);
        self::assertSame(0, $first->errorCode);
        self::assertSame(0, $first->baseOffset, 'the first batch starts at the beginning of the log');

        $second = $this->produce($stream, self::batchOf([['key', 'c'], [null, 'd'], [null, 'e']]), 1, 12);
        self::assertSame(0, $second->errorCode);
        self::assertSame(2, $second->baseOffset, 'the offsets increase by the number of appended messages');

        $third = $this->produce($stream, self::batchOf([[null, 'f']]), 1, 13);
        self::assertSame(5, $third->baseOffset);
    }

    public function testProduceWithAcksMinusOneWaitsForTheInSyncReplicas(): void
    {
        $response = $this->produce($this->connect(), self::batchOf([[null, 'committed']]), -1, 21);

        self::assertSame(0, $response->errorCode);
        self::assertSame(0, $response->baseOffset);
    }

    public function testProduceWithAcksZeroIsNotAnsweredButStillAppends(): void
    {
        $stream  = $this->connect();
        $request = new ProduceRequestV3(
            [$this->topic => [0 => self::batchOf([[null, 'fire'], [null, 'forget']])]],
            0,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            31
        );

        self::assertFalse($request->expectsResponse());
        $request->writeTo($stream);

        // Nothing at all comes back for that request: the next answer on this connection belongs to the next request
        new MetadataRequest([$this->topic], true, self::CLIENT_ID, 32)->writeTo($stream);
        self::assertSame(32, MetadataResponse::unpack($stream)->getCorrelationId());

        // ... and the two messages were nevertheless appended, so the next batch starts at offset 2
        $acknowledged = $this->produce($stream, self::batchOf([[null, 'acked']]), 1, 33);

        self::assertSame(0, $acknowledged->errorCode);
        self::assertSame(2, $acknowledged->baseOffset);
    }

    public function testProducerSendsRecordsThroughTheCluster(): void
    {
        $producer = new KafkaProducer($this->producerConfiguration(1));

        $acknowledged = [];
        $collect      = static function (RecordMetadata $metadata) use (&$acknowledged): void {
            $acknowledged[] = $metadata;
        };

        $producer->send($this->topic, Record::fromValue('through the producer'), 0)->then($collect);
        $producer->send($this->topic, Record::fromKeyValue('key', 'and another one'), 0)->then($collect);

        // `batch.size` defaults to 0, so every record is sent on its own and both promises are already settled
        self::assertCount(2, $acknowledged);
        self::assertSame($this->topic, $acknowledged[0]->topic);
        self::assertSame(0, $acknowledged[0]->partition);
        self::assertSame([0, 1], array_column($acknowledged, 'offset'));
        self::assertCount(3, $producer->partitionsFor($this->topic));
    }

    public function testClientDoesNotWaitForAnAnswerWithAcksZero(): void
    {
        $configuration = $this->producerConfiguration(0) + ProducerConfig::getDefaultConfiguration();
        $cluster       = Cluster::bootstrap($configuration);

        $result = new Client($cluster, $configuration)
            ->produce([$this->topic => [1 => [Record::fromValue('no acknowledgement')]]]);

        self::assertSame([], $result, 'a fire-and-forget produce request has no response to report');

        // The very same connection carried the request, so the broker has appended it before answering this one
        $acknowledged = $this->produce(
            $cluster->leaderFor($this->topic, 1)->getConnection($configuration),
            self::batchOf([[null, 'acked']]),
            1,
            41,
            1
        );

        self::assertSame(0, $acknowledged->errorCode);
        self::assertSame(1, $acknowledged->baseOffset, 'the fire-and-forget message occupies the offset 0');
    }

    public function testEveryVersionOfTheAnswerCarriesTheFieldsOfItsOwnVersion(): void
    {
        $stream = $this->connect();

        new ProduceRequestV3(
            [$this->topic => [0 => self::batchOf([[null, 'version three']])]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            41
        )->writeTo($stream);
        $versionThree = ProduceResponseV3::unpack($stream);

        self::assertSame(41, $versionThree->getCorrelationId());
        self::assertSame(0, $versionThree->topics[$this->topic]->partitions[0]->errorCode);
        self::assertSame(0, $versionThree->throttleTime, 'the test broker enforces no producer quota');
        self::assertSame(-1, $versionThree->topics[$this->topic]->partitions[0]->logAppendTime);

        new ProduceRequestV5(
            [$this->topic => [0 => self::batchOf([[null, 'version five']])]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            42
        )->writeTo($stream);
        $versionFive = ProduceResponseV5::unpack($stream);

        self::assertSame(42, $versionFive->getCorrelationId());
        self::assertSame(1, $versionFive->topics[$this->topic]->partitions[0]->baseOffset);
        self::assertSame(0, $versionFive->topics[$this->topic]->partitions[0]->logStartOffset);
        self::assertSame(
            $versionThree->getMessageSize() + 8,
            $versionFive->getMessageSize(),
            'the LogStartOffset of the partition is the only difference between v5 and v3'
        );
    }

    public function testTheVersionsZeroToTwoAreAdvertisedButCloseTheConnection(): void
    {
        // KIP-896 (Kafka 4.0): "Versions 0-2 were removed in Apache Kafka 4.0, version 3 is the new baseline. Due to
        // a bug in librdkafka, these versions have to be included in the api versions response (see KAFKA-18659),
        // but are rejected otherwise" - `ProduceRequest.json` @ 4.0.0
        $stream = $this->connect();
        new ApiVersionsRequest(self::CLIENT_ID, 44)->writeTo($stream);
        $produce = ApiVersionsResponse::unpack($stream)->apiVersions[ApiKeys::PRODUCE];

        self::assertSame(0, $produce->minVersion, 'the row still starts at 0 (KAFKA-18659)');
        self::assertGreaterThanOrEqual(ProduceRequest::BASELINE_RAISED_WITH_VERSION, $produce->maxVersion);

        $probe = new RemovedVersionProbe(self::firstBootstrapServer());
        $batch = self::batchOf([[null, 'removed']]);
        foreach ([ProduceRequestV0::class, ProduceRequestV1::class, ProduceRequestV2::class] as $version => $class) {
            self::assertSame(
                RemovedVersionProbe::CLOSED,
                $probe->send(new $class([$this->topic => [0 => $batch]], 1, self::PRODUCE_TIMEOUT_MS, self::CLIENT_ID, 45)),
                "a Produce v{$version} costs the connection, whatever its record set"
            );
        }

        // ... and nothing of them reached the log
        self::assertSame(0, $this->produce($stream, self::batchOf([[null, 'served']]), 1, 46)->baseOffset);
    }

    public function testTheAppendTimeOfATopicThatKeepsTheCreateTimeIsMinusOne(): void
    {
        $partition = $this->produce(
            $this->connect(),
            RecordBatch::fromRecords([new Record('create time', null, 0, null, self::CREATE_TIME)])->toBuffer(),
            1,
            51
        );

        self::assertSame(0, $partition->errorCode);
        self::assertSame(
            ProduceResponsePartition::NO_LOG_APPEND_TIME,
            $partition->logAppendTime,
            'the broker stamped nothing, the timestamps of the producer are the ones the log holds'
        );
    }

    public function testALogAppendTimeTopicAnswersWithTheClockOfTheBroker(): void
    {
        $topic = $this->createLogAppendTimeTopic();

        $before    = (int) (microtime(true) * 1000);
        $partition = $this->produce(
            $this->connect(),
            RecordBatch::fromRecords([
                new Record('append time', null, 0, null, self::CREATE_TIME),
                new Record('same batch', null, 0, null, self::CREATE_TIME + 1000),
            ])->toBuffer(),
            1,
            52,
            0,
            $topic
        );
        $after = (int) (microtime(true) * 1000);

        self::assertSame(0, $partition->errorCode);
        self::assertGreaterThanOrEqual($before, $partition->logAppendTime);
        self::assertLessThanOrEqual($after, $partition->logAppendTime);

        // That value is what the log really holds: the broker overwrote the MaxTimestamp of the batch with it and
        // marked the batch with the timestamp type LogAppendTime, which every record of it inherits - read with
        // the lowest Fetch version a 4.x node serves, 4, which answers the batch as the log holds it
        $stream = $this->connect();
        new FetchRequestV4([$topic => [0 => 0]], 1000, 1, 65536, -1, self::CLIENT_ID, 53)->writeTo($stream);
        $records = FetchResponseV4::unpack($stream)
            ->topics[$topic]
            ->partitions[0]
            ->getRecords()
            ->getRecords();

        self::assertCount(2, $records);
        self::assertSame(
            [$partition->logAppendTime, $partition->logAppendTime],
            array_map(static fn(Record $record): ?int => $record->timestamp, $records),
            'every message of the batch carries the append time the answer reported'
        );
        self::assertSame(
            [TimestampType::LOG_APPEND_TIME, TimestampType::LOG_APPEND_TIME],
            array_map(static fn(Record $record): int => $record->timestampType, $records)
        );
    }

    public function testAMessageFormatV0OrV1BatchCanNoLongerReachTheLog(): void
    {
        // The only versions that carry a message set are the removed ones: a Produce v2 costs the connection, and a
        // version 3 or higher request refuses the magic 0 and 1 per partition with the 87 of KIP-467, as it did on
        // 3.9.2. What a 3.x broker did with a v0/v1 message set of a Produce v2 - convert it into the format of the
        // topic - is gone with the version, and so is the topic config `message.format.version` itself
        $probe = new RemovedVersionProbe(self::firstBootstrapServer());
        foreach ([Message::MAGIC_V0, Message::MAGIC_V1] as $magic) {
            $messageSet = MessageSet::fromRecords([new Record('magic ' . $magic)], 0, $magic)->toBuffer();

            self::assertSame(
                RemovedVersionProbe::CLOSED,
                $probe->send(new ProduceRequestV2(
                    [$this->topic => [0 => $messageSet]],
                    1,
                    self::PRODUCE_TIMEOUT_MS,
                    self::CLIENT_ID,
                    54
                )),
                "a magic {$magic} message set in a Produce v2"
            );

            $partition = $this->produce($this->connect(), $messageSet, 1, 55);
            self::assertSame(KafkaException::INVALID_RECORD, $partition->errorCode, "a magic {$magic} message set in a Produce v3");
            self::assertSame(-1, $partition->baseOffset);
        }
    }

    public function testAVersionTwoRequestIsRefusedEvenWithARecordBatchOfTheMessageFormatV2(): void
    {
        // On a 3.x broker the check ran the other way round only - a batch of the NEWEST format was accepted by every
        // version. A 4.x node never gets that far: the version is refused by the request parser
        $batch = RecordBatch::fromRecords([new Record('magic two')->withCreateTime(self::currentTimestampMs())])
            ->toBuffer();

        self::assertSame(
            RemovedVersionProbe::CLOSED,
            new RemovedVersionProbe(self::firstBootstrapServer())->send(new ProduceRequestV2(
                [$this->topic => [0 => $batch]],
                1,
                self::PRODUCE_TIMEOUT_MS,
                self::CLIENT_ID,
                56
            ))
        );
        self::assertSame(0, $this->produce($this->connect(), $batch, 1, 57)->baseOffset, 'the log is untouched');
    }

    public function testAVersionTwelveRequestOutsideATransactionIsAnsweredLikeVersionEleven(): void
    {
        // `ProduceRequest.json` @ 4.0.0: "Version 12 is the same as version 11 (KIP-890)" - what it changes is the
        // meaning of a TRANSACTIONAL batch on a node with `transaction.version` 2; a batch without a transactional id
        // is appended exactly as at version 11, into an answer of the very same bytes
        $stream = $this->connect();
        $batch  = self::batchOf([[null, 'outside a transaction']]);

        new ProduceRequestV11([$this->topic => [0 => $batch]], 1, self::PRODUCE_TIMEOUT_MS, self::CLIENT_ID, 58)
            ->writeTo($stream);
        $eleven = ProduceResponseV11::unpack($stream);
        new ProduceRequest([$this->topic => [0 => $batch]], 1, self::PRODUCE_TIMEOUT_MS, self::CLIENT_ID, 58)
            ->writeTo($stream);
        $twelve = ProduceResponse::unpack($stream);

        self::assertSame(12, $twelve::VERSION);
        self::assertSame(0, $eleven->topics[$this->topic]->partitions[0]->baseOffset);
        self::assertSame(0, $twelve->topics[$this->topic]->partitions[0]->errorCode);
        self::assertSame(1, $twelve->topics[$this->topic]->partitions[0]->baseOffset);
        $eleven->topics[$this->topic]->partitions[0]->baseOffset = 1;
        self::assertSame(
            bin2hex((string) $eleven),
            bin2hex((string) $twelve),
            'the two answers differ in the base offset alone'
        );
    }

    public function testAVersionThreeRequestCarriesTheRecordBatchAndTheTransactionalIdOfItsProducer(): void
    {
        $stream    = $this->connect();
        $timestamp = self::currentTimestampMs();
        $batch     = RecordBatch::fromRecords([
            new Record('with a header', 'a key')->withCreateTime($timestamp)
                ->withHeaders(new Header('trace-id', 'abc'), new Header('empty')),
        ]);

        new ProduceRequestV3(
            [$this->topic => [0 => $batch]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            57
        )->writeTo($stream);
        $response  = ProduceResponseV3::unpack($stream);
        $partition = $response->topics[$this->topic]->partitions[0];

        self::assertSame(57, $response->getCorrelationId());
        self::assertSame(0, $partition->errorCode);
        self::assertSame(0, $partition->baseOffset);
        self::assertSame(-1, $partition->logAppendTime, 'the topic keeps the CreateTime of the producer');
        self::assertSame(
            $response->getMessageSize(),
            $this->produceResponseSizeOfVersionFour($this->topic, 1),
            'the answer of version 3 is the answer of version 4, byte for byte - and of version 2 before it'
        );

        // Only a Fetch v4 or above brings the headers back: every lower version is answered in a format that has
        // no place for them
        $stream = $this->connect();
        new FetchRequest(
            [$this->topic => [0 => 0]],
            1000,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            58,
            // Version 13 names the topic by its id and by nothing else (KIP-516)
            topicIds: [$this->topic => self::topicIdOf($this->topic)]
        )->writeTo($stream);
        $fetched = self::fetchedTopic(FetchResponse::unpack($stream), $this->topic)->partitions[0];
        $records = $fetched->getRecords()->getRecords();

        self::assertSame(RecordBatch::MAGIC, $fetched->getRecords()->getMagic());
        self::assertCount(1, $records);
        self::assertSame('with a header', $records[0]->value);
        self::assertSame('a key', $records[0]->key);
        self::assertSame($timestamp, $records[0]->timestamp);
        self::assertSame(['trace-id', 'empty'], array_map(
            static fn(Header $header): string => $header->key,
            $records[0]->headers
        ));
        self::assertSame(['abc', null], array_map(
            static fn(Header $header): ?string => $header->value,
            $records[0]->headers
        ));
    }

    public function testAVersionThreeOrHigherRequestRefusesAMessageSetWithTheErrorCode87(): void
    {
        // `ProduceRequest.validateRecords` @ 2.8.2: "Produce requests with version 3 or higher are only allowed to
        // contain record batches with magic version 2". A **1.1.1** broker turned that into an
        // InvalidRequestException and closed the socket; a 2.8.2 broker answers the offending partition with the
        // error code 87 INVALID_RECORD - the code KIP-467 gave to record validation - and leaves the connection
        // open, which is what every version from 3 up does here.
        $stream = $this->connect();

        new ProduceRequest(
            [$this->topic => [0 => MessageSet::fromRecords([new Record('a message set')], 0, Message::MAGIC_V1)]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            59
        )->writeTo($stream);

        $response  = ProduceResponse::unpack($stream);
        $partition = $response->topics[$this->topic]->partitions[0];

        self::assertSame(59, $response->getCorrelationId());
        self::assertSame(
            KafkaException::INVALID_RECORD,
            $partition->errorCode,
            'a legacy message set in a version 12 request is refused per partition, not by closing the connection'
        );
        self::assertSame(87, KafkaException::INVALID_RECORD);
        self::assertInstanceOf(
            InvalidRecordException::class,
            KafkaException::fromCode($partition->errorCode, ['topic' => $this->topic])
        );

        // The connection survives it: the very same records travel as a record batch right afterwards
        $accepted = $this->produce($stream, self::batchOf([[null, 'a record batch']]), 1, 60);
        self::assertSame(0, $accepted->errorCode);
    }

    public function testAVersionFourRequestIsAnsweredWithTheVersionThreeFrame(): void
    {
        // PRODUCE_RESPONSE_V4 = PRODUCE_RESPONSE_V3 = PRODUCE_RESPONSE_V2 @ 1.1.1: version 4 (Kafka 1.0) states
        // that the client understands the error code 56 KAFKA_STORAGE_ERROR and changes no byte of either frame
        $stream = $this->connect();
        new ProduceRequestV4(
            [$this->topic => [2 => RecordBatch::fromRecords(
                [new Record('version four')->withCreateTime(self::currentTimestampMs())]
            )]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            61
        )->writeTo($stream);

        $response  = ProduceResponseV4::unpack($stream);
        $partition = $response->topics[$this->topic]->partitions[2];

        self::assertSame(61, $response->getCorrelationId());
        self::assertSame(0, $partition->errorCode);
        self::assertSame(0, $partition->baseOffset);
        self::assertSame(-1, $partition->logAppendTime);
        self::assertSame(
            ProduceResponsePartition::INVALID_OFFSET,
            $partition->logStartOffset,
            'a version below 5 does not report a log start offset at all'
        );
        self::assertSame(
            $response->getMessageSize(),
            $this->produceResponseSizeOfVersionThree($this->topic, 1),
            'the answer of version 4 is the answer of version 3, byte for byte'
        );
    }

    public function testAVersionFiveAnswerReportsTheLogStartOffsetOfThePartition(): void
    {
        // Version 5 (Kafka 1.0) appended LogStartOffset to every partition entry: the first offset the log still
        // holds. It is 0 for an untouched log and moves with the retention or with a DeleteRecords request, which
        // is what tells an idempotent producer that a 59 UNKNOWN_PRODUCER_ID is spurious.
        $stream = $this->connect();
        $first  = $this->produceRecordBatch($stream, 1, ['first', 'second'], 62);

        self::assertSame(0, $first->errorCode);
        self::assertSame(0, $first->baseOffset);
        self::assertSame(0, $first->logStartOffset, 'nothing has been deleted from the front of this log yet');

        new DeleteRecordsRequest([$this->topic => [1 => 2]], 30000, self::CLIENT_ID, 63)->writeTo($stream);
        $deleted = DeleteRecordsResponse::unpack($stream)->topics[$this->topic]->partitions[1];

        self::assertSame(0, $deleted->errorCode);
        self::assertSame(2, $deleted->lowWatermark);

        $second = $this->produceRecordBatch($stream, 1, ['third'], 64);

        self::assertSame(0, $second->errorCode);
        self::assertSame(2, $second->baseOffset);
        self::assertSame(2, $second->logStartOffset, 'the DeleteRecords request moved the front of the log');
        self::assertSame(3, $this->produceRecordBatch($stream, 1, ['fourth'], 65)->baseOffset);
    }

    public function testTheBrokerAcceptsTheVersionsThreeToSixWithOneAndTheSameBody(): void
    {
        // `ProduceRequest.json` @ 2.8.2 has no field above version 3, and the broker really accepts all four:
        // what the later versions state is which error code, which answer and - with version 6 (Kafka 2.0,
        // KIP-219) - which throttling behaviour the client understands
        $stream    = $this->connect();
        $timestamp = self::currentTimestampMs();
        $frames    = [];
        $offsets   = [];
        $versions  = [
            3 => [ProduceRequestV3::class, ProduceResponseV3::class],
            4 => [ProduceRequestV4::class, ProduceResponseV4::class],
            5 => [ProduceRequestV5::class, ProduceResponseV5::class],
            6 => [ProduceRequestV6::class, ProduceResponseV6::class],
        ];
        foreach ($versions as $version => [$requestClass, $responseClass]) {
            $request = new $requestClass(
                [$this->topic => [0 => RecordBatch::fromRecords(
                    [new Record('same body')->withCreateTime($timestamp)]
                )]],
                1,
                self::PRODUCE_TIMEOUT_MS,
                self::CLIENT_ID,
                60 + $version
            );
            // Everything behind `Size ApiKey ApiVersion CorrelationId`, i.e. the client id and the body
            $frames[$version] = substr(bin2hex((string) $request), 24);
            $request->writeTo($stream);
            $partition          = $responseClass::unpack($stream)->topics[$this->topic]->partitions[0];
            $offsets[$version]  = $partition->baseOffset;
            self::assertSame(0, $partition->errorCode, "the broker accepted the version {$version} request");
        }

        self::assertCount(1, array_unique($frames), 'the four versions send one and the same body');
        self::assertSame([3 => 0, 4 => 1, 5 => 2, 6 => 3], $offsets, 'every one of them appended one record');
    }

    /**
     * Appends a record batch of the message format v2 with a Produce v5 request and returns the partition entry
     *
     * @param list<string> $values Values of the records to append
     */
    private function produceRecordBatch(Stream $stream, int $partition, array $values, int $correlationId): ProduceResponsePartition
    {
        $records = [];
        foreach ($values as $value) {
            $records[] = new Record($value)->withCreateTime(self::currentTimestampMs());
        }

        new ProduceRequest(
            [$this->topic => [$partition => RecordBatch::fromRecords($records)]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            $correlationId
        )->writeTo($stream);

        return ProduceResponse::unpack($stream)->topics[$this->topic]->partitions[$partition];
    }

    /**
     * Returns the size of the answer that the very same append gets from a version 3 request
     */
    private function produceResponseSizeOfVersionThree(string $topic, int $partition): int
    {
        $stream = $this->connect();
        new ProduceRequestV3(
            [$topic => [$partition => RecordBatch::fromRecords(
                [new Record('version three')->withCreateTime(self::currentTimestampMs())]
            )]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            60
        )->writeTo($stream);

        return ProduceResponseV3::unpack($stream)->getMessageSize();
    }

    /**
     * Returns the size of the answer that the very same append gets from a version 4 request
     */
    private function produceResponseSizeOfVersionFour(string $topic, int $partition): int
    {
        $stream = $this->connect();
        new ProduceRequestV4(
            [$topic => [$partition => RecordBatch::fromRecords(
                [new Record('version four')->withCreateTime(self::currentTimestampMs())]
            )]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            60
        )->writeTo($stream);

        return ProduceResponseV4::unpack($stream)->getMessageSize();
    }

    /**
     * Builds a record batch of the message format v2 out of `[key, value]` pairs, stamped with the current time
     *
     * @param list<array{string|null, string|null}> $keyValues
     */
    private static function batchOf(array $keyValues): string
    {
        $timestamp = self::currentTimestampMs();
        $records   = [];
        foreach ($keyValues as [$key, $value]) {
            $records[] = new Record($value, $key)->withCreateTime($timestamp);
        }

        return RecordBatch::fromRecords($records)->toBuffer();
    }

    /**
     * The current time in milliseconds, the `CreateTime` a producer stamps a record with.
     *
     * A test never writes a fixed timestamp of the past: the retention of the broker deletes a segment by the
     * largest timestamp it holds, so a record stamped with 2020 disappears from a container whose clock says 2026.
     */
    private static function currentTimestampMs(): int
    {
        return (int) round(microtime(true) * 1000);
    }

    /**
     * Creates a topic whose broker stamps every message it appends with its own clock
     */
    private function createLogAppendTimeTopic(): string
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
        $topic = self::uniqueTopicName('t4-produce-lat');

        $errors = new AdminClient(Cluster::bootstrap($configuration), $configuration)->createTopics([
            new NewTopic($topic, 1, 1, [], ['message.timestamp.type' => 'LogAppendTime']),
        ]);
        self::assertSame([$topic => null], $errors, 'the controller created the LogAppendTime topic');

        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($topic);

        return $topic;
    }

    /**
     * Produces one record set to a partition of the topic of this test and returns the answer for that partition
     *
     * The request is a **version 3**, the lowest version a node of Kafka 4.0 or later serves (KIP-896) and the
     * first that carries a record batch of the message format v2; a message set of the formats v0 and v1 is
     * refused in it per partition with the 87 `INVALID_RECORD`.
     */
    private function produce(
        Stream $stream,
        string $recordSet,
        int $requiredAcks,
        int $correlationId,
        int $partition = 0,
        ?string $topic = null
    ): ProduceResponsePartition {
        $topic ??= $this->topic;
        $request = new ProduceRequestV3(
            [$topic => [$partition => $recordSet]],
            $requiredAcks,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            $correlationId
        );
        $request->writeTo($stream);

        $response = ProduceResponseV3::unpack($stream);
        self::assertSame($correlationId, $response->getCorrelationId());
        self::assertArrayHasKey($topic, $response->topics);

        return $response->topics[$topic]->partitions[$partition];
    }

    /**
     * @return array<string, mixed>
     */
    private function producerConfiguration(int $requiredAcks): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID         => self::CLIENT_ID,
            ProducerConfig::ACKS            => $requiredAcks,
            ProducerConfig::TIMEOUT_MS      => self::PRODUCE_TIMEOUT_MS,
        ];
    }
}
