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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetAndTimestamp;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV6;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV7;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV8;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV9;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV6;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV7;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV8;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the timestamp lookup of the Offsets (ListOffset) API - version 1 of Kafka 0.10.1, and version 2 with
 * the isolation level of Kafka 0.11 - against a real 0.11.0.3 broker.
 *
 * Every partition under test holds five records one second apart, with `CreateTime` values that start one hour
 * before the run, so that the answer of the broker is fully determined by the timestamp that is searched for. The
 * timestamps are relative to the clock and not fixed, because time-based retention deletes a segment by the
 * largest timestamp it holds: a segment stamped with a date years in the past is removed at the next retention
 * check of the broker (every five minutes), in the middle of the run. What the tests assert is the table of
 * "What the broker answers" in the protocol document.
 *
 * The one answer the node cannot be made to give any more is the **43 UNSUPPORTED_FOR_MESSAGE_FORMAT** of a
 * timestamp lookup on a log without timestamps: that needed a message format v0 log, and KIP-724 (Kafka 3.0)
 * retired `message.format.version`, so every log of this node is a record batch v2 and every partition has a time
 * index. The code stays in {@see KafkaException} and in the error table, it is simply not reachable from a client
 * of a 3.x broker.
 *
 * @see docs/protocol/4.3.md, section "Offsets API (key 2, v0 to v10), a.k.a. ListOffset"
 */
#[CoversClass(Client::class)]
#[CoversClass(AdminClient::class)]
#[CoversClass(KafkaConsumer::class)]
#[CoversClass(OffsetAndTimestamp::class)]
#[CoversClass(OffsetsRequest::class)]
#[CoversClass(OffsetsRequestV7::class)]
#[CoversClass(OffsetsRequestV8::class)]
#[CoversClass(OffsetsResponse::class)]
#[CoversClass(OffsetsResponseV7::class)]
#[CoversClass(OffsetsResponseV8::class)]
#[CoversClass(OffsetsResponsePartition::class)]
final class OffsetsByTimestampTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t5-timestamps';

    /**
     * The partition that every test of this class produces into and looks up
     */
    private const int PARTITION = 0;

    /**
     * `CreateTime` of the first record of a prepared topic, one hour before the run; the following ones are one
     * second apart. Fixed for the whole class, because the prepared topics are shared by the test methods.
     */
    private static ?int $firstTimestamp = null;

    /**
     * Number of records that a prepared topic holds
     */
    private const int RECORD_COUNT = 5;

    /**
     * `CreateTime` offsets, relative to {@see self::firstTimestamp()}, of the records of the max-timestamp topic
     *
     * They are deliberately **not** monotonic: the largest one sits in the middle of the log, so that the answer
     * of the target time -3 (KIP-734) is the offset **1** while the log ends at the offset 3. A log whose
     * timestamps rise with its offsets could not tell the two questions apart.
     *
     * @var list<int>
     */
    private const array OUT_OF_ORDER_STAMPS = [0, 5000, 2000];

    /**
     * How long to wait for a freshly created topic to have a leader, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * Topics of this test run, prepared once and shared by the test methods, as key => topic name
     *
     * @var array<string, string>
     */
    private static array $topics = [];

    public function testTheTwoSpecialTargetTimesAnswerTheBoundsOfTheLogWithoutATimestamp(): void
    {
        $topic = $this->preparedTopic('plain');

        $earliest = $this->lookUp($topic, OffsetsRequest::EARLIEST);
        $latest   = $this->lookUp($topic, OffsetsRequest::LATEST);

        self::assertInstanceOf(OffsetAndTimestamp::class, $earliest);
        self::assertSame(0, $earliest->offset, 'nothing was deleted, so the log still starts at offset 0');
        self::assertSame(
            OffsetsResponsePartition::UNKNOWN_TIMESTAMP,
            $earliest->timestamp,
            'the broker does not read the message the offset points at'
        );

        self::assertInstanceOf(OffsetAndTimestamp::class, $latest);
        self::assertSame(self::RECORD_COUNT, $latest->offset, 'the offset the next produced record will get');
        self::assertSame(OffsetsResponsePartition::UNKNOWN_TIMESTAMP, $latest->timestamp);
    }

    public function testATimestampBelowTheFirstRecordFindsTheFirstRecord(): void
    {
        $found = $this->lookUp($this->preparedTopic('plain'), self::firstTimestamp() - 5000);

        self::assertInstanceOf(OffsetAndTimestamp::class, $found);
        self::assertSame(0, $found->offset);
        self::assertSame(self::firstTimestamp(), $found->timestamp);
    }

    public function testATimestampBetweenTwoRecordsFindsTheLaterOne(): void
    {
        $found = $this->lookUp($this->preparedTopic('plain'), self::firstTimestamp() + 1500);

        self::assertInstanceOf(OffsetAndTimestamp::class, $found);
        self::assertSame(2, $found->offset, 'the first record whose own timestamp is at or after the target time');
        self::assertSame(self::firstTimestamp() + 2000, $found->timestamp);
    }

    public function testTheTimestampOfARecordFindsThatVeryRecord(): void
    {
        $found = $this->lookUp($this->preparedTopic('plain'), self::firstTimestamp() + 4000);

        self::assertInstanceOf(OffsetAndTimestamp::class, $found);
        self::assertSame(4, $found->offset);
        self::assertSame(self::firstTimestamp() + 4000, $found->timestamp);
    }

    public function testATimestampAboveTheLastRecordIsAnsweredWithoutAnOffsetAndWithoutAnError(): void
    {
        $topic = $this->preparedTopic('plain');

        self::assertNull(
            $this->lookUp($topic, self::firstTimestamp() + 9000),
            'the broker answers the error code 0 with the offset -1, which this client reports as null'
        );
        self::assertSame(
            [$topic => [self::PARTITION => OffsetsResponsePartition::UNKNOWN_OFFSET]],
            $this->client($topic)->fetchTopicPartitionOffsets(
                [$topic => [self::PARTITION => self::firstTimestamp() + 9000]]
            ),
            'the plain offset of a partition that holds no such message is -1'
        );
    }

    public function testAnEmptyPartitionMatchesNoTimestampAtAll(): void
    {
        $topic = $this->preparedTopic('empty');

        self::assertNull($this->lookUp($topic, self::firstTimestamp()));
        self::assertSame(0, $this->lookUp($topic, OffsetsRequest::LATEST)?->offset, 'an empty log ends at offset 0');
        self::assertSame(0, $this->lookUp($topic, OffsetsRequest::EARLIEST)?->offset);
    }

    public function testACompressedBatchIsResolvedToTheInnerRecord(): void
    {
        // The whole batch is one wrapper message on disk, and the broker still answers the offset of the single
        // inner record, not the base offset of the wrapper
        $found = $this->lookUp($this->preparedTopic('gzip'), self::firstTimestamp() + 1500);

        self::assertInstanceOf(OffsetAndTimestamp::class, $found);
        self::assertSame(2, $found->offset);
        self::assertSame(self::firstTimestamp() + 2000, $found->timestamp);
    }

    public function testALogAppendTimeTopicIsSearchedByTheAppendTimeOfTheBroker(): void
    {
        $topic = $this->preparedTopic('log-append-time');

        // The CreateTime values the producer sent are replaced by the broker, and they lie far in the past, so a
        // lookup for them finds the very first record
        $found = $this->lookUp($topic, self::firstTimestamp() + 1500);

        self::assertInstanceOf(OffsetAndTimestamp::class, $found);
        self::assertSame(0, $found->offset);
        self::assertGreaterThan(
            self::firstTimestamp(),
            $found->timestamp,
            'the answer carries the append time of the broker, not the timestamp of the producer'
        );

        // Every record of one batch is appended at the same instant, so nothing lies after that append time
        self::assertNull($this->lookUp($topic, $found->timestamp + 60000));
    }

    public function testAnUnknownPartitionIsReportedPerPartition(): void
    {
        $topic = $this->preparedTopic('plain');

        try {
            // The topic is created with one partition, so partition 7 does not exist
            $this->client($topic)->fetchTopicPartitionOffsetsForTimes([$topic => [7 => OffsetsRequest::LATEST]]);
            self::fail('An unknown partition is expected to fail');
        } catch (TopicPartitionRequestException $exception) {
            self::assertArrayHasKey($topic, $exception->getExceptions());
        }
    }

    public function testTheConsumerApiReportsTheBoundsAndTheTimestampsOfItsPartitions(): void
    {
        $topic    = $this->preparedTopic('plain');
        $consumer = new KafkaConsumer([
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ConsumerConfig::GROUP_ID                => 't5-timestamps-' . bin2hex(random_bytes(4)),
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
        ]);

        try {
            self::assertSame(
                [$topic => [self::PARTITION => 0]],
                $consumer->beginningOffsets([$topic => [self::PARTITION]])
            );
            self::assertSame(
                [$topic => [self::PARTITION => self::RECORD_COUNT]],
                $consumer->endOffsets([$topic => [self::PARTITION]])
            );

            $found = $consumer->offsetsForTimes([$topic => [self::PARTITION => self::firstTimestamp() + 2500]]);

            $offsetAndTimestamp = $found[$topic][self::PARTITION];
            self::assertInstanceOf(OffsetAndTimestamp::class, $offsetAndTimestamp);
            self::assertSame(3, $offsetAndTimestamp->offset);
            self::assertSame(self::firstTimestamp() + 3000, $offsetAndTimestamp->timestamp);

            // Nothing was moved by the lookup, so a consumer that wants to read from there seeks itself
            $consumer->assign([$topic => [self::PARTITION]]);
            $consumer->seek($topic, self::PARTITION, $offsetAndTimestamp->offset);
            self::assertSame($offsetAndTimestamp->offset, $consumer->position($topic, self::PARTITION));
        } finally {
            $consumer->close();
        }
    }

    public function testTheMaxTimestampFindsTheLargestTimestampAndNotTheEndOfTheLog(): void
    {
        $topic = $this->preparedTopic('max-timestamp');

        $found = $this->lookUp($topic, OffsetsRequest::MAX_TIMESTAMP);

        self::assertInstanceOf(OffsetAndTimestamp::class, $found);
        self::assertSame(
            1,
            $found->offset,
            'the largest timestamp of this log sits in the middle of it, at the offset 1'
        );
        self::assertSame(
            self::firstTimestamp() + self::OUT_OF_ORDER_STAMPS[1],
            $found->timestamp,
            'unlike -1 and -2, the answer of -3 carries the timestamp it found'
        );
        self::assertSame(
            count(self::OUT_OF_ORDER_STAMPS),
            $this->lookUp($topic, OffsetsRequest::LATEST)?->offset,
            'the log ends behind the record that carries the largest timestamp, which is the whole point of -3'
        );
    }

    public function testTheConsumerAndTheAdminClientBothAskForTheMaxTimestamp(): void
    {
        $topic    = $this->preparedTopic('max-timestamp');
        $expected = self::firstTimestamp() + self::OUT_OF_ORDER_STAMPS[1];

        $consumer = new KafkaConsumer([
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ConsumerConfig::GROUP_ID                => 't5-timestamps-' . bin2hex(random_bytes(4)),
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
        ]);

        try {
            $fromConsumer = $consumer->maxTimestampOffsets([$topic => [self::PARTITION]])[$topic][self::PARTITION];
            self::assertInstanceOf(OffsetAndTimestamp::class, $fromConsumer);
            self::assertSame(1, $fromConsumer->offset);
            self::assertSame($expected, $fromConsumer->timestamp);
            self::assertSame(
                0,
                $fromConsumer->leaderEpoch,
                'the answer carries the leader epoch of KIP-320 like every other lookup of this api'
            );
        } finally {
            $consumer->close();
        }

        $configuration = $this->configuration();
        $admin         = new AdminClient(Cluster::bootstrap($configuration, $topic), $configuration);
        $fromAdmin     = $admin->listMaxTimestampOffsets([$topic => [self::PARTITION]])[$topic][self::PARTITION];

        self::assertInstanceOf(OffsetAndTimestamp::class, $fromAdmin);
        self::assertSame(1, $fromAdmin->offset);
        self::assertSame($expected, $fromAdmin->timestamp);
        self::assertSame(
            [$topic => [self::PARTITION => count(self::OUT_OF_ORDER_STAMPS)]],
            $admin->listOffsets([$topic => [self::PARTITION]]),
            'the plain listOffsets() still answers the end of the log'
        );
    }

    public function testAnEmptyPartitionHasNoMaxTimestampAndNoError(): void
    {
        // An empty log has no largest timestamp, and the node reports that the way it reports "no record matches
        // this timestamp": the error code 0 with the offset -1, which this client hands out as null
        self::assertNull($this->lookUp($this->preparedTopic('empty'), OffsetsRequest::MAX_TIMESTAMP));
    }

    public function testAVersionBelowSevenIsRefusedTheMaxTimestampPerPartition(): void
    {
        // `KafkaApis.handleListOffsetRequestV1AndAbove` @ 3.9.2 demands version 7 for the target time -3 and
        // answers the partition of a lower version with 35 - without closing the connection
        $topic  = $this->preparedTopic('max-timestamp');
        $stream = $this->connect();

        try {
            new OffsetsRequestV6(
                [$topic => [self::PARTITION => OffsetsRequest::MAX_TIMESTAMP]],
                OffsetsRequest::CONSUMER_REPLICA_ID,
                FetchRequest::READ_UNCOMMITTED,
                self::CLIENT_ID,
                7341
            )->writeTo($stream);
            $refusal = OffsetsResponseV6::unpack($stream);

            self::assertSame(7341, $refusal->getCorrelationId());
            $partition = $refusal->topics[$topic]->partitions[self::PARTITION];
            self::assertSame(KafkaException::UNSUPPORTED_VERSION, $partition->errorCode);
            self::assertSame(OffsetsResponsePartition::UNKNOWN_OFFSET, $partition->offset);
            self::assertSame(OffsetsResponsePartition::UNKNOWN_TIMESTAMP, $partition->timestamp);

            // The connection is untouched: the very same frame with -1 is served on it
            new OffsetsRequestV6(
                [$topic => [self::PARTITION => OffsetsRequest::LATEST]],
                OffsetsRequest::CONSUMER_REPLICA_ID,
                FetchRequest::READ_UNCOMMITTED,
                self::CLIENT_ID,
                7342
            )->writeTo($stream);
            $served = OffsetsResponseV6::unpack($stream);

            self::assertSame(7342, $served->getCorrelationId());
            self::assertSame(
                KafkaException::NO_ERROR,
                $served->topics[$topic]->partitions[self::PARTITION]->errorCode
            );
            self::assertSame(
                count(self::OUT_OF_ORDER_STAMPS),
                $served->topics[$topic]->partitions[self::PARTITION]->offset
            );
        } finally {
            $stream->disconnect();
        }

        // The same branch of the broker refuses every negative target time it does not know at all: the map of
        // `KafkaApis.handleListOffsetRequestV1AndAbove` @ 3.9.2 ends at the -5 of KIP-1005, which the version 9
        // this client sends does carry, so -6 is the first value no version of this api can ask for
        try {
            $this->client($topic)->fetchTopicPartitionOffsetsForTimes([$topic => [self::PARTITION => -6]]);
            self::fail('A target time this cluster does not know is expected to fail');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(
                UnsupportedVersionException::class,
                $exception->getExceptions()[$topic][self::PARTITION]
            );
        }
    }

    public function testTheLocalLogStartOffsetOfKip405IsTheStartOfTheLogWithoutTieredStorage(): void
    {
        // KIP-405 (Kafka 3.5), the target time -4: "where does the part of this partition that is still on the
        // broker's own disk begin?". The node runs without remote storage, so `UnifiedLog.localLogStartOffset`
        // @ 3.9.2 is the log start offset itself and the answer is the one of -2
        $topic = $this->preparedTopic('max-timestamp');

        $local = $this->lookUp($topic, OffsetsRequest::EARLIEST_LOCAL_TIMESTAMP);

        self::assertInstanceOf(OffsetAndTimestamp::class, $local);
        self::assertSame(0, $local->offset, 'nothing of this log was ever moved anywhere');
        self::assertSame(
            OffsetsResponsePartition::UNKNOWN_TIMESTAMP,
            $local->timestamp,
            'like -1 and -2, the lookup reads no record and answers the timestamp -1'
        );
        self::assertSame(0, $local->leaderEpoch, 'the leader epoch of KIP-320 is answered as everywhere else');
        self::assertEquals(
            $this->lookUp($topic, OffsetsRequest::EARLIEST),
            $local,
            'without tiered storage the earliest offset and the earliest LOCAL offset are one and the same'
        );

        $configuration = $this->configuration();
        $admin         = new AdminClient(Cluster::bootstrap($configuration, $topic), $configuration);

        self::assertSame(
            [$topic => [self::PARTITION => 0]],
            $admin->listEarliestLocalOffsets([$topic => [self::PARTITION]]),
            'AdminClient::listEarliestLocalOffsets() is the OffsetSpec.earliestLocal() of the Java admin client'
        );
        self::assertSame(
            $admin->listOffsets([$topic => [self::PARTITION]], OffsetsRequest::EARLIEST),
            $admin->listEarliestLocalOffsets([$topic => [self::PARTITION]])
        );
    }

    public function testAnEmptyPartitionAnswersTheLocalLogStartOffsetZeroAndNotMinusOne(): void
    {
        // An empty log starts where it ends, and its local log start offset is that same 0 - which is the
        // difference to the max timestamp -3, that has nothing to report on an empty partition
        $topic = $this->preparedTopic('empty');
        $local = $this->lookUp($topic, OffsetsRequest::EARLIEST_LOCAL_TIMESTAMP);

        self::assertInstanceOf(OffsetAndTimestamp::class, $local);
        self::assertSame(0, $local->offset);
        self::assertSame(OffsetsResponsePartition::UNKNOWN_TIMESTAMP, $local->timestamp);
        self::assertNull(
            $this->lookUp($topic, OffsetsRequest::MAX_TIMESTAMP),
            'the max timestamp of an empty log is the -1 / -1 this client hands out as null'
        );
    }

    public function testAVersionBelowEightIsRefusedTheLocalLogStartOffsetPerPartition(): void
    {
        // `KafkaApis.handleListOffsetRequestV1AndAbove` @ 3.9.2 demands version 8 for the target time -4 and
        // answers the partition of a lower version with 35 - without closing the connection
        $topic  = $this->preparedTopic('max-timestamp');
        $stream = $this->connect();

        try {
            new OffsetsRequestV7(
                [$topic => [self::PARTITION => OffsetsRequest::EARLIEST_LOCAL_TIMESTAMP]],
                OffsetsRequest::CONSUMER_REPLICA_ID,
                FetchRequest::READ_UNCOMMITTED,
                self::CLIENT_ID,
                4051
            )->writeTo($stream);
            $refusal = OffsetsResponseV7::unpack($stream);

            self::assertSame(4051, $refusal->getCorrelationId());
            $partition = $refusal->topics[$topic]->partitions[self::PARTITION];
            self::assertSame(KafkaException::UNSUPPORTED_VERSION, $partition->errorCode);
            self::assertSame(OffsetsResponsePartition::UNKNOWN_OFFSET, $partition->offset);
            self::assertSame(OffsetsResponsePartition::UNKNOWN_TIMESTAMP, $partition->timestamp);

            // The connection is untouched, and the very same version answers the max timestamp of KIP-734
            new OffsetsRequestV7(
                [$topic => [self::PARTITION => OffsetsRequest::MAX_TIMESTAMP]],
                OffsetsRequest::CONSUMER_REPLICA_ID,
                FetchRequest::READ_UNCOMMITTED,
                self::CLIENT_ID,
                4052
            )->writeTo($stream);
            $served = OffsetsResponseV7::unpack($stream);

            self::assertSame(4052, $served->getCorrelationId());
            self::assertSame(
                KafkaException::NO_ERROR,
                $served->topics[$topic]->partitions[self::PARTITION]->errorCode
            );
            self::assertSame(1, $served->topics[$topic]->partitions[self::PARTITION]->offset);
        } finally {
            $stream->disconnect();
        }

        // And the frame of the version 8 is the frame of the version 7, byte for byte behind the api version
        $arguments = [
            [$topic => [self::PARTITION => OffsetsRequest::LATEST]],
            OffsetsRequest::CONSUMER_REPLICA_ID,
            FetchRequest::READ_UNCOMMITTED,
            self::CLIENT_ID,
            4053,
        ];
        $eight = bin2hex((string) new OffsetsRequestV8(...$arguments));
        $seven = bin2hex((string) new OffsetsRequestV7(...$arguments));

        self::assertSame($seven, substr_replace($eight, '0007', 12, 4));
        self::assertSame(7, OffsetsRequestV7::VERSION);
        self::assertSame(7, OffsetsResponseV7::VERSION);
        self::assertSame(8, OffsetsRequestV8::VERSION);
        self::assertSame(-4, OffsetsRequest::EARLIEST_LOCAL_TIMESTAMP);
    }

    public function testTheLastTieredOffsetOfKip1005IsMinusOneWithoutRemoteStorage(): void
    {
        // KIP-1005 (Kafka 3.9), the target time -5: "which offset does the part of this partition that has been
        // moved to remote storage end at?". `UnifiedLog.fetchOffsetByTimestamp` @ 3.9.2 answers
        // `highestOffsetInRemoteStorage()` only inside an `if (remoteLogEnabled())` and writes the literal
        // TimestampAndOffset(NO_TIMESTAMP, -1L, Optional.of(-1)) when it is off, which is how this node runs
        $topic         = $this->preparedTopic('max-timestamp');
        $configuration = $this->configuration();
        $admin         = new AdminClient(Cluster::bootstrap($configuration, $topic), $configuration);

        self::assertSame(
            [$topic => [self::PARTITION => OffsetsResponsePartition::UNKNOWN_OFFSET]],
            $admin->listLatestTieredOffsets([$topic => [self::PARTITION]]),
            'AdminClient::listLatestTieredOffsets() is the OffsetSpec.latestTiered() of the Java admin client'
        );
        self::assertSame(
            [$topic => [self::PARTITION => 0]],
            $admin->listEarliestLocalOffsets([$topic => [self::PARTITION]]),
            'the -4 of the same partition is the offset 0: the two ends of the tiered range are not the same'
        );
        self::assertNull(
            $this->lookUp($topic, OffsetsRequest::LATEST_TIERED_TIMESTAMP),
            'the -1 / -1 of "nothing is tiered" is what this client hands a timestamp lookup out as null'
        );

        // The error code is 0: the question is valid and there is nothing to report, which is why the offset -1
        // of a log that HAS records has to be read as "nothing of it is tiered" and never as a failure
        $stream = $this->connect();

        try {
            new OffsetsRequest(
                [$topic => [self::PARTITION => OffsetsRequest::LATEST_TIERED_TIMESTAMP]],
                OffsetsRequest::CONSUMER_REPLICA_ID,
                FetchRequest::READ_UNCOMMITTED,
                self::CLIENT_ID,
                4061
            )->writeTo($stream);
            $answer    = OffsetsResponse::unpack($stream);
            $partition = $answer->topics[$topic]->partitions[self::PARTITION];

            self::assertSame(4061, $answer->getCorrelationId());
            self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
            self::assertSame(OffsetsResponsePartition::UNKNOWN_OFFSET, $partition->offset);
            self::assertSame(OffsetsResponsePartition::UNKNOWN_TIMESTAMP, $partition->timestamp);
            self::assertSame(-1, $partition->leaderEpoch, 'and the leader epoch of a -1 offset is -1 as well');
        } finally {
            $stream->disconnect();
        }
    }

    public function testAnEmptyPartitionAnswersTheSameLastTieredOffsetAsAFilledOne(): void
    {
        // Unlike -4, which answers the 0 of a log that starts where it ends, -5 does not depend on the log at all
        // on a broker without tiered storage
        $topic         = $this->preparedTopic('empty');
        $configuration = $this->configuration();
        $admin         = new AdminClient(Cluster::bootstrap($configuration, $topic), $configuration);

        self::assertSame(
            [$topic => [self::PARTITION => OffsetsResponsePartition::UNKNOWN_OFFSET]],
            $admin->listLatestTieredOffsets([$topic => [self::PARTITION]])
        );
        self::assertSame(
            [$topic => [self::PARTITION => 0]],
            $admin->listEarliestLocalOffsets([$topic => [self::PARTITION]]),
            'where the local log start offset of an empty log is its 0'
        );
    }

    public function testAVersionBelowNineIsRefusedTheLastTieredOffsetPerPartition(): void
    {
        // `KafkaApis.handleListOffsetRequestV1AndAbove` @ 3.9.2 demands version 9 for the target time -5 and
        // answers the partition of a lower version with 35 - without closing the connection
        $topic  = $this->preparedTopic('max-timestamp');
        $stream = $this->connect();

        try {
            new OffsetsRequestV8(
                [$topic => [self::PARTITION => OffsetsRequest::LATEST_TIERED_TIMESTAMP]],
                OffsetsRequest::CONSUMER_REPLICA_ID,
                FetchRequest::READ_UNCOMMITTED,
                self::CLIENT_ID,
                4071
            )->writeTo($stream);
            $refusal = OffsetsResponseV8::unpack($stream);

            self::assertSame(4071, $refusal->getCorrelationId());
            $partition = $refusal->topics[$topic]->partitions[self::PARTITION];
            self::assertSame(KafkaException::UNSUPPORTED_VERSION, $partition->errorCode);
            self::assertSame(OffsetsResponsePartition::UNKNOWN_OFFSET, $partition->offset);
            self::assertSame(OffsetsResponsePartition::UNKNOWN_TIMESTAMP, $partition->timestamp);

            // The connection is untouched, and the very same version answers the local log start offset of KIP-405
            new OffsetsRequestV8(
                [$topic => [self::PARTITION => OffsetsRequest::EARLIEST_LOCAL_TIMESTAMP]],
                OffsetsRequest::CONSUMER_REPLICA_ID,
                FetchRequest::READ_UNCOMMITTED,
                self::CLIENT_ID,
                4072
            )->writeTo($stream);
            $served = OffsetsResponseV8::unpack($stream);

            self::assertSame(4072, $served->getCorrelationId());
            self::assertSame(
                KafkaException::NO_ERROR,
                $served->topics[$topic]->partitions[self::PARTITION]->errorCode
            );
            self::assertSame(0, $served->topics[$topic]->partitions[self::PARTITION]->offset);
        } finally {
            $stream->disconnect();
        }

        // And the frame of the version 9 is the frame of the version 8, byte for byte behind the api version
        $arguments = [
            [$topic => [self::PARTITION => OffsetsRequest::LATEST]],
            OffsetsRequest::CONSUMER_REPLICA_ID,
            FetchRequest::READ_UNCOMMITTED,
            self::CLIENT_ID,
            4073,
        ];
        $nine  = bin2hex((string) new OffsetsRequestV9(...$arguments));
        $eight = bin2hex((string) new OffsetsRequestV8(...$arguments));

        self::assertSame($eight, substr_replace($nine, '0008', 12, 4));
        self::assertSame(8, OffsetsRequestV8::VERSION);
        self::assertSame(8, OffsetsResponseV8::VERSION);
        self::assertSame(9, OffsetsRequestV9::VERSION);
        self::assertSame(10, OffsetsRequest::VERSION, 'the version Kafka 4.0 added (KIP-1075)');
        self::assertSame(-5, OffsetsRequest::LATEST_TIERED_TIMESTAMP);
    }


    /**
     * Looks one target time up in the partition under test and returns what the broker found
     */
    private static function firstTimestamp(): int
    {
        return self::$firstTimestamp ??= (int) (floor(microtime(true)) * 1000) - 3600000;
    }

    private function lookUp(string $topic, int $timestamp): ?OffsetAndTimestamp
    {
        return $this->client($topic)
            ->fetchTopicPartitionOffsetsForTimes([$topic => [self::PARTITION => $timestamp]])[$topic][self::PARTITION];
    }

    /**
     * Returns the name of a topic of this test run, creating it and filling it on first use
     */
    private function preparedTopic(string $key): string
    {
        if (isset(self::$topics[$key])) {
            return self::$topics[$key];
        }

        $topic   = self::uniqueTopicName('t5-timestamps-' . $key);
        $configs = match ($key) {
            'log-append-time' => ['message.timestamp.type' => 'LogAppendTime'],
            default           => [],
        };

        $this->createTopic($topic, $configs);
        if ($key === 'max-timestamp') {
            $this->produceOutOfOrderRecords($topic);
        } elseif ($key !== 'empty') {
            $this->produceTimestampedRecords($topic, $key === 'gzip' ? 'gzip' : ProducerConfig::COMPRESSION_TYPE_NONE);
        }

        return self::$topics[$key] = $topic;
    }

    /**
     * Creates a topic with one partition and the given topic-level options and waits for its leader
     *
     * The cluster of the admin client is bootstrapped on a probe topic of this test class, never on the topic that
     * is about to be created: `auto.create.topics.enable` is on, so a Metadata request would create it first, with
     * the defaults of the broker instead of the options this method asks for.
     *
     * @param array<string, string> $configs Topic-level options of the new topic
     */
    private function createTopic(string $topic, array $configs): void
    {
        $configuration = $this->configuration();
        $admin         = new AdminClient(
            Cluster::bootstrap($configuration, self::$topics['probe'] ??= self::uniqueTopicName('t5-timestamps-probe')),
            $configuration
        );

        $error = $admin->createTopics([new NewTopic($topic, 1, 1, [], $configs)])[$topic];
        self::assertNull($error, "The topic {$topic} could not be created");

        // A KRaft node names the leader of a fresh partition in its metadata before the replica manager serves it
        // (the 6 NotLeaderForPartition of a produce right after the creation, seen on a slow CI runner): wait until
        // the leader answers a ListOffsets of the partition with the code 0
        new TopicMetadataProbe(fn(): Stream => $this->connect(), self::TOPIC_TIMEOUT, 't5-timestamps')
            ->awaitTopicWithLeaders($topic);
    }

    /**
     * Appends the three records of {@see self::OUT_OF_ORDER_STAMPS} to the partition under test
     *
     * They travel in one batch, so their `CreateTime` values reach the log exactly as they are and the largest one
     * ends up at the offset 1.
     */
    private function produceOutOfOrderRecords(string $topic): void
    {
        $records = [];
        foreach (self::OUT_OF_ORDER_STAMPS as $index => $stamp) {
            $records[] = new Record(
                'value-' . $index,
                null,
                CompressionCodec::NONE,
                null,
                self::firstTimestamp() + $stamp
            );
        }

        $result = $this->client($topic)->produce([$topic => [self::PARTITION => $records]]);

        self::assertSame(
            KafkaException::NO_ERROR,
            $result[$topic][self::PARTITION]->errorCode,
            "The records of {$topic} were not accepted by the broker"
        );
    }

    /**
     * Appends the five records of this test class, one second apart, to the partition under test
     */
    private function produceTimestampedRecords(string $topic, string $compressionType): void
    {
        $records = [];
        for ($index = 0; $index < self::RECORD_COUNT; $index++) {
            $records[] = new Record(
                'value-' . $index,
                null,
                CompressionCodec::NONE,
                null,
                self::firstTimestamp() + $index * 1000
            );
        }

        $result = $this->client($topic, [ProducerConfig::COMPRESSION_TYPE => $compressionType])
            ->produce([$topic => [self::PARTITION => $records]]);

        self::assertSame(
            KafkaException::NO_ERROR,
            $result[$topic][self::PARTITION]->errorCode,
            "The records of {$topic} were not accepted by the broker"
        );
    }

    /**
     * Builds a low-level client whose cluster metadata only covers the topic under test
     *
     * @param array<string, mixed> $overrides Options that override the defaults of this test class
     */
    private function client(string $topic, array $overrides = []): Client
    {
        $configuration = $this->configuration($overrides);

        return new Client(Cluster::bootstrap($configuration, $topic), $configuration);
    }

    /**
     * @param array<string, mixed> $overrides Options that override the defaults of this test class
     *
     * @return array<string, mixed>
     */
    private function configuration(array $overrides = []): array
    {
        return $overrides + [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ProducerConfig::ACKS                    => 1,
            ProducerConfig::TIMEOUT_MS              => 5000,
        ] + ProducerConfig::getDefaultConfiguration();
    }
}
