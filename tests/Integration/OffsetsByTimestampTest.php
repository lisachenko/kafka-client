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
use Protocol\Kafka\Common\Errors\UnsupportedForMessageFormatException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetAndTimestamp;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * Verifies version 1 of the Offsets (ListOffset) API - the timestamp lookup of Kafka 0.10.1 - against a real
 * 0.10.2.2 broker.
 *
 * Every partition under test holds five records one second apart, with the `CreateTime` values 1600000000000 to
 * 1600000004000, so that the answer of the broker is fully determined by the timestamp that is searched for. What
 * the tests assert is the table of "What a 0.10.2.2 broker answers" in the protocol document.
 *
 * @see docs/protocol/0.11.0.md, section "Offsets API (key 2, v0 and v1), a.k.a. ListOffset"
 */
#[CoversClass(Client::class)]
#[CoversClass(AdminClient::class)]
#[CoversClass(KafkaConsumer::class)]
#[CoversClass(OffsetAndTimestamp::class)]
#[CoversClass(OffsetsRequest::class)]
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
     * `CreateTime` of the first record of a prepared topic; the following ones are one second apart
     */
    private const int FIRST_TIMESTAMP = 1600000000000;

    /**
     * Number of records that a prepared topic holds
     */
    private const int RECORD_COUNT = 5;

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
        $found = $this->lookUp($this->preparedTopic('plain'), self::FIRST_TIMESTAMP - 5000);

        self::assertInstanceOf(OffsetAndTimestamp::class, $found);
        self::assertSame(0, $found->offset);
        self::assertSame(self::FIRST_TIMESTAMP, $found->timestamp);
    }

    public function testATimestampBetweenTwoRecordsFindsTheLaterOne(): void
    {
        $found = $this->lookUp($this->preparedTopic('plain'), self::FIRST_TIMESTAMP + 1500);

        self::assertInstanceOf(OffsetAndTimestamp::class, $found);
        self::assertSame(2, $found->offset, 'the first record whose own timestamp is at or after the target time');
        self::assertSame(self::FIRST_TIMESTAMP + 2000, $found->timestamp);
    }

    public function testTheTimestampOfARecordFindsThatVeryRecord(): void
    {
        $found = $this->lookUp($this->preparedTopic('plain'), self::FIRST_TIMESTAMP + 4000);

        self::assertInstanceOf(OffsetAndTimestamp::class, $found);
        self::assertSame(4, $found->offset);
        self::assertSame(self::FIRST_TIMESTAMP + 4000, $found->timestamp);
    }

    public function testATimestampAboveTheLastRecordIsAnsweredWithoutAnOffsetAndWithoutAnError(): void
    {
        $topic = $this->preparedTopic('plain');

        self::assertNull(
            $this->lookUp($topic, self::FIRST_TIMESTAMP + 9000),
            'the broker answers the error code 0 with the offset -1, which this client reports as null'
        );
        self::assertSame(
            [$topic => [self::PARTITION => OffsetsResponsePartition::UNKNOWN_OFFSET]],
            $this->client($topic)->fetchTopicPartitionOffsets(
                [$topic => [self::PARTITION => self::FIRST_TIMESTAMP + 9000]]
            ),
            'the plain offset of a partition that holds no such message is -1'
        );
    }

    public function testAnEmptyPartitionMatchesNoTimestampAtAll(): void
    {
        $topic = $this->preparedTopic('empty');

        self::assertNull($this->lookUp($topic, self::FIRST_TIMESTAMP));
        self::assertSame(0, $this->lookUp($topic, OffsetsRequest::LATEST)?->offset, 'an empty log ends at offset 0');
        self::assertSame(0, $this->lookUp($topic, OffsetsRequest::EARLIEST)?->offset);
    }

    public function testATopicWithoutMessageTimestampsRefusesATimestampLookupButKeepsTheSpecialTimes(): void
    {
        // message.format.version=0.9.0 makes the broker store the records in message format v0, which has no
        // timestamps at all, so there is nothing for the time index to search
        $topic = $this->preparedTopic('legacy-format');

        self::assertSame(self::RECORD_COUNT, $this->lookUp($topic, OffsetsRequest::LATEST)?->offset);
        self::assertSame(0, $this->lookUp($topic, OffsetsRequest::EARLIEST)?->offset);

        try {
            $this->lookUp($topic, self::FIRST_TIMESTAMP + 1500);
            self::fail('A timestamp lookup on a message format v0 topic is expected to fail');
        } catch (TopicPartitionRequestException $exception) {
            $error = $exception->getExceptions()[$topic][self::PARTITION];

            self::assertInstanceOf(UnsupportedForMessageFormatException::class, $error);
            self::assertSame(KafkaException::UNSUPPORTED_FOR_MESSAGE_FORMAT, $error->getCode());
        }
    }

    public function testACompressedBatchIsResolvedToTheInnerRecord(): void
    {
        // The whole batch is one wrapper message on disk, and the broker still answers the offset of the single
        // inner record, not the base offset of the wrapper
        $found = $this->lookUp($this->preparedTopic('gzip'), self::FIRST_TIMESTAMP + 1500);

        self::assertInstanceOf(OffsetAndTimestamp::class, $found);
        self::assertSame(2, $found->offset);
        self::assertSame(self::FIRST_TIMESTAMP + 2000, $found->timestamp);
    }

    public function testALogAppendTimeTopicIsSearchedByTheAppendTimeOfTheBroker(): void
    {
        $topic = $this->preparedTopic('log-append-time');

        // The CreateTime values the producer sent are replaced by the broker, and they lie far in the past, so a
        // lookup for them finds the very first record
        $found = $this->lookUp($topic, self::FIRST_TIMESTAMP + 1500);

        self::assertInstanceOf(OffsetAndTimestamp::class, $found);
        self::assertSame(0, $found->offset);
        self::assertGreaterThan(
            self::FIRST_TIMESTAMP,
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

            $found = $consumer->offsetsForTimes([$topic => [self::PARTITION => self::FIRST_TIMESTAMP + 2500]]);

            $offsetAndTimestamp = $found[$topic][self::PARTITION];
            self::assertInstanceOf(OffsetAndTimestamp::class, $offsetAndTimestamp);
            self::assertSame(3, $offsetAndTimestamp->offset);
            self::assertSame(self::FIRST_TIMESTAMP + 3000, $offsetAndTimestamp->timestamp);

            // Nothing was moved by the lookup, so a consumer that wants to read from there seeks itself
            $consumer->assign([$topic => [self::PARTITION]]);
            $consumer->seek($topic, self::PARTITION, $offsetAndTimestamp->offset);
            self::assertSame($offsetAndTimestamp->offset, $consumer->position($topic, self::PARTITION));
        } finally {
            $consumer->close();
        }
    }

    /**
     * Looks one target time up in the partition under test and returns what the broker found
     */
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
            'legacy-format'   => ['message.format.version' => '0.9.0'],
            'log-append-time' => ['message.timestamp.type' => 'LogAppendTime'],
            default           => [],
        };

        $this->createTopic($topic, $configs);
        if ($key !== 'empty') {
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

        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            $metadata = $admin->describeTopics([$topic])[$topic] ?? null;
            if ($metadata !== null
                && $metadata->topicErrorCode === KafkaException::NO_ERROR
                && $metadata->partitions !== []) {
                return;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The topic {$topic} did not get a leader in time");
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
                self::FIRST_TIMESTAMP + $index * 1000
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
