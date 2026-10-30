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
use Protocol\Kafka\Admin\AlterConfigOp;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\Consumer\AcknowledgeType;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\KafkaShareConsumer;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsResponseResource;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequestV12;
use Protocol\Kafka\Protocol\Request\ProduceResponseV12;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * The dead-letter queue of share groups (KIP-1191, Kafka 4.4, `share.version` 2) against the 4.4.0 node.
 *
 * There is no new api and no new field: a share group gets a DLQ through its group config
 * `errors.deadletterqueue.topic.name` (IncrementalAlterConfigs on the config resource type 32), the DLQ topic has to
 * carry the topic config `errors.deadletterqueue.group.enable=true` and a name that starts with the cluster's
 * `errors.deadletterqueue.topic.name.prefix` (`dlq.`), and the node writes a record into it for every record the
 * share consumer REJECTs and for every record that reaches `share.delivery.count.limit`. Every test has a source topic,
 * a DLQ topic and a share group of its own; the cluster-wide `errors.deadletterqueue.auto.create.topics.enable` is
 * never touched (it is `false` on the node), so every DLQ topic is created by the test.
 *
 * @see docs/protocol/4.4.md, section "The dead-letter queue of KIP-1191 (share.version 2)"
 */
#[CoversClass(KafkaShareConsumer::class)]
final class ShareGroupDeadLetterQueueTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-44-dlq';

    private const string TOPIC_PREFIX = 't3-44-dlq';

    /**
     * The group config resource of KIP-848 and KIP-932 (`ConfigResource.Type.GROUP`, id 32)
     */
    private const int GROUP_CONFIG_RESOURCE = 32;

    /**
     * The headers of a DLQ record, `ShareGroupDLQStateManager.ProduceRequestHandler` @ 4.4.0, in the order it writes
     */
    private const array DLQ_HEADERS = [
        '__dlq.errors.topic',
        '__dlq.errors.partition',
        '__dlq.errors.offset',
        '__dlq.errors.group',
        '__dlq.errors.delivery.count',
        '__dlq.errors.message',
    ];

    private static ?Cluster $sharedCluster = null;

    /**
     * Every group this class created
     *
     * @var list<string>
     */
    private static array $groups = [];

    /**
     * Consumers of the running test, closed in tearDown
     *
     * @var list<KafkaShareConsumer|KafkaConsumer>
     */
    private array $consumers = [];

    protected function tearDown(): void
    {
        foreach ($this->consumers as $consumer) {
            try {
                $consumer->close();
            } catch (\Throwable) {
                // A consumer a test closed already must not fail the suite
            }
        }
        $this->consumers = [];

        parent::tearDown();
    }

    public static function tearDownAfterClass(): void
    {
        if (self::$groups !== []) {
            try {
                $configuration = self::baseConfiguration() + ConsumerConfig::getDefaultConfiguration();
                new AdminClient(Cluster::bootstrap($configuration), $configuration)->deleteConsumerGroups(self::$groups);
            } catch (KafkaException) {
                // A group that is gone already must not fail the suite
            }
        }
        self::$groups = [];

        parent::tearDownAfterClass();
    }

    /**
     * Without `errors.deadletterqueue.copy.record.enable` a DLQ record is the metadata of the source record alone: no
     * key, no value, the six `__dlq.errors.*` headers and the time the node wrote it
     */
    public function testARejectedAndAnExhaustedRecordLandInTheDeadLetterQueueAsTheirMetadata(): void
    {
        [$source, $dlq, $groupId] = $this->deadLetterQueue(copyRecords: false);
        $before                   = (int) (microtime(true) * 1000);

        $this->rejectTheFirstAndExhaustTheSecond($source, $groupId);
        $records = $this->readDeadLetterQueue($dlq, 2);

        self::assertSame([0, 1], array_map(static fn(Record $record): ?int => $record->offset, $records));
        foreach ($records as $record) {
            self::assertNull($record->key, 'the key of the source record is not copied by default');
            self::assertNull($record->value, 'neither is its value');
            self::assertSame(TimestampType::CREATE_TIME, $record->timestampType);
            self::assertGreaterThanOrEqual(
                $before,
                $record->timestamp,
                'the timestamp is the moment the node wrote the DLQ record, not the one of the source record'
            );
        }
        self::assertSame(
            self::headers($source, 0, 0, $groupId, 1, 'Offset rejected by client.'),
            self::headersOf($records[0]),
            'a REJECT: the delivery count it was rejected at and the CLIENT_REJECT cause of ShareGroupDLQManager'
        );
        self::assertSame(
            self::headers($source, 0, 1, $groupId, 2, 'Offset delivery count exceeded the threshold.'),
            self::headersOf($records[1]),
            'a RELEASE at share.delivery.count.limit: the DELIVERY_COUNT_EXCEEDED cause'
        );
    }

    /**
     * `errors.deadletterqueue.copy.record.enable=true` copies the key and the value of the source record - and nothing
     * else: its headers and its timestamp are not carried over
     */
    public function testTheCopyRecordConfigCopiesTheKeyAndTheValueButNotTheHeaders(): void
    {
        [$source, $dlq, $groupId] = $this->deadLetterQueue(copyRecords: true);
        $before                   = (int) (microtime(true) * 1000);

        $this->rejectTheFirstAndExhaustTheSecond($source, $groupId);
        $records = $this->readDeadLetterQueue($dlq, 2);

        self::assertSame(
            [['key-0', 'value-0'], ['key-1', 'value-1']],
            array_map(static fn(Record $record): array => [$record->key, $record->value], $records)
        );
        self::assertSame(
            self::headers($source, 0, 0, $groupId, 1, 'Offset rejected by client.'),
            self::headersOf($records[0]),
            'the header `origin` of the source record is gone: the DLQ record carries its own six alone'
        );
        self::assertSame(self::DLQ_HEADERS, array_column(self::headersOf($records[1]), 0));
        self::assertGreaterThanOrEqual($before, $records[0]->timestamp, 'not the 1700000000000 of the source record');
        self::assertSame(TimestampType::CREATE_TIME, $records[0]->timestampType);
    }

    /**
     * A DLQ the node cannot write to is no error the client ever sees: the record is archived all the same, and the
     * broker logs "Failed to write to DLQ, proceeding to ARCHIVED regardless." with the ConfigException of the check
     * that failed
     */
    public function testAMisconfiguredDeadLetterQueueArchivesTheRecordsWithoutWritingThem(): void
    {
        foreach (['no-prefix', 'not-enabled', 'missing'] as $case) {
            [$source, $dlq, $groupId] = $this->deadLetterQueue(copyRecords: false, misconfiguration: $case);

            $this->rejectTheFirstAndExhaustTheSecond($source, $groupId);

            $other = $this->shareConsumer($groupId);
            $other->subscribe([$source]);
            self::assertSame([], $other->poll(3000), "{$case}: both records are archived, never delivered again");
            $other->close();

            $topics = $this->admin()->listTopics();
            if ($case === 'missing') {
                self::assertNotContains($dlq, $topics, 'auto-create is off: the node does not create the DLQ topic');
            } else {
                self::assertSame(
                    [$dlq => [0 => 0]],
                    $this->reader()->endOffsets([$dlq => [0]]),
                    "{$case}: nothing was written into the topic"
                );
            }
        }
    }

    /**
     * The one name IncrementalAlterConfigs refuses is the one of an internal topic; a name without the prefix, or of a
     * topic that does not exist, is accepted and only fails when the node writes the first DLQ record
     */
    public function testTheGroupConfigRefusesTheNameOfAnInternalTopic(): void
    {
        $groupId        = self::TOPIC_PREFIX . '-group-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;

        $answer = $this->setGroupConfig($groupId, 'errors.deadletterqueue.topic.name', '__dlq-internal');

        self::assertSame(KafkaException::INVALID_CONFIG, $answer->errorCode);
        self::assertSame(
            "errors.deadletterqueue.topic.name: DLQ topic name must not start with '__'",
            $answer->errorMessage
        );
        self::assertSame(
            KafkaException::NO_ERROR,
            $this->setGroupConfig($groupId, 'errors.deadletterqueue.topic.name', 'no-prefix-' . $groupId)->errorCode,
            'GroupConfig.validate @ 4.4.0 knows nothing of the prefix'
        );
        self::assertSame(
            KafkaException::NO_ERROR,
            $this->setGroupConfig($groupId, 'errors.deadletterqueue.copy.record.enable', 'true')->errorCode
        );
    }

    /**
     * Creates a source topic with two records, a DLQ topic and a share group that reads the source from `earliest`,
     * archives a record at its second delivery and writes its dead letters into the DLQ topic
     *
     * @param string|null $misconfiguration `no-prefix` names a DLQ topic without the `dlq.` prefix, `not-enabled` one
     *                                      without `errors.deadletterqueue.group.enable`, `missing` one that does not
     *                                      exist
     *
     * @return array{string, string, string} Source topic, DLQ topic, group id
     */
    private function deadLetterQueue(bool $copyRecords, ?string $misconfiguration = null): array
    {
        $source = self::uniqueTopicName(self::TOPIC_PREFIX . '-src');
        $dlq    = self::uniqueTopicName(($misconfiguration === 'no-prefix' ? '' : 'dlq.') . self::TOPIC_PREFIX);
        $topics = [new NewTopic($source, 1, 1)];
        if ($misconfiguration !== 'missing') {
            $configs  = $misconfiguration === 'not-enabled' ? [] : ['errors.deadletterqueue.group.enable' => 'true'];
            $topics[] = new NewTopic($dlq, 1, 1, configs: $configs);
        }
        $this->admin()->createTopics($topics);
        $probe = new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::TOPIC_PREFIX);
        $probe->awaitTopicWithLeaders($source);
        if ($misconfiguration !== 'missing') {
            $probe->awaitTopicWithLeaders($dlq);
        }

        $this->produce($source, [
            new Record('value-0', 'key-0', 0, null, 1700000000000, TimestampType::CREATE_TIME, [new Header('origin', 'h-0')]),
            new Record('value-1', 'key-1', 0, null, 1700000000001, TimestampType::CREATE_TIME, [new Header('origin', 'h-1')]),
        ]);

        $groupId        = self::TOPIC_PREFIX . '-group-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;
        $configs        = [
            'share.auto.offset.reset'           => 'earliest',
            // 2 is the lowest limit the node accepts
            'share.delivery.count.limit'        => '2',
            'errors.deadletterqueue.topic.name' => $dlq,
        ];
        if ($copyRecords) {
            $configs['errors.deadletterqueue.copy.record.enable'] = 'true';
        }
        foreach ($configs as $name => $value) {
            self::assertSame(
                KafkaException::NO_ERROR,
                $this->setGroupConfig($groupId, $name, $value)->errorCode,
                "{$name} of {$groupId}"
            );
        }

        return [$source, $dlq, $groupId];
    }

    /**
     * REJECTs the record at the offset 0 and RELEASEs the one at the offset 1 twice, which reaches the limit 2
     */
    private function rejectTheFirstAndExhaustTheSecond(string $source, string $groupId): void
    {
        $consumer = $this->shareConsumer($groupId);
        $consumer->subscribe([$source]);

        [$first, $second] = $this->pollUntil($consumer, 2);
        $consumer->acknowledge($first, AcknowledgeType::REJECT);
        $consumer->acknowledge($second, AcknowledgeType::RELEASE);
        $consumer->commitSync();

        [$again] = $this->pollUntil($consumer, 1);
        self::assertSame([1, 2], [$again->offset, $again->deliveryCount]);
        $consumer->acknowledge($again, AcknowledgeType::RELEASE);
        $consumer->commitSync();
        $consumer->close();
    }

    /**
     * Reads the partition 0 of the DLQ topic from its beginning until the given number of records arrived
     *
     * The node writes a DLQ record after it answered the acknowledgement, from a sender thread of its own, so the
     * records are polled for until they are there. A consumer without a deserializer returns plain records.
     *
     * @return list<Record>
     */
    private function readDeadLetterQueue(string $dlq, int $count): array
    {
        $reader = $this->reader();
        $reader->assign([$dlq => [0]]);
        $reader->seekToBeginning([$dlq => [0]]);

        $records  = [];
        $deadline = microtime(true) + 15.0;
        while (count($records) < $count && microtime(true) < $deadline) {
            foreach ($reader->poll(1000) as $partitions) {
                foreach ($partitions as $partitionRecords) {
                    array_push($records, ...$partitionRecords);
                }
            }
        }
        self::assertCount($count, $records, "records of the DLQ topic {$dlq}");

        return $records;
    }

    private function shareConsumer(string $groupId): KafkaShareConsumer
    {
        $consumer = new KafkaShareConsumer([
            ConsumerConfig::GROUP_ID                   => $groupId,
            ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_EXPLICIT,
        ] + self::baseConfiguration());
        $this->consumers[] = $consumer;

        return $consumer;
    }

    private function reader(): KafkaConsumer
    {
        $reader = new KafkaConsumer(self::baseConfiguration() + [
            ConsumerConfig::GROUP_ID           => self::TOPIC_PREFIX . '-reader-' . bin2hex(random_bytes(6)),
            ConsumerConfig::ENABLE_AUTO_COMMIT => false,
        ]);
        $this->consumers[] = $reader;

        return $reader;
    }

    /**
     * Polls until the given number of records came back in one poll()
     *
     * @return list<ConsumerRecord>
     */
    private function pollUntil(KafkaShareConsumer $consumer, int $count): array
    {
        $records  = [];
        $deadline = microtime(true) + 30.0;
        while (count($records) < $count && microtime(true) < $deadline) {
            foreach ($consumer->poll(5000) as $partitions) {
                foreach ($partitions as $partitionRecords) {
                    array_push($records, ...$partitionRecords);
                }
            }
        }
        self::assertCount($count, $records, 'records of the share consumer');

        return $records;
    }

    /**
     * @param list<Record> $records
     */
    private function produce(string $topic, array $records): void
    {
        $stream = $this->connect();
        new ProduceRequestV12([$topic => [0 => RecordBatch::fromRecords($records)]], 1, 10000, self::CLIENT_ID)
            ->writeTo($stream);

        self::assertSame(
            KafkaException::NO_ERROR,
            ProduceResponseV12::unpack($stream)->topics[$topic]->partitions[0]->errorCode
        );
    }

    /**
     * Sets a group config through IncrementalAlterConfigs and the config resource type 32, and returns the answer
     */
    private function setGroupConfig(string $groupId, string $name, string $value): IncrementalAlterConfigsResponseResource
    {
        $stream = $this->connect();
        new IncrementalAlterConfigsRequest(
            [new IncrementalAlterConfigsRequestResource(self::GROUP_CONFIG_RESOURCE, $groupId, [AlterConfigOp::set($name, $value)])],
            false,
            self::CLIENT_ID
        )->writeTo($stream);

        $answer = IncrementalAlterConfigsResponse::unpack($stream)->responses[0] ?? null;
        self::assertNotNull($answer);

        return $answer;
    }

    /**
     * The six headers of a DLQ record, as `[name, value]` pairs
     *
     * @return list<array{string, string}>
     */
    private static function headers(
        string $topic,
        int $partition,
        int $offset,
        string $groupId,
        int $deliveryCount,
        string $message
    ): array {
        return array_map(
            null,
            self::DLQ_HEADERS,
            [$topic, (string) $partition, (string) $offset, $groupId, (string) $deliveryCount, $message]
        );
    }

    /**
     * @return list<array{string, string|null}>
     */
    private static function headersOf(Record $record): array
    {
        return array_map(static fn(Header $header): array => [$header->key, $header->value], $record->headers);
    }

    private function admin(): AdminClient
    {
        $configuration = self::baseConfiguration() + ConsumerConfig::getDefaultConfiguration();

        return new AdminClient(self::$sharedCluster ??= Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * @return array<string, mixed>
     */
    private static function baseConfiguration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
        ];
    }
}
