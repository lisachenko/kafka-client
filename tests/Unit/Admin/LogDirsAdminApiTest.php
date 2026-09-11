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

namespace Protocol\Kafka\Tests\Unit\Admin;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\LogDirInfo;
use Protocol\Kafka\Admin\ReplicaInfo;
use Protocol\Kafka\Admin\TopicPartitionReplica;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\BrokerNotAvailableException;
use Protocol\Kafka\Common\Errors\KafkaStorageException;
use Protocol\Kafka\Common\Errors\LogDirNotFoundException;
use Protocol\Kafka\Common\Errors\ReplicaNotAvailableException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Tests\Compliance\VectorFile;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * Tests the two JBOD apis of KIP-113 against scripted brokers: that each request goes to the broker it is about,
 * and how the answers are mapped onto the return values of {@see AdminClient}.
 *
 * The canned answers are the documented wire vectors of `docs/protocol/vectors` wherever one fits, so this suite
 * and the compliance suite cannot disagree about what a broker says.
 *
 * @see docs/protocol/2.8.md, sections "DescribeLogDirs API (key 35, v0 to v2)" and
 *      "AlterReplicaLogDirs API (key 34, v0 and v1)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(LogDirInfo::class)]
#[CoversClass(ReplicaInfo::class)]
#[CoversClass(TopicPartitionReplica::class)]
final class LogDirsAdminApiTest extends TestCase
{
    private const string TOPIC = 't5-vectors-logdirs';

    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    private const string FIRST_BROKER = 'tcp://kafka-1:9092';

    private const string SECOND_BROKER = 'tcp://kafka-2:9093';

    private const string FIRST_DIR = '/tmp/kafka-logs';

    private const string SECOND_DIR = '/tmp/kafka-logs-2';

    private ScriptedConnections $brokers;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testDescribeLogDirsAsksEveryNamedBrokerAndKeysTheResultByItsId(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(self::describeLogDirsResponse([
                [0, self::FIRST_DIR, []],
                [0, self::SECOND_DIR, [self::TOPIC => [[0, 8203970, 0, false]]]],
            ])))
            ->on(self::SECOND_BROKER, new BrokerConnection(self::describeLogDirsResponse([
                [0, self::FIRST_DIR, []],
                [0, self::SECOND_DIR, []],
            ])))
            ->install();

        $result = $this->adminClient()->describeLogDirs([0, 1], [self::TOPIC => [0]]);

        self::assertSame([0, 1], array_keys($result), 'the result is keyed by the broker id');
        self::assertSame([self::FIRST_DIR, self::SECOND_DIR], array_keys($result[0]));

        $replica = $result[0][self::SECOND_DIR]->replica(self::TOPIC, 0);
        self::assertInstanceOf(ReplicaInfo::class, $replica);
        self::assertSame(8203970, $replica->size);
        self::assertSame(0, $replica->offsetLag);
        self::assertFalse($replica->isFuture);
        self::assertNull($result[0][self::FIRST_DIR]->error, 'an online directory carries no error');
        self::assertSame([], $result[0][self::FIRST_DIR]->replicaInfos, 'and holds none of the requested replicas');

        self::assertSame([], $result[1][self::FIRST_DIR]->replicaInfos, 'the second broker was asked separately');
    }

    public function testDescribeLogDirsReportsAMovingReplicaInBothDirectories(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(self::describeLogDirsResponse([
                [0, self::FIRST_DIR, [self::TOPIC => [[0, 4096, 1000, true]]]],
                [0, self::SECOND_DIR, [self::TOPIC => [[0, 8203970, 0, false]]]],
            ])))
            ->install();

        $directories = $this->adminClient()->describeLogDirs([0], [self::TOPIC => [0]])[0];

        $future  = $directories[self::FIRST_DIR]->replica(self::TOPIC, 0);
        $current = $directories[self::SECOND_DIR]->replica(self::TOPIC, 0);

        self::assertNotNull($future);
        self::assertNotNull($current);
        self::assertTrue($future->isFuture, 'the destination of the running move');
        self::assertSame(1000, $future->offsetLag, 'the records the mover still has to copy');
        self::assertFalse($current->isFuture);
        self::assertSame(0, $current->offsetLag);
    }

    public function testDescribeLogDirsSendsANullTopicArrayWhenNoPartitionIsNamed(): void
    {
        $broker = new BrokerConnection(self::describeLogDirsResponse([
            [0, self::FIRST_DIR, []],
            [0, self::SECOND_DIR, []],
        ]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, $broker)
            ->install();

        $this->adminClient()->describeLogDirs([0]);

        // The compact null of the flexible version 2, then the tag buffer of the body: "every replica of every
        // log directory" is two bytes instead of the `ff ff ff ff` of the versions below
        self::assertStringEndsWith('0000', bin2hex($broker->getReceivedFrames()[0]));
    }

    public function testDescribeLogDirsSendsAnEmptyTopicArrayForAnEmptySelection(): void
    {
        $broker = new BrokerConnection(self::describeLogDirsResponse([
            [0, self::FIRST_DIR, []],
            [0, self::SECOND_DIR, []],
        ]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, $broker)
            ->install();

        $this->adminClient()->describeLogDirs([0], []);

        self::assertStringEndsWith(
            '0100',
            bin2hex($broker->getReceivedFrames()[0]),
            'the empty compact array, then the tag buffer of the body: no replica at all'
        );
    }

    public function testDescribeLogDirsAcceptsTopicPartitionObjects(): void
    {
        $broker = new BrokerConnection(self::describeLogDirsResponse([
            [0, self::FIRST_DIR, []],
            [0, self::SECOND_DIR, [self::TOPIC => [[0, 8203970, 0, false]]]],
        ]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, $broker)
            ->install();

        $this->adminClient()->describeLogDirs([0], [new TopicPartition(self::TOPIC, 0)]);

        self::assertStringContainsString(bin2hex(self::TOPIC), bin2hex($broker->getReceivedFrames()[0]));
    }

    public function testAnOfflineDirectoryCarriesItsErrorAndNoReplica(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(self::offlineDirectoryResponse()))
            ->install();

        $directories = $this->adminClient()->describeLogDirs([0])[0];

        self::assertInstanceOf(KafkaStorageException::class, $directories[self::FIRST_DIR]->error);
        self::assertSame([], $directories[self::FIRST_DIR]->replicaInfos);
    }

    public function testDescribeLogDirsRefusesABrokerIdTheClusterDoesNotHave(): void
    {
        // `nodeById()` reloads the metadata once before it gives up, so the bootstrap answers twice
        $this->brokers
            ->on(
                self::BOOTSTRAP_ADDRESS,
                new BrokerConnection($this->clusterMetadata()),
                new BrokerConnection($this->clusterMetadata())
            )
            ->install();

        $this->expectException(BrokerNotAvailableException::class);

        // Asking any other broker would answer about ITS disks, so there is no fallback to `sendAnyNode()`
        $this->adminClient()->describeLogDirs([7]);
    }

    public function testAlterReplicaLogDirsGoesToTheBrokerOfEachReplica(): void
    {
        $first  = new BrokerConnection(self::vector('alter-replica-log-dirs', 'alterreplicalogdirs.response.v0'));
        $second = new BrokerConnection(self::alterResponse([1 => 0]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, $first)
            ->on(self::SECOND_BROKER, $second)
            ->install();

        $result = $this->adminClient()->alterReplicaLogDirs([
            TopicPartitionReplica::of(self::TOPIC, 0, 0)->key() => self::FIRST_DIR,
            TopicPartitionReplica::of(self::TOPIC, 1, 1)->key() => self::SECOND_DIR,
        ]);

        self::assertSame(1, $first->getRequestCount(), 'one request per broker');
        self::assertSame(1, $second->getRequestCount());
        self::assertSame(
            [self::TOPIC . '-0-0' => null, self::TOPIC . '-1-1' => null],
            $result,
            'an accepted move is reported with null, like an altered config resource'
        );
    }

    public function testAReplicaTheBrokerRefusedCarriesItsExceptionWithoutThrowing(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(
                self::vector('alter-replica-log-dirs', 'alterreplicalogdirs.response.v0.unknown-log-dir')
            ))
            ->install();

        $result = $this->adminClient()->alterReplicaLogDirs([
            TopicPartitionReplica::of(self::TOPIC, 0, 0)->key() => '/tmp/kafka-logs-3',
        ]);

        self::assertInstanceOf(LogDirNotFoundException::class, $result[self::TOPIC . '-0-0']);
    }

    public function testAReplicaTheBrokerDoesNotHostIsReplicaNotAvailable(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(self::alterResponse([0 => 9])))
            ->install();

        $result = $this->adminClient()->alterReplicaLogDirs([
            TopicPartitionReplica::of(self::TOPIC, 0, 0)->key() => self::FIRST_DIR,
        ]);

        self::assertInstanceOf(ReplicaNotAvailableException::class, $result[self::TOPIC . '-0-0']);
    }

    public function testAReplicaTheBrokerDidNotAnswerBecomesAnUnknownError(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(self::alterResponse([0 => 0])))
            ->install();

        $result = $this->adminClient()->alterReplicaLogDirs([
            TopicPartitionReplica::of(self::TOPIC, 0, 0)->key() => self::FIRST_DIR,
            TopicPartitionReplica::of(self::TOPIC, 2, 0)->key() => self::FIRST_DIR,
        ]);

        self::assertNull($result[self::TOPIC . '-0-0']);
        self::assertInstanceOf(UnknownErrorException::class, $result[self::TOPIC . '-2-0']);
    }

    public function testTheRequestIsGroupedByDirectoryInsideOneBroker(): void
    {
        $broker = new BrokerConnection(self::alterResponse([0 => 0, 1 => 0]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, $broker)
            ->install();

        $this->adminClient()->alterReplicaLogDirs([
            TopicPartitionReplica::of(self::TOPIC, 0, 0)->key() => self::FIRST_DIR,
            TopicPartitionReplica::of(self::TOPIC, 1, 0)->key() => self::SECOND_DIR,
        ]);

        $frame = bin2hex($broker->getReceivedFrames()[0]);
        self::assertStringContainsString('00000002', $frame, 'the log_dirs array holds two entries');
        self::assertStringContainsString(bin2hex(self::SECOND_DIR), $frame);
    }

    public function testAReplicaKeyIsTheJavaToStringOfTopicPartitionReplica(): void
    {
        $replica = TopicPartitionReplica::of('events', 3, 7);

        self::assertSame('events-3-7', $replica->key());
        self::assertSame('events-3-7', (string) $replica);
        self::assertTrue($replica->equals(TopicPartitionReplica::fromKey('events-3-7')));
        self::assertSame('events-3', (string) $replica->topicPartition());
    }

    public function testAReplicaKeyOfATopicWithDashesRoundTrips(): void
    {
        // The last two fields are the partition and the broker id, so everything before them is the topic name
        $replica = TopicPartitionReplica::fromKey('t5-log-dirs-12-3');

        self::assertSame('t5-log-dirs', $replica->topic);
        self::assertSame(12, $replica->partition);
        self::assertSame(3, $replica->brokerId);
    }

    public function testAKeyThatNamesNoReplicaIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);

        TopicPartitionReplica::fromKey('events');
    }

    /**
     * Metadata of a two-broker cluster whose topic has one partition on each of them
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
     * Builds an admin client on the cluster that the scripted brokers answer for
     */
    private function adminClient(): AdminClient
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't5-logdirs',
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
        ];

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * An AlterReplicaLogDirs answer of one topic: partition id => error code
     *
     * @param array<int, int> $partitions
     */
    private static function alterResponse(array $partitions): string
    {
        $body = pack('N', 0) /* throttle time */ . pack('N', 1) . pack('n', strlen(self::TOPIC)) . self::TOPIC;
        $body .= pack('N', count($partitions));
        foreach ($partitions as $partitionId => $errorCode) {
            $body .= pack('N', $partitionId) . pack('n', $errorCode);
        }

        return ResponseFrame::of(0, $body);
    }

    /**
     * A DescribeLogDirs answer whose only directory is offline, i.e. carries the error code 56
     */
    private static function offlineDirectoryResponse(): string
    {
        return self::describeLogDirsResponse([[56, self::FIRST_DIR, []]]);
    }

    /**
     * Builds a DescribeLogDirs answer of version **2**, the flexible one the client sends since Kafka 2.6
     *
     * The answer of the versions 0 and 1 is the same fields in the plain encoding; the wire vectors of those
     * versions are replayed by `tests/Compliance` through their own classes, while the scripted broker of these
     * tests has to speak the version the client sends.
     *
     * @param list<array{0: int, 1: string, 2: array<string, list<array{0: int, 1: int, 2: int, 3: bool}>>}> $dirs
     *        Error code, path and replicas - by topic, each `[partition, size, offsetLag, isFuture]` - per directory
     */
    private static function describeLogDirsResponse(array $dirs): string
    {
        $body = pack('N', 0)                       // throttle_time_ms
            . self::unsignedVarint(count($dirs) + 1);

        foreach ($dirs as [$errorCode, $logDir, $topics]) {
            $body .= pack('n', $errorCode) . self::compactString($logDir)
                . self::unsignedVarint(count($topics) + 1);

            foreach ($topics as $topic => $partitions) {
                $body .= self::compactString((string) $topic) . self::unsignedVarint(count($partitions) + 1);
                foreach ($partitions as [$partition, $size, $offsetLag, $isFuture]) {
                    $body .= pack('N', $partition) . pack('J', $size) . pack('J', $offsetLag)
                        . ($isFuture ? "\x01" : "\x00") . "\x00";
                }
                $body .= "\x00";                    // the tag buffer of the topic
            }
            $body .= "\x00";                        // the tag buffer of the directory
        }

        // The response header v1 carries a tagged-field section of its own, the body ends in one
        return ResponseFrame::of(0, "\x00" . $body . "\x00");
    }

    /**
     * An unsigned varint of KIP-482: the value itself, seven bits per byte, least significant group first
     */
    private static function unsignedVarint(int $value): string
    {
        $bytes = '';
        while (($value & ~0x7F) !== 0) {
            $bytes .= chr(($value & 0x7F) | 0x80);
            $value >>= 7;
        }

        return $bytes . chr($value);
    }

    /**
     * A string of a flexible version: the length plus one as an unsigned varint, then the bytes
     */
    private static function compactString(string $value): string
    {
        return self::unsignedVarint(strlen($value) + 1) . $value;
    }

    /**
     * Returns the raw frame of a documented wire vector
     */
    private static function vector(string $api, string $id): string
    {
        foreach (VectorFile::read($api)['vectors'] as $vector) {
            if ($vector['id'] === $id) {
                return (string) hex2bin($vector['hex']);
            }
        }

        self::fail("There is no wire vector {$id} in docs/protocol/vectors/{$api}.json");
    }
}
