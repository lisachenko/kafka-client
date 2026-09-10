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

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\Config;
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Admin\DeletedRecords;
use Protocol\Kafka\Admin\RecordsToDelete;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\OffsetOutOfRangeException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Tests\Compliance\VectorFile;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * Tests the three admin apis Kafka 0.11 added against scripted brokers: where each request goes, and how the
 * answers are mapped onto the return values of {@see AdminClient}.
 *
 * The canned answers are the documented wire vectors of `docs/protocol/vectors` wherever one fits, so this suite
 * and the compliance suite cannot disagree about what a broker says.
 *
 * @see docs/protocol/1.1.md, sections "DeleteRecords API (key 21, v0)", "DescribeConfigs API (key 32, v0)" and
 *      "AlterConfigs API (key 33, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(Client::class)]
final class ConfigAdminApiTest extends TestCase
{
    private const string TOPIC = 't5-vectors';

    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    private const string FIRST_BROKER = 'tcp://kafka-1:9092';

    private const string SECOND_BROKER = 'tcp://kafka-2:9093';

    private ScriptedConnections $brokers;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testDescribeConfigsOfATopicGoesToAnyBrokerAndIsKeyedByTheResource(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(self::vector('describe-configs', 'describeconfigs.response.v0.topic')))
            ->install();

        $configs = $this->adminClient()->describeConfigs([ConfigResource::topic(self::TOPIC)]);

        self::assertSame(['topic:' . self::TOPIC], array_keys($configs));
        $config = $configs['topic:' . self::TOPIC];
        self::assertInstanceOf(Config::class, $config);
        self::assertSame('604800000', $config->value('retention.ms'));
        self::assertSame('delete', $config->value('cleanup.policy'));
        self::assertSame([], $config->nonDefaultValues(), 'the topic of the vector has no option of its own');
    }

    public function testDescribeConfigsOfABrokerGoesToThatBroker(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::SECOND_BROKER, new BrokerConnection(self::brokerConfigResponse(1)))
            ->install();

        $configs = $this->adminClient()->describeConfigs([ConfigResource::broker(1)], ['broker.id']);

        self::assertSame(['broker:1'], array_keys($configs));
        self::assertSame('1', $configs['broker:1']->value('broker.id'));
        self::assertTrue($configs['broker:1']->get('broker.id')->isReadOnly);
        self::assertNotContains(
            self::FIRST_BROKER,
            $this->brokers->getOpenedAddresses(),
            'a broker resource is only answered by the broker it names'
        );
    }

    public function testDescribeConfigsThrowsTheErrorOfARefusedResource(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::SECOND_BROKER, new BrokerConnection(
                self::vector('describe-configs', 'describeconfigs.response.v0.unknown-broker')
            ))
            ->install();

        $this->expectException(InvalidRequestException::class);

        $this->adminClient()->describeConfigs([ConfigResource::broker(1)]);
    }

    public function testAlterConfigsReportsTheErrorOfEveryResourceWithoutThrowing(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(
                self::vector('alter-configs', 'alterconfigs.response.v0.unknown-config')
            ))
            ->install();

        $result = $this->adminClient()->alterConfigs([
            ConfigResource::topic(self::TOPIC)->key() => ['no.such.option' => '1'],
        ]);

        self::assertSame(['topic:' . self::TOPIC], array_keys($result));
        self::assertInstanceOf(InvalidConfigException::class, $result['topic:' . self::TOPIC]);
    }

    public function testAnAlteredResourceIsReportedWithNull(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(self::vector('alter-configs', 'alterconfigs.response.v0')))
            ->install();

        $result = $this->adminClient()->alterConfigs([
            ConfigResource::topic(self::TOPIC)->key() => ['retention.ms' => '3600000'],
        ]);

        self::assertSame(['topic:' . self::TOPIC => null], $result);
    }

    public function testAResourceTheBrokerDidNotAnswerBecomesAnUnknownError(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(self::vector('alter-configs', 'alterconfigs.response.v0')))
            ->install();

        $result = $this->adminClient()->alterConfigs([
            ConfigResource::topic(self::TOPIC)->key() => ['retention.ms' => '3600000'],
            ConfigResource::topic('other')->key()     => ['retention.ms' => '1'],
        ]);

        self::assertNull($result['topic:' . self::TOPIC]);
        self::assertInstanceOf(UnknownErrorException::class, $result['topic:other']);
    }

    public function testDeleteRecordsIsSplitPerPartitionLeader(): void
    {
        $first  = new BrokerConnection(self::deleteRecordsResponse([0 => [2, 0]]));
        $second = new BrokerConnection(self::deleteRecordsResponse([1 => [7, 0]]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, $first)
            ->on(self::SECOND_BROKER, $second)
            ->install();

        $result = $this->adminClient()->deleteRecords([
            self::TOPIC => [0 => 2, 1 => RecordsToDelete::allRecords()],
        ]);

        self::assertSame(1, $first->getRequestCount(), 'one request per leader, like a produce');
        self::assertSame(1, $second->getRequestCount());
        self::assertInstanceOf(DeletedRecords::class, $result[self::TOPIC][0]);
        self::assertSame(2, $result[self::TOPIC][0]->lowWatermark);
        self::assertSame(7, $result[self::TOPIC][1]->lowWatermark);
    }

    public function testAPartitionThatCouldNotBeDeletedIsReportedWithThePartialResult(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(self::deleteRecordsResponse([0 => [-1, 1]])))
            ->on(self::SECOND_BROKER, new BrokerConnection(self::deleteRecordsResponse([1 => [7, 0]])))
            ->install();

        try {
            $this->adminClient()->deleteRecords([self::TOPIC => [0 => 100, 1 => 7]]);
            self::fail('the partition that answered an error has to be reported');
        } catch (TopicPartitionRequestException $exception) {
            self::assertInstanceOf(
                OffsetOutOfRangeException::class,
                $exception->getExceptions()[self::TOPIC][0],
                'an offset above the high watermark is answered with 1, not with 3 or 42'
            );
            self::assertSame(7, $exception->getPartialResult()[self::TOPIC][1]->lowWatermark);
        }
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
            ClientConfig::CLIENT_ID                 => 't5',
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
        ];

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * A DeleteRecords answer of one topic: partition id => [low watermark, error code]
     *
     * @param array<int, array{0: int, 1: int}> $partitions
     */
    private static function deleteRecordsResponse(array $partitions): string
    {
        $body = pack('N', 0) /* throttle time */ . pack('N', 1) . pack('n', strlen(self::TOPIC)) . self::TOPIC;
        $body .= pack('N', count($partitions));
        foreach ($partitions as $partitionId => [$lowWatermark, $errorCode]) {
            $body .= pack('N', $partitionId) . pack('J', $lowWatermark) . pack('n', $errorCode);
        }

        return ResponseFrame::of(0, $body);
    }

    /**
     * A DescribeConfigs answer of one broker resource with its `broker.id`, which is read only and not a default
     */
    private static function brokerConfigResponse(int $brokerId): string
    {
        $name = (string) $brokerId;
        $body = pack('N', 0) /* throttle time */ . pack('N', 1);
        $body .= pack('n', 0) . pack('n', 0xFFFF) . pack('c', ConfigResource::TYPE_BROKER);
        $body .= pack('n', strlen($name)) . $name;
        $body .= pack('N', 1) . pack('n', 9) . 'broker.id' . pack('n', strlen($name)) . $name;
        $body .= pack('C', 1) . pack('C', 0) . pack('C', 0);

        return ResponseFrame::of(0, $body);
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
