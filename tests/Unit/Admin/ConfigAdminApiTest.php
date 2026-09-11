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
use Protocol\Kafka\Admin\ConfigSource;
use Protocol\Kafka\Admin\ConfigType;
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
use Protocol\Kafka\Protocol\ApiKeys;
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
 * @see docs/protocol/2.8.md, sections "DeleteRecords API (key 21, v0 to v2)", "DescribeConfigs API (key 32, v0 to v4)" and
 *      "AlterConfigs API (key 33, v0 to v2)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(Client::class)]
final class ConfigAdminApiTest extends TestCase
{
    private const string TOPIC = 't5-vectors';

    /**
     * The topic of the DescribeConfigs v1 vectors, which was created with `segment.bytes` of its own
     */
    private const string VECTOR_TOPIC = 't4-26-own';

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
        // The answer of a 2.8.2 broker to the version 3 this client sends: the resource of the ANSWER is what the
        // result is keyed by, every entry carries its config source and its synonyms (KIP-226) and, behind them,
        // the data type and the documentation of KIP-569
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(
                self::describeConfigsResponse([[
                    0,
                    '',
                    ConfigResource::TYPE_TOPIC,
                    self::VECTOR_TOPIC,
                    [
                        ['segment.bytes', '104857600', false, ConfigSource::TOPIC_CONFIG, false, [
                            ['segment.bytes', '104857600', ConfigSource::TOPIC_CONFIG],
                            ['log.segment.bytes', '1073741824', ConfigSource::STATIC_BROKER_CONFIG],
                            ['log.segment.bytes', '1073741824', ConfigSource::DEFAULT_CONFIG],
                        ], 3, null],
                        ['retention.ms', '604800000', false, ConfigSource::DEFAULT_CONFIG, false, [], 5, null],
                    ],
                ]])
            ))
            ->install();

        $configs = $this->adminClient()->describeConfigs([ConfigResource::topic(self::VECTOR_TOPIC)], null, true);

        self::assertSame(['topic:' . self::VECTOR_TOPIC], array_keys($configs));
        $config = $configs['topic:' . self::VECTOR_TOPIC];
        self::assertInstanceOf(Config::class, $config);
        self::assertSame('604800000', $config->value('retention.ms'));
        self::assertSame('104857600', $config->value('segment.bytes'));
        self::assertSame(ConfigSource::TOPIC_CONFIG, $config->get('segment.bytes')->source);
        self::assertSame(ConfigSource::DEFAULT_CONFIG, $config->get('retention.ms')->source);
        self::assertTrue($config->get('retention.ms')->isDefault, 'derived from the source, not from a boolean');
        self::assertCount(3, $config->get('segment.bytes')->synonyms);
        self::assertSame(
            ['segment.bytes' => '104857600'],
            $config->ownValues(),
            'the topic of the vector set exactly one option of its own'
        );
    }

    public function testTheRequestIsTheFlexibleVersionFourWithBothFlagsOfKip226AndKip569(): void
    {
        $broker = new BrokerConnection(self::describeConfigsResponse([[
            0,
            '',
            ConfigResource::TYPE_TOPIC,
            self::VECTOR_TOPIC,
            [
                ['segment.bytes', '104857600', false, ConfigSource::TOPIC_CONFIG, false, [
                    ['segment.bytes', '104857600', ConfigSource::TOPIC_CONFIG],
                    ['log.segment.bytes', '1073741824', ConfigSource::STATIC_BROKER_CONFIG],
                    ['log.segment.bytes', '1073741824', ConfigSource::DEFAULT_CONFIG],
                ], 3, null],
                ['retention.ms', '604800000', false, ConfigSource::DEFAULT_CONFIG, false, [], 5, null],
            ],
        ]]));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::FIRST_BROKER, $broker)
            ->install();

        $this->adminClient()->describeConfigs([ConfigResource::topic(self::VECTOR_TOPIC)], ['segment.bytes'], true);

        // The received frame is the request without its Size, so the api key and the version open it
        $frame = $broker->getReceivedFrames()[0];
        self::assertSame(ApiKeys::DESCRIBE_CONFIGS, unpack('n', substr($frame, 0, 2))[1]);
        self::assertSame(
            4,
            unpack('n', substr($frame, 2, 2))[1],
            'the api version of the header is the flexible 4 that Kafka 2.8 added (KIP-482)'
        );
        self::assertSame(
            "\x01\x00\x00",
            substr($frame, -3),
            'and the two flags of KIP-226 and KIP-569 close the body, before its tagged-field section'
        );
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
        self::assertTrue($configs['broker:1']->get('broker.id')->isReadOnly, 'the id of a broker is never dynamic');
        self::assertSame(ConfigSource::STATIC_BROKER_CONFIG, $configs['broker:1']->get('broker.id')->source);
        self::assertFalse($configs['broker:1']->get('broker.id')->isDefault, 'a static value is not a default');
        self::assertSame(ConfigType::INT, $configs['broker:1']->get('broker.id')->type, 'the type of KIP-569');
        self::assertNull(
            $configs['broker:1']->get('broker.id')->documentation,
            'which the broker fills whatever `include_documentation` says, unlike the documentation'
        );
        self::assertSame(
            ['broker.id' => '1'],
            $configs['broker:1']->nonDefaultValues(),
            'while the resource itself owns nothing: ownValues() is empty'
        );
        self::assertSame([], $configs['broker:1']->ownValues());
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
            // A resource that was refused carries no config entry at all, so its frame is the same in every
            // version of the api - the v0 vector of the 0.11 line is a valid v3 answer
            ->on(self::SECOND_BROKER, new BrokerConnection(
                self::describeConfigsResponse([[
                    42,
                    'Unexpected broker id, expected 0, but received 7',
                    ConfigResource::TYPE_BROKER,
                    '7',
                    [],
                ]])
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
                self::alterConfigsResponse([[
                    40,
                    'Unknown topic config name: not.an.option',
                    ConfigResource::TYPE_TOPIC,
                    self::TOPIC,
                ]])
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
            ->on(self::FIRST_BROKER, new BrokerConnection(self::alterConfigsResponse([[0, null, ConfigResource::TYPE_TOPIC, self::TOPIC]])))
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
            ->on(self::FIRST_BROKER, new BrokerConnection(self::alterConfigsResponse([[0, null, ConfigResource::TYPE_TOPIC, self::TOPIC]])))
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
        // Version 2 (Kafka 2.6) is the first FLEXIBLE version of this api (KIP-482): every string and array
        // announces its length as the unsigned varint `length + 1`, the answer carries the response header v1
        // and every structure ends in a tagged-field section, which is `00` while nothing is tagged
        $tagBuffer = "\x00";
        $body      = pack('N', 0) /* throttle time */ . chr(1 + 1)
            . chr(strlen(self::TOPIC) + 1) . self::TOPIC
            . chr(count($partitions) + 1);
        foreach ($partitions as $partitionId => [$lowWatermark, $errorCode]) {
            $body .= pack('N', $partitionId) . pack('J', $lowWatermark) . pack('n', $errorCode) . $tagBuffer;
        }
        $body .= $tagBuffer /* the topic entry */ . $tagBuffer /* the body */;

        return ResponseFrame::of(0, $tagBuffer . $body);
    }

    /**
     * A DescribeConfigs v1 answer of one broker resource with its `broker.id`
     *
     * `broker.id` is the one option of a broker that is read-only even on a 1.1 broker - no synonym of it is in
     * `DynamicBrokerConfig.AllDynamicConfigs` - and its source is the `server.properties` of the container, with
     * the built-in default `-1` behind it as a synonym.
     */
    private static function brokerConfigResponse(int $brokerId): string
    {
        $name = (string) $brokerId;

        return self::describeConfigsResponse([[
            0,
            null,
            ConfigResource::TYPE_BROKER,
            $name,
            [[
                'broker.id',
                $name,
                true,                                  // the id of a broker is never dynamic
                ConfigSource::STATIC_BROKER_CONFIG,
                false,
                [
                    ['broker.id', $name, ConfigSource::STATIC_BROKER_CONFIG],
                    ['broker.id', '-1', ConfigSource::DEFAULT_CONFIG],
                ],
                ConfigType::INT,                       // the two fields KIP-569 appended with the version 3
                null,
            ]],
        ]]);
    }

    /**
     * Builds a DescribeConfigs answer of version **4**, the flexible one the client sends since Kafka 2.8
     *
     * The frames of the versions 0 to 3 are the wire vectors of this document, replayed by `tests/Compliance`
     * through their own classes; a scripted broker has to speak the version the client sends, and the version 4
     * of KIP-482 is that version now.
     *
     * @param list<array{0: int, 1: string|null, 2: int, 3: string, 4: list<array{0: string, 1: string|null,
     *        2: bool, 3: int, 4: bool, 5: list<array{0: string, 1: string|null, 2: int}>, 6: int, 7: string|null}>}>
     *        $resources Error code, message, type, name and entries of every resource of the answer
     */
    private static function describeConfigsResponse(array $resources): string
    {
        $body = "\x00" . pack('N', 0) . self::unsignedVarint(count($resources) + 1);

        foreach ($resources as [$errorCode, $errorMessage, $type, $name, $entries]) {
            $body .= pack('n', $errorCode) . self::compactNullableString($errorMessage)
                . chr($type) . self::compactString($name)
                . self::unsignedVarint(count($entries) + 1);

            foreach ($entries as [$entry, $value, $readOnly, $source, $sensitive, $synonyms, $entryType, $doc]) {
                $body .= self::compactString($entry) . self::compactNullableString($value)
                    . ($readOnly ? "\x01" : "\x00") . chr($source) . ($sensitive ? "\x01" : "\x00")
                    . self::unsignedVarint(count($synonyms) + 1);

                foreach ($synonyms as [$synonym, $synonymValue, $synonymSource]) {
                    $body .= self::compactString($synonym) . self::compactNullableString($synonymValue)
                        . chr($synonymSource) . "\x00";
                }

                $body .= chr($entryType) . self::compactNullableString($doc) . "\x00";
            }

            $body .= "\x00";
        }

        return ResponseFrame::of(0, $body . "\x00");
    }

    /**
     * Builds an AlterConfigs answer of version **2**, the flexible one the client sends since Kafka 2.8
     *
     * @param list<array{0: int, 1: string|null, 2: int, 3: string}> $resources Error code, message, type and name
     */
    private static function alterConfigsResponse(array $resources): string
    {
        $body = "\x00" . pack('N', 0) . self::unsignedVarint(count($resources) + 1);

        foreach ($resources as [$errorCode, $errorMessage, $type, $name]) {
            $body .= pack('n', $errorCode) . self::compactNullableString($errorMessage)
                . chr($type) . self::compactString($name) . "\x00";
        }

        return ResponseFrame::of(0, $body . "\x00");
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
     * The same, with the 0 that means null
     */
    private static function compactNullableString(?string $value): string
    {
        return $value === null ? "\x00" : self::compactString($value);
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
