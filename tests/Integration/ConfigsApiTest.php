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
use Protocol\Kafka\Admin\Config;
use Protocol\Kafka\Admin\ConfigEntry;
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Protocol\Data\AlterConfigsRequestConfigEntry;
use Protocol\Kafka\Protocol\Data\AlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\AlterConfigsResponseResource;
use Protocol\Kafka\Protocol\Data\DescribeConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntry;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseResource;
use Protocol\Kafka\Protocol\Request\AlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\AlterConfigsResponse;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequest;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponse;

/**
 * Exercises the DescribeConfigs (key 32) and AlterConfigs (key 33) apis against a real Kafka 1.1.1 broker.
 *
 * Both arrived with Kafka 0.11 (KIP-133) and Kafka 1.1 raised them with KIP-226: DescribeConfigs got a version 1
 * that answers a config **source** and its synonyms instead of an `is_default` boolean, and AlterConfigs started
 * accepting a **broker** resource for the options a broker can change at runtime. This class sends version 0 of
 * both and pins what a 1.1 broker answers to it - T4 of this line owns the version 1 and the dynamic broker
 * configuration. Nothing here changes a broker-level setting: every AlterConfigs of a broker resource below names
 * an option that is *not* dynamically updatable and is therefore refused, and the container is shared with the
 * other suites of this line.
 *
 * @see docs/protocol/1.1.md, sections "DescribeConfigs API (key 32, v0)" and "AlterConfigs API (key 33, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(Config::class)]
#[CoversClass(ConfigEntry::class)]
#[CoversClass(ConfigResource::class)]
#[CoversClass(DescribeConfigsRequest::class)]
#[CoversClass(DescribeConfigsResponse::class)]
#[CoversClass(DescribeConfigsRequestResource::class)]
#[CoversClass(DescribeConfigsResponseResource::class)]
#[CoversClass(DescribeConfigsResponseConfigEntry::class)]
#[CoversClass(AlterConfigsRequest::class)]
#[CoversClass(AlterConfigsResponse::class)]
#[CoversClass(AlterConfigsRequestResource::class)]
#[CoversClass(AlterConfigsRequestConfigEntry::class)]
#[CoversClass(AlterConfigsResponseResource::class)]
final class ConfigsApiTest extends IntegrationTestCase
{
    /**
     * The topic options that a 1.1 broker never reports as defaults, because the container sets their broker synonym
     *
     * KIP-226 replaced the `is_default` boolean of the DescribeConfigs answer with a config **source**, and the
     * `is_default` of the version 0 answer is derived from it: `AdminManager.createTopicConfigEntry()` @ 1.1.1 walks
     * `LogConfig.TopicConfigSynonyms`, and an option whose broker synonym stands in the `server.properties` of the
     * container gets the source `STATIC_BROKER_CONFIG` - which is not `DEFAULT_CONFIG`, so `is_default` is false
     * although the topic itself set nothing. A 0.11.0.3 broker answered `is_default = !topicProps.containsKey(name)`
     * and reported such an option as a default.
     *
     * The image of this line sets exactly one of them: `log.segment.bytes=1073741824`, the synonym of the topic
     * option `segment.bytes` (`log.retention.hours` is set too, but the synonym of `retention.ms` that the broker
     * looks at first is `log.retention.ms`, which is not set). T4 owns the api and its version 1, which reports the
     * source itself.
     */
    private const array STATIC_BROKER_TOPIC_OPTIONS = ['segment.bytes' => '1073741824'];

    private Cluster $cluster;

    private AdminClient $admin;

    /**
     * Topics this test class created, deleted again after every test
     *
     * @var list<string>
     */
    private array $createdTopics = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    public function testAFreshTopicHasOnlyTheOptionsTheBrokerSetsStatically(): void
    {
        $topic    = $this->createTopic('describe');
        $resource = ConfigResource::topic($topic);

        $configs = $this->admin->describeConfigs([$resource]);

        self::assertSame([$resource->key()], array_keys($configs));
        $config = $configs[$resource->key()];
        self::assertGreaterThan(20, count($config->entries), 'a 1.1 topic has more than twenty options');
        self::assertSame(
            self::STATIC_BROKER_TOPIC_OPTIONS,
            $config->nonDefaultValues(),
            'a topic created without options has none of its own but the ones the broker sets statically'
        );
        self::assertFalse($config->get('segment.bytes')->isDefault, 'log.segment.bytes IS in the server.properties');
        self::assertSame('604800000', $config->value('retention.ms'), 'the broker default of seven days');
        self::assertTrue($config->get('retention.ms')->isDefault, 'log.retention.ms is not in the server.properties');
        self::assertFalse($config->get('retention.ms')->isReadOnly, 'a topic option can be altered');
        self::assertFalse($config->get('retention.ms')->isSensitive);
        self::assertFalse(
            $config->get('segment.bytes')->isReadOnly,
            'a topic entry is never read-only: `createTopicConfigEntry()` passes the flag as false'
        );
    }

    public function testATopicOptionOfTheRequestIsReportedAsNotDefault(): void
    {
        $topic    = $this->createTopic('created-with', ['retention.ms' => '3600000']);
        $resource = ConfigResource::topic($topic);

        $config = $this->admin->describeConfigs([$resource], ['retention.ms', 'cleanup.policy'])[$resource->key()];

        self::assertEqualsCanonicalizing(
            ['retention.ms', 'cleanup.policy'],
            array_keys($config->entries),
            'the broker walks a Scala map, so the entries are not in the order of the request'
        );
        self::assertSame('3600000', $config->value('retention.ms'));
        self::assertFalse($config->get('retention.ms')->isDefault, 'it was written into the ZooKeeper node');
        self::assertTrue($config->get('cleanup.policy')->isDefault);
        self::assertSame(['retention.ms' => '3600000'], $config->nonDefaultValues());
    }

    /**
     * The `is_default` of a version 0 answer is derived from the config source of KIP-226
     *
     * A 0.11.0.3 broker answered `is_default = !topicProps.containsKey(name)`, i.e. "the topic did not set it". A 1.1
     * broker answers the source instead - `TOPIC_CONFIG`, `STATIC_BROKER_CONFIG`, `DEFAULT_CONFIG`, … - and
     * `DescribeConfigsResponse` writes `is_default = (source == DEFAULT_CONFIG)` into the version 0 frame. The two
     * disagree for exactly the options whose **broker synonym** stands in the `server.properties` of the container.
     */
    public function testATopicOptionThatTheBrokerSetsStaticallyIsNotReportedAsADefault(): void
    {
        $topic    = $this->createTopic('static-source');
        $resource = ConfigResource::topic($topic);

        $config = $this->admin->describeConfigs([$resource], ['segment.bytes', 'retention.ms'])[$resource->key()];

        self::assertFalse(
            $config->get('segment.bytes')->isDefault,
            'log.segment.bytes stands in the server.properties, so the source is STATIC_BROKER_CONFIG'
        );
        self::assertSame('1073741824', $config->value('segment.bytes'));
        self::assertTrue(
            $config->get('retention.ms')->isDefault,
            'log.retention.ms does not, and log.retention.hours is not the synonym the broker looks at first'
        );
    }

    public function testAnOptionNameTheBrokerDoesNotKnowIsDroppedFromTheAnswer(): void
    {
        $topic    = $this->createTopic('unknown-name');
        $resource = ConfigResource::topic($topic);

        $config = $this->admin->describeConfigs([$resource], ['no.such.option'])[$resource->key()];

        self::assertSame([], $config->entries, 'there is no "unknown config" error in DescribeConfigs');
    }

    public function testATopicThatDoesNotExistIsAnsweredWithTheDefaults(): void
    {
        // `AdminUtils.fetchEntityConfig` returns empty properties for a topic without a ZooKeeper node, so the
        // answer is the log defaults of the broker with the error code 0 - the api says nothing about existence
        $topic    = self::uniqueTopicName('t5-configs-never-created');
        $resource = ConfigResource::topic($topic);

        $config = $this->admin->describeConfigs([$resource], ['retention.ms'])[$resource->key()];

        self::assertSame('604800000', $config->value('retention.ms'));
        self::assertTrue($config->get('retention.ms')->isDefault);
        self::assertNotContains($topic, $this->admin->listTopics(), 'and the topic was not created either');
    }

    public function testAnIllegalTopicNameIsRefusedWithSeventeen(): void
    {
        $this->expectException(InvalidTopicException::class);

        $this->admin->describeConfigs([ConfigResource::topic('t5 configs illegal name')]);
    }

    /**
     * `is_read_only` of a broker entry means "not dynamically updatable" since KIP-226
     *
     * Up to 0.11 every entry of a broker resource was read-only, because a broker could not change any of its own
     * configuration at runtime. `AdminManager.createBrokerConfigEntry()` @ 1.1.1 computes
     * `readOnly = !allNames.exists(DynamicBrokerConfig.AllDynamicConfigs.contains)` instead, so an option that
     * KIP-226 made dynamic - directly or through one of its synonyms - is answered as **writable**. `broker.id` is
     * still read-only, `log.retention.hours` is not, because its synonym `log.retention.ms` is a dynamic config.
     *
     * Writable is not the same as updatable under this name, though: AlterConfigs of `log.retention.hours` is
     * refused, see below. T4 owns the api and what a client should do with the distinction.
     */
    public function testABrokerEntryIsReadOnlyOnlyWhenItIsNotDynamicallyUpdatable(): void
    {
        $brokerId = array_key_first($this->admin->findAllBrokers());
        $resource = ConfigResource::broker($brokerId);

        $config = $this->admin->describeConfigs(
            [$resource],
            ['broker.id', 'log.retention.hours', 'ssl.keystore.password']
        )[$resource->key()];

        self::assertSame((string) $brokerId, $config->value('broker.id'));
        self::assertTrue($config->get('broker.id')->isReadOnly, 'the id of a broker is never dynamic');
        self::assertFalse(
            $config->get('log.retention.hours')->isReadOnly,
            'its synonym log.retention.ms is one of the dynamic configs of KIP-226'
        );
        self::assertFalse($config->get('broker.id')->isDefault, 'it is in the server.properties of the container');

        $password = $config->get('ssl.keystore.password');
        self::assertTrue($password->isSensitive);
        self::assertNull($password->value, 'the broker never sends the value of a PASSWORD option');
    }

    public function testABrokerIdThatIsNotTheOneThatAnswersIsRefused(): void
    {
        // The container runs a single broker, so an id that no broker has can only be answered by the wrong one
        $this->expectException(InvalidRequestException::class);

        $this->admin->describeConfigs([ConfigResource::broker(4242)], ['broker.id']);
    }

    public function testABrokerIdThatIsNotANumberIsRefused(): void
    {
        $this->expectException(InvalidRequestException::class);

        $this->admin->describeConfigs([ConfigResource::broker('nope')], ['broker.id']);
    }

    public function testAlterConfigsReplacesTheWholeConfigurationOfATopic(): void
    {
        $topic    = $this->createTopic('alter');
        $resource = ConfigResource::topic($topic);

        $first = $this->admin->alterConfigs([
            $resource->key() => ['retention.ms' => '3600000', 'cleanup.policy' => 'compact'],
        ]);
        self::assertSame([$resource->key() => null], $first);
        self::assertEqualsCanonicalizing(
            ['retention.ms' => '3600000', 'cleanup.policy' => 'compact'] + self::STATIC_BROKER_TOPIC_OPTIONS,
            $this->nonDefaults($resource),
            'both options are now the topic\'s own, next to the ones the broker sets statically'
        );

        // The second request names only one of them, and the other one falls back to the broker default: the api
        // REPLACES the ZooKeeper node of the topic instead of patching it
        $second = $this->admin->alterConfigs([$resource->key() => ['retention.ms' => '7200000']]);
        self::assertSame([$resource->key() => null], $second);
        self::assertEqualsCanonicalizing(
            ['retention.ms' => '7200000'] + self::STATIC_BROKER_TOPIC_OPTIONS,
            $this->nonDefaults($resource)
        );

        // And an empty entry list resets every option of the topic
        self::assertSame([$resource->key() => null], $this->admin->alterConfigs([$resource->key() => []]));
        self::assertSame(self::STATIC_BROKER_TOPIC_OPTIONS, $this->nonDefaults($resource));
    }

    public function testValidateOnlyChangesNothing(): void
    {
        $topic    = $this->createTopic('validate');
        $resource = ConfigResource::topic($topic);

        $result = $this->admin->alterConfigs([$resource->key() => ['retention.ms' => '3600000']], true);

        self::assertSame([$resource->key() => null], $result, 'the request was valid');
        self::assertSame(
            self::STATIC_BROKER_TOPIC_OPTIONS,
            $this->nonDefaults($resource),
            'and nothing was written to ZooKeeper'
        );
    }

    public function testAnUnknownOptionNameIsRefusedWithForty(): void
    {
        $topic    = $this->createTopic('bad-name');
        $resource = ConfigResource::topic($topic);

        $result = $this->admin->alterConfigs([$resource->key() => ['no.such.option' => '1']]);

        $error = $result[$resource->key()];
        self::assertInstanceOf(InvalidConfigException::class, $error);
        self::assertStringContainsString('Unknown topic config name: no.such.option', $error->getMessage());
    }

    public function testAnUnparsableOptionValueIsRefusedWithFortyTwo(): void
    {
        // The same value is answered with -1 by CreateTopics, which does not catch the ConfigException itself
        $topic    = $this->createTopic('bad-value');
        $resource = ConfigResource::topic($topic);

        $result = $this->admin->alterConfigs([$resource->key() => ['retention.ms' => 'soon']]);

        $error = $result[$resource->key()];
        self::assertInstanceOf(InvalidRequestException::class, $error);
        self::assertStringContainsString('Not a number of type LONG', $error->getMessage());
    }

    public function testANullOptionValueIsAnsweredWithMinusOne(): void
    {
        // The schema declares the value nullable, but `Properties.setProperty` throws a NullPointerException on it
        $topic    = $this->createTopic('null-value');
        $resource = ConfigResource::topic($topic);

        $result = $this->admin->alterConfigs([$resource->key() => ['retention.ms' => null]]);

        self::assertInstanceOf(UnknownErrorException::class, $result[$resource->key()]);
        self::assertSame(self::STATIC_BROKER_TOPIC_OPTIONS, $this->nonDefaults($resource), 'and nothing was changed');
    }

    public function testAlteringATopicThatDoesNotExistIsAnsweredWithMinusOne(): void
    {
        $topic    = self::uniqueTopicName('t5-configs-missing');
        $resource = ConfigResource::topic($topic);

        $result = $this->admin->alterConfigs([$resource->key() => ['retention.ms' => '3600000']]);

        $error = $result[$resource->key()];
        self::assertInstanceOf(UnknownErrorException::class, $error);
        self::assertStringContainsString('does not exist', $error->getMessage());
    }

    /**
     * A broker resource is accepted by AlterConfigs since KIP-226, and refused **per option**
     *
     * A 0.11.0.3 broker refused every AlterConfigs of a broker resource outright, with the message
     * `AlterConfigs is only supported for topics, but resource type is BROKER`. A 1.1 broker takes the request,
     * hands the entries to `DynamicBrokerConfig.validate()` and answers 42 with the names it cannot change at
     * runtime: `Cannot update these configs dynamically: Set(log.retention.hours)`. An option of the dynamic set -
     * `log.cleaner.threads`, for instance - is really applied, which is why nothing here sends one: the container
     * is shared with the other suites of this line, and T4 owns the api and the dynamic configuration.
     */
    public function testAStaticOptionOfABrokerResourceIsRefusedPerOptionAndChangesNothing(): void
    {
        $brokerId = array_key_first($this->admin->findAllBrokers());
        $resource = ConfigResource::broker($brokerId);
        $before   = $this->admin->describeConfigs([$resource], ['log.retention.hours'])[$resource->key()];

        $result = $this->admin->alterConfigs([$resource->key() => ['log.retention.hours' => '169']]);

        $error = $result[$resource->key()];
        self::assertInstanceOf(InvalidRequestException::class, $error);
        self::assertStringContainsString(
            'Cannot update these configs dynamically: Set(log.retention.hours)',
            $error->getMessage()
        );
        self::assertStringNotContainsString(
            'AlterConfigs is only supported for topics',
            $error->getMessage(),
            'the blanket refusal of a 0.11 broker is gone'
        );

        $after = $this->admin->describeConfigs([$resource], ['log.retention.hours'])[$resource->key()];
        self::assertSame(
            $before->value('log.retention.hours'),
            $after->value('log.retention.hours'),
            'a refused entry changes nothing'
        );
    }

    public function testValidateOnlyDoesNotHelpABrokerResourceEither(): void
    {
        $brokerId = array_key_first($this->admin->findAllBrokers());
        $resource = ConfigResource::broker($brokerId);

        $result = $this->admin->alterConfigs([$resource->key() => ['log.retention.hours' => '169']], true);

        self::assertInstanceOf(InvalidRequestException::class, $result[$resource->key()]);
    }

    /**
     * Returns the options of a resource that are not defaults
     *
     * @return array<string, string|null>
     */
    private function nonDefaults(ConfigResource $resource): array
    {
        return $this->admin->describeConfigs([$resource])[$resource->key()]->nonDefaultValues();
    }

    /**
     * Creates a topic of one partition and remembers it for the cleanup
     *
     * @param array<string, string> $configs Topic-level options of the new topic
     */
    private function createTopic(string $purpose, array $configs = []): string
    {
        $topic                 = self::uniqueTopicName("t5-configs-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame(
            [$topic => null],
            $this->admin->createTopics([new NewTopic($topic, 1, 1, configs: $configs)])
        );

        return $topic;
    }

    /**
     * Client configuration pointing at the broker under test
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 't5-configs',
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}
