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
use Protocol\Kafka\Admin\Config;
use Protocol\Kafka\Admin\ConfigEntry;
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Admin\ConfigSource;
use Protocol\Kafka\Admin\ConfigSynonym;
use Protocol\Kafka\Admin\ConfigType;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Protocol\Data\AlterConfigsRequestConfigEntry;
use Protocol\Kafka\Protocol\Data\AlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\AlterConfigsResponseResource;
use Protocol\Kafka\Protocol\Data\DescribeConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntry;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntryV0;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigSynonym;
use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseResource;
use Protocol\Kafka\Protocol\Request\AlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\AlterConfigsRequestV0;
use Protocol\Kafka\Protocol\Request\AlterConfigsResponse;
use Protocol\Kafka\Protocol\Request\AlterConfigsResponseV0;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequest;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequestV1;
use Protocol\Kafka\Protocol\Request\DescribeConfigsRequestV2;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponse;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponseV0;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponseV1;
use Protocol\Kafka\Protocol\Request\DescribeConfigsResponseV2;

/**
 * Exercises the DescribeConfigs (key 32) and AlterConfigs (key 33) apis against a real Kafka 2.8.2 broker.
 *
 * Both arrived with Kafka 0.11 (KIP-133) and Kafka 1.1 raised them with KIP-226: DescribeConfigs got a **version 1**
 * that answers a config SOURCE and the synonyms of every option instead of an `is_default` boolean, and AlterConfigs
 * - whose frame did not change at all - started accepting a **broker** resource for the options a broker can change
 * at runtime. Kafka 2.0 raised both apis by one more version without touching a byte (KIP-219), which is what the
 * client sends now: **DescribeConfigs v2** and **AlterConfigs v1**. Every version below is still served and is
 * measured here as well - including the version 0 whose `is_default` boolean a 2.8.2 broker no longer fills in.
 *
 * The container is shared with the other suites of this line, so the two tests that really change a broker option
 * touch `log.cleaner.backoff.ms` alone - a log-cleaner back-off nothing here depends on - and put the documented
 * default back in a `finally`, explicitly and not by removing the entry, see the quirk in the AlterConfigs section.
 *
 * @see docs/protocol/2.8.md, sections "DescribeConfigs API (key 32, v0 to v4)" and "AlterConfigs API (key 33, v0 to v2)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(Config::class)]
#[CoversClass(ConfigEntry::class)]
#[CoversClass(ConfigResource::class)]
#[CoversClass(ConfigSource::class)]
#[CoversClass(DescribeConfigsResponseV2::class)]
#[CoversClass(DescribeConfigsRequestV2::class)]
#[CoversClass(ConfigType::class)]
#[CoversClass(ConfigSynonym::class)]
#[CoversClass(DescribeConfigsRequest::class)]
#[CoversClass(DescribeConfigsRequestV0::class)]
#[CoversClass(DescribeConfigsRequestV1::class)]
#[CoversClass(DescribeConfigsResponse::class)]
#[CoversClass(DescribeConfigsResponseV0::class)]
#[CoversClass(DescribeConfigsResponseV1::class)]
#[CoversClass(DescribeConfigsRequestResource::class)]
#[CoversClass(DescribeConfigsResponseResource::class)]
#[CoversClass(DescribeConfigsResponseConfigEntry::class)]
#[CoversClass(DescribeConfigsResponseConfigEntryV0::class)]
#[CoversClass(DescribeConfigsResponseConfigSynonym::class)]
#[CoversClass(AlterConfigsRequest::class)]
#[CoversClass(AlterConfigsRequestV0::class)]
#[CoversClass(AlterConfigsResponse::class)]
#[CoversClass(AlterConfigsResponseV0::class)]
#[CoversClass(AlterConfigsRequestResource::class)]
#[CoversClass(AlterConfigsRequestConfigEntry::class)]
#[CoversClass(AlterConfigsResponseResource::class)]
final class ConfigsApiTest extends IntegrationTestCase
{
    /**
     * The topic options a 2.8.2 broker never reports as defaults, because the container sets their broker synonym
     *
     * KIP-226 replaced the `is_default` boolean of the DescribeConfigs answer with a config **source**:
     * `ConfigHelper.createTopicConfigEntry()` @ 2.8.2 walks `LogConfig.TopicConfigSynonyms`, and an option whose
     * broker synonym stands in the `server.properties` of the container gets the source `STATIC_BROKER_CONFIG` -
     * which is not `DEFAULT_CONFIG`, so the option is not a default although the topic itself set nothing. A
     * 0.11.0.3 broker answered `is_default = !topicProps.containsKey(name)` and reported such an option as a
     * default.
     *
     * The image of this line sets exactly two of them: `log.segment.bytes=1073741824`, the synonym of the topic
     * option `segment.bytes`, and `log.message.format.version=2.8-IV1`, the synonym of `message.format.version` -
     * which the 1.1.1 image of the line below did not write into its `server.properties` at all, so the second
     * entry is new on this line. `log.retention.hours` is set too, and `retention.ms` is a default nevertheless:
     * the synonyms of that option are looked up under `log.retention.ms`, and `ConfigHelper.configSynonyms()`
     * @ 2.8.2 only records the broker options that really carry a value under one of the synonym NAMES of
     * `log.retention.ms` - `log.retention.hours` is not one of them.
     */
    private const array STATIC_BROKER_TOPIC_OPTIONS = [
        'segment.bytes'          => '1073741824',
        'message.format.version' => '2.8-IV1',
    ];

    /**
     * The broker option the two dynamic tests change, with the value the `server.properties` of the image implies
     *
     * `log.cleaner.backoff.ms` is one of `LogCleaner.ReconfigurableConfigs` and therefore dynamically updatable,
     * and nothing of this suite - or of the other suites that share the container - depends on how long the log
     * cleaner sleeps between two runs.
     */
    private const string DYNAMIC_OPTION = 'log.cleaner.backoff.ms';

    /**
     * How long a dynamic broker option may take to reach the broker that answers, in seconds
     *
     * `AlterConfigs` writes the option into ZooKeeper and the broker applies it when its watch fires, so a
     * `DescribeConfigs` that overtakes that watch still answers the previous value.
     */
    private const float DYNAMIC_OPTION_TIMEOUT = 5.0;

    private const string DYNAMIC_OPTION_DEFAULT = '15000';

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

    public function testAFreshTopicInheritsEveryOptionAndSaysWhereItComesFrom(): void
    {
        $topic    = $this->createTopic('describe');
        $resource = ConfigResource::topic($topic);

        $configs = $this->admin->describeConfigs([$resource], null, true);

        self::assertSame([$resource->key()], array_keys($configs));
        $config = $configs[$resource->key()];
        self::assertGreaterThan(20, count($config->entries), 'a 1.1 topic has more than twenty options');
        self::assertSame([], $config->ownValues(), 'a topic created without options has none of its own');
        self::assertSame(
            self::STATIC_BROKER_TOPIC_OPTIONS,
            $config->nonDefaultValues(),
            'but the one whose broker synonym the container sets is not a default any more'
        );

        $segment = $config->get('segment.bytes');
        self::assertSame(ConfigSource::STATIC_BROKER_CONFIG, $segment->source, 'log.segment.bytes is configured');
        self::assertFalse($segment->isDefault);
        self::assertSame('1073741824', $segment->value);
        self::assertFalse($segment->isReadOnly, 'a topic entry is never read-only');
        self::assertFalse($segment->isSensitive);
        self::assertSame(
            ['log.segment.bytes', 'log.segment.bytes'],
            array_map(static fn(ConfigSynonym $synonym): string => $synonym->name, $segment->synonyms),
            'the synonyms of a topic option are the BROKER options behind it, here the static one and the default'
        );
        self::assertSame(
            [ConfigSource::STATIC_BROKER_CONFIG, ConfigSource::DEFAULT_CONFIG],
            array_map(static fn(ConfigSynonym $synonym): int => $synonym->source, $segment->synonyms),
            'the winning source comes first, and it is the source of the entry itself'
        );

        $retention = $config->get('retention.ms');
        self::assertSame(ConfigSource::DEFAULT_CONFIG, $retention->source);
        self::assertTrue($retention->isDefault);
        self::assertSame('604800000', $retention->value, 'the seven days of log.retention.hours=168');
        self::assertSame(
            [],
            $retention->synonyms,
            'and no synonym at all: the dropWhile of configSynonyms() drops a list without a log.retention.ms in it'
        );
    }

    public function testAnOptionTheTopicSetsItselfIsReportedWithTheTopicSourceInFrontOfItsSynonyms(): void
    {
        $topic    = $this->createTopic('own-option', ['segment.bytes' => '104857600']);
        $resource = ConfigResource::topic($topic);

        $config = $this->admin->describeConfigs([$resource], ['segment.bytes'], true)[$resource->key()];

        $entry = $config->get('segment.bytes');
        self::assertSame(ConfigSource::TOPIC_CONFIG, $entry->source);
        self::assertSame('104857600', $entry->value);
        self::assertFalse($entry->isDefault);
        self::assertSame(['segment.bytes' => '104857600'], $config->ownValues());

        self::assertCount(3, $entry->synonyms, 'its own value, and the two broker options it shadows');
        self::assertSame('segment.bytes', $entry->synonyms[0]->name);
        self::assertSame(ConfigSource::TOPIC_CONFIG, $entry->synonyms[0]->source);
        self::assertSame('104857600', $entry->synonyms[0]->value);
        self::assertSame('log.segment.bytes', $entry->synonyms[1]->name);
        self::assertSame(ConfigSource::STATIC_BROKER_CONFIG, $entry->synonyms[1]->source);
        self::assertSame(ConfigSource::DEFAULT_CONFIG, $entry->synonyms[2]->source);
    }

    public function testWithoutTheSynonymFlagTheAnswerCarriesTheSourceAndNoSynonym(): void
    {
        $topic    = $this->createTopic('no-synonyms', ['segment.bytes' => '104857600']);
        $resource = ConfigResource::topic($topic);

        $config = $this->admin->describeConfigs([$resource], ['segment.bytes'])[$resource->key()];

        self::assertSame(ConfigSource::TOPIC_CONFIG, $config->get('segment.bytes')->source);
        self::assertSame('104857600', $config->get('segment.bytes')->value);
        self::assertSame(
            [],
            $config->get('segment.bytes')->synonyms,
            'include_synonyms only decides whether the broker sends the list it built anyway'
        );
    }

    /**
     * A 2.8.2 broker still serves the version 0 - and its `is_default` boolean is **false for every option**
     *
     * Up to Kafka 1.1 the broker computed the boolean from the source it had derived (`is_default = (source ==
     * DEFAULT_CONFIG)`), so a version 0 answer still said which options nobody had configured. From Kafka 2.4 the
     * answer is built from the generated `DescribeConfigsResponseData`, and `ConfigHelper.createTopicConfigEntry()`
     * @ 2.8.2 sets the **source** alone - nothing ever calls `setIsDefault()`, so the field keeps the `false` of
     * the generator and every entry of a version 0 frame carries it, an option the topic itself set and an
     * untouched default alike.
     *
     * The client-side derivation {@see DescribeConfigsResponseConfigEntry::source()} therefore answers
     * `TOPIC_CONFIG` for every entry of a version 0 answer of this broker: the boolean has lost its meaning, and
     * a client that wants the source asks for version 1 or higher.
     */
    public function testTheVersionZeroAnswerOfATwoEightBrokerCarriesIsDefaultFalseForEveryOption(): void
    {
        $topic  = $this->createTopic('version-zero');
        $stream = $this->connect();

        new DescribeConfigsRequestV0(
            [new DescribeConfigsRequestResource(
                ConfigResource::TYPE_TOPIC,
                $topic,
                ['segment.bytes', 'retention.ms']
            )],
            't5-configs',
            42
        )->writeTo($stream);
        $response = DescribeConfigsResponseV0::unpack($stream);

        $entries = $response->resources[0]->configEntries;
        self::assertInstanceOf(DescribeConfigsResponseConfigEntryV0::class, $entries['segment.bytes']);
        self::assertFalse(
            $entries['segment.bytes']->isDefault,
            'the option is a STATIC_BROKER_CONFIG, which is not a default in any release'
        );
        self::assertFalse(
            $entries['retention.ms']->isDefault,
            'and neither is the untouched default of the topic: a 2.8.2 broker never sets the boolean at all'
        );
        self::assertSame([], $entries['segment.bytes']->configSynonyms, 'version 0 has no synonyms on the wire');
        self::assertSame(
            ConfigSource::TOPIC_CONFIG,
            $entries['segment.bytes']->source(ConfigResource::TYPE_TOPIC),
            'a client derives the source back from the boolean and the resource type, as the Java client does - '
            . 'and the derivation is lossy: the option is a STATIC_BROKER_CONFIG, which the boolean cannot say'
        );
        self::assertSame(
            ConfigSource::TOPIC_CONFIG,
            $entries['retention.ms']->source(ConfigResource::TYPE_TOPIC),
            'and on this broker it is wrong for every entry, because the boolean is always false'
        );
    }

    public function testAnOptionNameTheBrokerDoesNotKnowIsDroppedFromTheAnswer(): void
    {
        $topic    = $this->createTopic('unknown-name');
        $resource = ConfigResource::topic($topic);

        $config = $this->admin->describeConfigs([$resource], ['no.such.option'])[$resource->key()];

        self::assertSame([], $config->entries, 'there is no "unknown config" error in DescribeConfigs');
    }

    /**
     * A topic without a metadata entry is refused with 3, where a 1.1.1 broker answered the log defaults
     *
     * `AdminManager.describeConfigs()` @ 1.1.1 read the entity config of the topic out of ZooKeeper and merged it
     * with the log defaults of the broker, so a topic that had never been created was answered with the error code
     * 0 and the defaults - the api said nothing about existence at all. `ConfigHelper.describeConfigs()` @ 2.8.2
     * asks the metadata cache first (`if (metadataCache.contains(topic)) … else UNKNOWN_TOPIC_OR_PARTITION`), so
     * the resource now carries the error code **3** and an empty option array.
     */
    public function testATopicThatDoesNotExistIsRefusedWithThree(): void
    {
        $topic    = self::uniqueTopicName('t5-configs-never-created');
        $resource = ConfigResource::topic($topic);

        try {
            $this->admin->describeConfigs([$resource], ['retention.ms']);
            self::fail('a topic the broker does not know has to be refused');
        } catch (UnknownTopicOrPartitionException $exception) {
            self::assertSame($resource->key(), $exception->getContext()['resource']);
            self::assertSame('', $exception->getContext()['error'], 'the resource carries no error message');
        }

        self::assertNotContains($topic, $this->admin->listTopics(), 'and the topic was not created either');
    }

    public function testAnIllegalTopicNameIsRefusedWithSeventeen(): void
    {
        $this->expectException(InvalidTopicException::class);

        $this->admin->describeConfigs([ConfigResource::topic('t5 configs illegal name')]);
    }

    /**
     * `is_read_only` of a broker entry means "not dynamically updatable" since KIP-226 - **under its own name**
     *
     * Up to 0.11 every entry of a broker resource was read-only, because a broker could not change any of its own
     * configuration at runtime. `AdminManager.createBrokerConfigEntry()` @ 1.1.1 computed
     * `readOnly = !allNames.exists(DynamicBrokerConfig.AllDynamicConfigs.contains)` - the option itself **or one
     * of its synonyms** - so `log.retention.hours` was answered as writable because `log.retention.ms` is dynamic.
     *
     * `ConfigHelper.createBrokerConfigEntry()` @ 2.8.2 dropped the synonyms from that test:
     * `readOnly = !DynamicBrokerConfig.AllDynamicConfigs.contains(name)`. `log.retention.hours` is therefore
     * **read-only** on a 2.8.2 broker where a 1.1.1 broker called it writable, and only an option that is itself
     * one of the dynamic configs - `log.cleaner.backoff.ms`, which this suite also alters below - is not.
     */
    public function testABrokerEntryIsReadOnlyOnlyWhenItsOwnNameIsDynamicallyUpdatable(): void
    {
        $brokerId = array_key_first($this->admin->findAllBrokers());
        $resource = ConfigResource::broker($brokerId);

        $config = $this->admin->describeConfigs(
            [$resource],
            ['broker.id', 'log.retention.hours', self::DYNAMIC_OPTION, 'ssl.key.password'],
            true
        )[$resource->key()];

        self::assertSame((string) $brokerId, $config->value('broker.id'));
        self::assertTrue($config->get('broker.id')->isReadOnly, 'the id of a broker is never dynamic');
        self::assertTrue(
            $config->get('log.retention.hours')->isReadOnly,
            'the name itself is not in AllDynamicConfigs, and 2.8.2 no longer looks at its synonym log.retention.ms'
        );
        self::assertFalse(
            $config->get(self::DYNAMIC_OPTION)->isReadOnly,
            'while an option that is itself one of the dynamic configs of KIP-226 is writable'
        );
        self::assertSame(ConfigSource::STATIC_BROKER_CONFIG, $config->get('broker.id')->source);
        self::assertFalse($config->get('broker.id')->isDefault, 'it is in the server.properties of the container');
        self::assertSame(
            [ConfigSource::STATIC_BROKER_CONFIG, ConfigSource::DEFAULT_CONFIG],
            array_map(static fn(ConfigSynonym $synonym): int => $synonym->source, $config->get('broker.id')->synonyms),
            'the value of the server.properties, and the -1 of the built-in default behind it'
        );

        $password = $config->get('ssl.key.password');
        self::assertTrue($password->isSensitive);
        self::assertNull($password->value, 'the broker never sends the value of a PASSWORD option');
        self::assertNotSame([], $password->synonyms, 'the container sets it in its server.properties');
        self::assertNull($password->synonyms[0]->value, 'and its synonym carries no value either');
    }

    public function testABrokerIdThatIsNotTheOneThatAnswersIsRefused(): void
    {
        // The container runs a single broker, so an id that no broker has can only be answered by the wrong one
        try {
            $this->admin->describeConfigs([ConfigResource::broker(4242)], ['broker.id']);
            self::fail('a broker id that is not the one that answers has to be refused');
        } catch (InvalidRequestException $exception) {
            self::assertStringContainsString(
                'Unexpected broker id, expected 0 or empty string, but received',
                $exception->getMessage(),
                'the sentence of a 1.1 broker names the empty string of KIP-226 as well'
            );
        }
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
            ['retention.ms' => '3600000', 'cleanup.policy' => 'compact'],
            $this->ownValues($resource),
            'both options are the topic\'s own now'
        );

        // The second request names only one of them, and the other one falls back to the broker default: the api
        // REPLACES the ZooKeeper node of the topic instead of patching it
        $second = $this->admin->alterConfigs([$resource->key() => ['retention.ms' => '7200000']]);
        self::assertSame([$resource->key() => null], $second);
        self::assertSame(['retention.ms' => '7200000'], $this->ownValues($resource));

        // And an empty entry list resets every option of the topic
        self::assertSame([$resource->key() => null], $this->admin->alterConfigs([$resource->key() => []]));
        self::assertSame([], $this->ownValues($resource));
        self::assertSame(
            self::STATIC_BROKER_TOPIC_OPTIONS,
            $this->nonDefaults($resource),
            'what is left is what the broker configuration gives the topic'
        );
    }

    public function testValidateOnlyChangesNothing(): void
    {
        $topic    = $this->createTopic('validate');
        $resource = ConfigResource::topic($topic);

        $result = $this->admin->alterConfigs([$resource->key() => ['retention.ms' => '3600000']], true);

        self::assertSame([$resource->key() => null], $result, 'the request was valid');
        self::assertSame([], $this->ownValues($resource), 'and nothing was written to ZooKeeper');
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
        self::assertSame([], $this->ownValues($resource), 'and nothing was changed');
    }

    /**
     * Altering a topic that does not exist is the error code 3 of KIP-412, not the -1 of a 1.1.1 broker
     *
     * `AdminManager.alterConfigs()` @ 1.1.1 let the `UnknownTopicOrPartitionException` of `changeTopicConfig`
     * escape as an unexpected exception, which is the error code **-1** with the message of the exception.
     * `ZkAdminManager.alterTopicConfigs()` @ 2.8.2 throws the very same exception deliberately, before it touches
     * ZooKeeper, and `ApiError.fromThrowable` turns it into the error code **3** - with the same message.
     */
    public function testAlteringATopicThatDoesNotExistIsAnsweredWithThree(): void
    {
        $topic    = self::uniqueTopicName('t5-configs-missing');
        $resource = ConfigResource::topic($topic);

        $result = $this->admin->alterConfigs([$resource->key() => ['retention.ms' => '3600000']]);

        $error = $result[$resource->key()];
        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $error);
        self::assertStringContainsString("The topic '{$topic}' does not exist.", $error->getMessage());
    }

    /**
     * KIP-226: a dynamically updatable option of a broker resource is applied at runtime
     */
    public function testADynamicOptionOfABrokerResourceIsAppliedAndReportedWithItsSource(): void
    {
        $resource = ConfigResource::broker(array_key_first($this->admin->findAllBrokers()));

        try {
            $result = $this->admin->alterConfigs([$resource->key() => [self::DYNAMIC_OPTION => '16000']]);

            self::assertSame(
                [$resource->key() => null],
                $result,
                'a 0.11 broker refused every broker resource with 42 and "AlterConfigs is only supported for topics"'
            );

            // A dynamic broker option travels through the ZooKeeper watch of the broker, so the read-back can
            // lag the accepted write by a moment on a loaded or freshly started container: the describe is
            // repeated for a few hundred milliseconds before the assertion decides
            $entry    = null;
            $deadline = microtime(true) + self::DYNAMIC_OPTION_TIMEOUT;
            do {
                $entry = $this->admin->describeConfigs([$resource], [self::DYNAMIC_OPTION], true)[$resource->key()]
                    ->get(self::DYNAMIC_OPTION);
                if ($entry?->value === '16000') {
                    break;
                }
                usleep(50000);
            } while (microtime(true) < $deadline);

            self::assertNotNull($entry);
            self::assertSame('16000', $entry->value, 'the new value is live once the broker applied it');
            self::assertSame(ConfigSource::DYNAMIC_BROKER_CONFIG, $entry->source);
            self::assertFalse($entry->isDefault);
            self::assertFalse($entry->isReadOnly);
            self::assertSame(
                [ConfigSource::DYNAMIC_BROKER_CONFIG, ConfigSource::DEFAULT_CONFIG],
                array_map(static fn(ConfigSynonym $synonym): int => $synonym->source, $entry->synonyms),
                'the value that was set, and the built-in default it shadows'
            );
            self::assertSame(self::DYNAMIC_OPTION_DEFAULT, $entry->synonyms[1]->value);
            self::assertSame(
                [self::DYNAMIC_OPTION => '16000'],
                $this->ownValues($resource),
                'a dynamic broker config is the only thing a broker resource owns'
            );
        } finally {
            $this->restoreTheDynamicOption($resource);
        }
    }

    /**
     * KIP-226: the broker resource with an EMPTY name is the cluster-wide default
     */
    public function testTheDefaultBrokerResourceHoldsTheClusterWideConfiguration(): void
    {
        $broker  = ConfigResource::broker(array_key_first($this->admin->findAllBrokers()));
        $default = ConfigResource::defaultBroker();
        self::assertSame('broker:', $default->key(), 'the resource type 4 with an empty name');

        try {
            // The cluster-wide default resource is the one resource of the broker that a unique name cannot
            // separate: every suite on the shared container writes into the very same `broker:`, and an
            // `AlterConfigs` of it replaces the WHOLE resource, so another suite can drop this option between the
            // write and the read. The write is therefore repeated until the read sees it, the assertion is a
            // SUPERSET - this option with this value - instead of the exact key list, and the cleanup below
            // deletes that one option instead of replacing the resource.
            $entry = null;
            for ($attempt = 0; $attempt < 5 && $entry === null; $attempt++) {
                $result = $this->admin->alterConfigs([$default->key() => [self::DYNAMIC_OPTION => '17000']]);
                self::assertSame([$default->key() => null], $result);

                $configs = $this->admin->describeConfigs([$default], null, true);
                $entry   = $configs[$default->key()]->get(self::DYNAMIC_OPTION);
            }

            self::assertNotNull(
                $entry,
                'the default resource holds the dynamic default configuration, not the options of a broker'
            );
            self::assertSame('17000', $entry->value);
            self::assertSame(ConfigSource::DYNAMIC_DEFAULT_BROKER_CONFIG, $entry->source);

            $ofTheBroker = $this->admin->describeConfigs([$broker], [self::DYNAMIC_OPTION], true)[$broker->key()]
                ->get(self::DYNAMIC_OPTION);
            self::assertSame('17000', $ofTheBroker->value, 'every broker of the cluster picks the default up');
            self::assertSame(ConfigSource::DYNAMIC_DEFAULT_BROKER_CONFIG, $ofTheBroker->source);
        } finally {
            // IncrementalAlterConfigs (KIP-339) removes this one option and leaves every other option of the
            // shared default resource alone, where an AlterConfigs of an empty map would wipe all of them
            $this->admin->incrementalAlterConfigs(
                [$default->key() => [AlterConfigOp::delete(self::DYNAMIC_OPTION)]]
            );
            $this->restoreTheDynamicOption($broker);
        }
    }

    /**
     * A broker resource is refused as a WHOLE when one of its options is not dynamically updatable
     *
     * A 0.11.0.3 broker refused every AlterConfigs of a broker resource outright, with the message
     * `AlterConfigs is only supported for topics, but resource type is BROKER`. A 1.1 broker takes the request,
     * hands the entries to `DynamicBrokerConfig.validate()` and answers 42 with the names it cannot change at
     * runtime.
     */
    public function testAStaticOptionOfABrokerResourceIsRefusedPerOptionAndChangesNothing(): void
    {
        $resource = ConfigResource::broker(array_key_first($this->admin->findAllBrokers()));
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

    public function testASecurityOptionIsOnlyUpdatablePerBroker(): void
    {
        $default = ConfigResource::defaultBroker();

        $result = $this->admin->alterConfigs([
            $default->key() => ['ssl.keystore.location' => '/opt/kafka/ssl/broker.keystore.jks'],
        ]);

        $error = $result[$default->key()];
        self::assertInstanceOf(InvalidRequestException::class, $error);
        self::assertStringContainsString(
            'These security configs can be dynamically updated only per-listener using the listener prefix',
            $error->getMessage()
        );
        self::assertSame(
            [],
            $this->admin->describeConfigs([$default])[$default->key()]->entries,
            'and the cluster-wide default configuration is still empty'
        );
    }

    /**
     * Quirk: an option name the broker does not know is ACCEPTED for a broker resource
     *
     * `DynamicBrokerConfig.validate()` only checks the names it knows - the non-dynamic ones, the security prefixes
     * and the types - so a name that is no broker option at all passes the validation and is written into
     * `/config/brokers/<id>` of ZooKeeper. A topic is validated against `LogConfig` and answers 40 for the same
     * mistake.
     */
    public function testAnUnknownOptionNameOfABrokerResourceIsAccepted(): void
    {
        $resource = ConfigResource::broker(array_key_first($this->admin->findAllBrokers()));

        try {
            $result = $this->admin->alterConfigs([$resource->key() => ['no.such.broker.option' => '1']]);

            self::assertSame(
                [$resource->key() => null],
                $result,
                'the broker stores an option it has no idea about, where a topic answers 40'
            );
            $entry = $this->admin->describeConfigs([$resource], ['no.such.broker.option'], true)[$resource->key()]
                ->get('no.such.broker.option');
            self::assertNotNull($entry, 'and reports it back as a dynamic broker config of its own');
            self::assertSame(ConfigSource::DYNAMIC_BROKER_CONFIG, $entry->source);
            self::assertTrue(
                $entry->isSensitive,
                '`createBrokerConfigEntry()` cannot determine the type of an unknown option and treats it as a '
                . 'secret to be safe, which is why the value is null on the wire'
            );
            self::assertNull($entry->value);
            self::assertTrue($entry->isReadOnly, 'and no name of it is in AllDynamicConfigs');
        } finally {
            $this->admin->alterConfigs([$resource->key() => []]);
        }
    }

    /**
     * Puts the dynamic option back to the value the container starts with
     *
     * Removing the entry is NOT enough: a 1.1.1 broker keeps the last dynamic value in its live `KafkaConfig` when
     * the entry disappears from ZooKeeper (the source falls back to `DEFAULT_CONFIG` while the value does not), so
     * the documented default is written explicitly first and the entry is removed afterwards.
     */
    private function restoreTheDynamicOption(ConfigResource $resource): void
    {
        $this->admin->alterConfigs([$resource->key() => [self::DYNAMIC_OPTION => self::DYNAMIC_OPTION_DEFAULT]]);
        $this->admin->alterConfigs([$resource->key() => []]);
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
     * Returns the options the resource itself carries, i.e. the ones an AlterConfigs has to send back
     *
     * @return array<string, string|null>
     */
    private function ownValues(ConfigResource $resource): array
    {
        return $this->admin->describeConfigs([$resource])[$resource->key()]->ownValues();
    }

    /**
     * Creates a topic of one partition and remembers it for the cleanup
     *
     * @param array<string, string> $configs Topic-level options of the new topic
     */
    /**
     * The two fields KIP-569 gave every entry with the **version 3** of the api (Kafka 2.6)
     *
     * `config_type` is the `ConfigDef.Type` of the option and the broker fills it whatever the request asked for;
     * `documentation` is the prose of `ConfigDef.define(...)` and is only sent for `include_documentation = true`.
     */
    public function testTheVersionThreeAnswersTheTypeOfEveryOptionAndItsDocumentationOnDemand(): void
    {
        $topic = $this->createTopic('type');
        $key   = ConfigResource::topic($topic)->key();

        // One option at a time: the documentation of a whole resource is tens of kilobytes of prose
        $documented = $this->admin->describeConfigs(
            [ConfigResource::topic($topic)],
            ['cleanup.policy'],
            false,
            true
        )[$key];
        $entry = $documented->get('cleanup.policy');

        self::assertInstanceOf(ConfigEntry::class, $entry);
        self::assertSame(ConfigType::LIST, $entry->type, '`cleanup.policy` is a LIST - the type APPEND accepts');
        self::assertNotNull($entry->documentation, 'the request asked for it');
        self::assertStringContainsString(
            'either "delete" or "compact"',
            (string) $entry->documentation,
            'the documentation of `LogConfig` @ 2.8.2'
        );

        // The very same request without the flag: the type is still filled, the documentation is not
        $plain = $this->admin->describeConfigs([ConfigResource::topic($topic)], ['cleanup.policy'])[$key];

        self::assertSame(ConfigType::LIST, $plain->get('cleanup.policy')->type, 'the type is unconditional');
        self::assertNull($plain->get('cleanup.policy')->documentation, 'the documentation is not');
        self::assertGreaterThanOrEqual(
            3,
            DescribeConfigsRequest::VERSION,
            'the documentation arrived with the version 3 and every version above it carries it'
        );
    }

    /**
     * Every `ConfigDef.Type` this container can show, in one request
     */
    public function testTheTypeOfAnOptionIsTheOneOfItsConfigDef(): void
    {
        $topic = $this->createTopic('types');
        $key   = ConfigResource::topic($topic)->key();

        $config = $this->admin->describeConfigs(
            [ConfigResource::topic($topic)],
            ['retention.ms', 'preallocate', 'compression.type', 'min.insync.replicas', 'cleanup.policy']
        )[$key];

        self::assertSame(ConfigType::LONG, $config->get('retention.ms')->type);
        self::assertSame(ConfigType::BOOLEAN, $config->get('preallocate')->type);
        self::assertSame(ConfigType::STRING, $config->get('compression.type')->type);
        self::assertSame(ConfigType::INT, $config->get('min.insync.replicas')->type);
        self::assertSame(ConfigType::LIST, $config->get('cleanup.policy')->type);
    }

    /**
     * A sensitive broker option is a `PASSWORD`, and its value is null as it always was
     */
    public function testASensitiveOptionIsAPasswordWhoseValueIsStillNull(): void
    {
        $resource = ConfigResource::broker(array_key_first($this->admin->findAllBrokers()));
        $config   = $this->admin->describeConfigs([$resource], ['ssl.key.password'], true, true)[$resource->key()];
        $entry    = $config->get('ssl.key.password');

        self::assertSame(ConfigType::PASSWORD, $entry->type);
        self::assertTrue($entry->isSensitive);
        self::assertNull($entry->value, 'the broker never sends the value of a sensitive option');
        self::assertNotNull($entry->documentation, 'but it does send its documentation');
    }

    /**
     * An answer of a version below 3 carries neither field, and the client reads the documented defaults
     */
    public function testAnAnswerOfTheVersionTwoCarriesNeitherTheTypeNorTheDocumentation(): void
    {
        $topic  = $this->createTopic('no-type');
        $nodes  = $this->cluster->nodes();
        $stream = reset($nodes)->getConnection($this->configuration());

        new DescribeConfigsRequestV2(
            [DescribeConfigsRequestResource::fromConfigResource(
                ConfigResource::topic($topic),
                ['cleanup.policy']
            )],
            false,
            true,
            't5-configs',
            801
        )->writeTo($stream);

        $answer = DescribeConfigsResponseV2::unpack($stream);
        $entry  = $answer->resources[0]->configEntries['cleanup.policy'];

        self::assertSame(KafkaException::NO_ERROR, $answer->resources[0]->errorCode);
        self::assertSame('delete', $entry->configValue);
        self::assertSame(ConfigType::UNKNOWN, $entry->configType, 'no type byte in the frame at all');
        self::assertNull($entry->documentation, 'and no documentation either, although the flag was set');
    }

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
