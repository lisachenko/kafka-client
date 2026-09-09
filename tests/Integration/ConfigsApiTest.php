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
 * Exercises the DescribeConfigs (key 32) and AlterConfigs (key 33) apis against a real Kafka 0.11.0.3 broker.
 *
 * Both arrived with Kafka 0.11 (KIP-133). Nothing here changes a **broker**-level setting: a 0.11 broker refuses
 * every AlterConfigs of a broker resource, which is one of the assertions below, and the container is shared with
 * the other suites of this branch.
 *
 * @see docs/protocol/0.11.0.md, sections "DescribeConfigs API (key 32, v0)" and "AlterConfigs API (key 33, v0)"
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

    public function testTheWholeConfigurationOfAFreshTopicIsMadeOfDefaults(): void
    {
        $topic    = $this->createTopic('describe');
        $resource = ConfigResource::topic($topic);

        $configs = $this->admin->describeConfigs([$resource]);

        self::assertSame([$resource->key()], array_keys($configs));
        $config = $configs[$resource->key()];
        self::assertGreaterThan(20, count($config->entries), 'a 0.11 topic has more than twenty options');
        self::assertSame([], $config->nonDefaultValues(), 'a topic created without options has none of its own');
        self::assertSame('604800000', $config->value('retention.ms'), 'the broker default of seven days');
        self::assertFalse($config->get('retention.ms')->isReadOnly, 'a topic option can be altered');
        self::assertFalse($config->get('retention.ms')->isSensitive);
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

    public function testTheBrokerResourceIsReadOnlyAndHidesItsSecrets(): void
    {
        $brokerId = array_key_first($this->admin->findAllBrokers());
        $resource = ConfigResource::broker($brokerId);

        $config = $this->admin->describeConfigs(
            [$resource],
            ['broker.id', 'log.retention.hours', 'ssl.keystore.password']
        )[$resource->key()];

        self::assertSame((string) $brokerId, $config->value('broker.id'));
        foreach ($config->entries as $entry) {
            self::assertTrue($entry->isReadOnly, 'a 0.11 broker can not change its own configuration at runtime');
        }
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
            ['retention.ms' => '3600000', 'cleanup.policy' => 'compact'],
            $this->nonDefaults($resource),
            'both options are now the topic\'s own'
        );

        // The second request names only one of them, and the other one falls back to the broker default: the api
        // REPLACES the ZooKeeper node of the topic instead of patching it
        $second = $this->admin->alterConfigs([$resource->key() => ['retention.ms' => '7200000']]);
        self::assertSame([$resource->key() => null], $second);
        self::assertSame(['retention.ms' => '7200000'], $this->nonDefaults($resource));

        // And an empty entry list resets every option of the topic
        self::assertSame([$resource->key() => null], $this->admin->alterConfigs([$resource->key() => []]));
        self::assertSame([], $this->nonDefaults($resource));
    }

    public function testValidateOnlyChangesNothing(): void
    {
        $topic    = $this->createTopic('validate');
        $resource = ConfigResource::topic($topic);

        $result = $this->admin->alterConfigs([$resource->key() => ['retention.ms' => '3600000']], true);

        self::assertSame([$resource->key() => null], $result, 'the request was valid');
        self::assertSame([], $this->nonDefaults($resource), 'and nothing was written to ZooKeeper');
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
        self::assertSame([], $this->nonDefaults($resource), 'and nothing was changed');
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

    public function testABrokerResourceIsRefusedAndTheBrokerKeepsItsConfiguration(): void
    {
        $brokerId = array_key_first($this->admin->findAllBrokers());
        $resource = ConfigResource::broker($brokerId);
        $before   = $this->admin->describeConfigs([$resource], ['log.retention.hours'])[$resource->key()];

        $result = $this->admin->alterConfigs([$resource->key() => ['log.retention.hours' => '169']]);

        $error = $result[$resource->key()];
        self::assertInstanceOf(InvalidRequestException::class, $error);
        self::assertStringContainsString(
            'AlterConfigs is only supported for topics, but resource type is BROKER',
            $error->getMessage()
        );

        $after = $this->admin->describeConfigs([$resource], ['log.retention.hours'])[$resource->key()];
        self::assertSame(
            $before->value('log.retention.hours'),
            $after->value('log.retention.hours'),
            'a 0.11 broker changes nothing at all for a broker resource - dynamic broker configs are Kafka 1.1'
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
