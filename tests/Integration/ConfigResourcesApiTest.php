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
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\ClusterAuthorizationFailedException;
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Protocol\Data\ClientMetricsResource;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesRequest;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesRequestV0;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesResponse;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesResponseV0;

/**
 * Exercises the api key 74 at the version 1 of KIP-1142 (Kafka 4.1), where it became `ListConfigResources`.
 *
 * The answer names the configuration resources of the types the request asks for - the topics and the brokers of
 * the metadata cache of the answering broker, its broker loggers, the client-metrics subscriptions and the groups
 * with a configuration of their own - and an empty type list is every type the broker supports. The class creates
 * one topic, which `tearDownAfterClass()` deletes again; the other agents of the node create and delete topics of
 * their own while it runs, so every assertion about topics is one about this class's topic alone.
 *
 * @see docs/protocol/4.3.md, section "The config resources of KIP-1142 (v1)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(ListClientMetricsResourcesRequest::class)]
#[CoversClass(ListClientMetricsResourcesRequestV0::class)]
#[CoversClass(ListClientMetricsResourcesResponse::class)]
#[CoversClass(ListClientMetricsResourcesResponseV0::class)]
#[CoversClass(ClientMetricsResource::class)]
final class ConfigResourcesApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t1-41-config-resources';

    private static ?string $topic = null;

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $this->admin = new AdminClient(Cluster::bootstrap($this->configuration()), $this->configuration());
        if (self::$topic === null) {
            self::$topic = self::uniqueTopicName('t1-41-config-resources');
            $this->admin->createTopics([new NewTopic(self::$topic, 1, 1)]);
            self::topicIdOf(self::$topic);
        }
    }

    public static function tearDownAfterClass(): void
    {
        self::$topic = null;

        parent::tearDownAfterClass();
    }

    /**
     * The brokers are listed once per type: the broker and its loggers are two resources of the same name
     */
    public function testABrokerIsListedOncePerType(): void
    {
        $resources = $this->admin->listConfigResources([
            ListClientMetricsResourcesRequest::RESOURCE_TYPE_BROKER,
            ListClientMetricsResourcesRequest::RESOURCE_TYPE_BROKER_LOGGER,
        ]);

        self::assertEquals(
            [
                new ConfigResource(ListClientMetricsResourcesRequest::RESOURCE_TYPE_BROKER_LOGGER, '1'),
                new ConfigResource(ListClientMetricsResourcesRequest::RESOURCE_TYPE_BROKER, '1'),
            ],
            $resources,
            'the node 1, as a broker logger and as a broker, in the order of `KafkaApis.handleListConfigResources`'
        );
    }

    /**
     * An empty type list is every type the broker supports, which includes the topic of this class
     */
    public function testAnEmptyTypeListIsEveryType(): void
    {
        $resources = $this->admin->listConfigResources();
        $types     = array_unique(array_map(static fn(ConfigResource $resource): int => $resource->type, $resources));

        self::assertContainsEquals(
            new ConfigResource(ListClientMetricsResourcesRequest::RESOURCE_TYPE_TOPIC, (string) self::$topic),
            $resources
        );
        self::assertContains(ListClientMetricsResourcesRequest::RESOURCE_TYPE_BROKER, $types);
        self::assertContains(ListClientMetricsResourcesRequest::RESOURCE_TYPE_BROKER_LOGGER, $types);
        self::assertContainsEquals(
            new ConfigResource(ListClientMetricsResourcesRequest::RESOURCE_TYPE_TOPIC, (string) self::$topic),
            $this->admin->listConfigResources([ListClientMetricsResourcesRequest::RESOURCE_TYPE_TOPIC])
        );
    }

    /**
     * A group configuration of this test class is listed under the group type 32, and only the asked types come back
     *
     * A fresh node has no client-metrics subscription and no group configuration at all, and a list of these two
     * types is then empty - which asserted nothing on the node of CI. The test writes one group configuration
     * (`consumer.session.timeout.ms` of a group nobody uses) with the tool of the container, polls the answer until
     * the controller's write has reached the broker, and removes the configuration again.
     */
    public function testTheSubscriptionsAndTheGroupConfigurationsAreTheirOwnTypes(): void
    {
        $group = 't1-41-config-group-' . bin2hex(random_bytes(6));
        self::groupConfig($group, '--add-config consumer.session.timeout.ms=45000');

        try {
            $expected = new ConfigResource(ListClientMetricsResourcesRequest::RESOURCE_TYPE_GROUP, $group);
            $deadline = microtime(true) + 30.0;
            do {
                $resources = $this->admin->listConfigResources([
                    ListClientMetricsResourcesRequest::RESOURCE_TYPE_CLIENT_METRICS,
                    ListClientMetricsResourcesRequest::RESOURCE_TYPE_GROUP,
                ]);
                if (in_array($expected, $resources, false)) {
                    break;
                }
                usleep(250000);
            } while (microtime(true) < $deadline);

            self::assertContainsEquals($expected, $resources, 'the group configuration written for this test');
            foreach ($resources as $resource) {
                self::assertContains(
                    $resource->type,
                    [
                        ListClientMetricsResourcesRequest::RESOURCE_TYPE_CLIENT_METRICS,
                        ListClientMetricsResourcesRequest::RESOURCE_TYPE_GROUP,
                    ]
                );
            }
        } finally {
            self::groupConfig($group, '--delete-config consumer.session.timeout.ms');
        }
    }

    /**
     * A type the broker does not support is the 35, including the group type 3 of the ACL resource types
     */
    public function testATypeTheBrokerDoesNotSupportIsUnsupportedVersion(): void
    {
        foreach ([0, ConfigResource::TYPE_GROUP, 99] as $type) {
            try {
                $this->admin->listConfigResources([$type]);
                self::fail("the type {$type} is not a config resource type");
            } catch (UnsupportedVersionException $exception) {
                self::assertSame([$type], $exception->getContext()['resourceTypes']);
            }
        }
    }

    /**
     * The version 0 frame asks for the client-metrics subscriptions alone, keyed by their name
     */
    public function testTheVersionZeroFrameListsTheSubscriptionsAlone(): void
    {
        $stream = $this->connect();
        new ListClientMetricsResourcesRequestV0(self::CLIENT_ID, 4911)->writeTo($stream);
        $below = ListClientMetricsResourcesResponseV0::unpack($stream);

        new ListClientMetricsResourcesRequest(
            self::CLIENT_ID,
            4912,
            [ListClientMetricsResourcesRequest::RESOURCE_TYPE_CLIENT_METRICS]
        )->writeTo($stream);
        $above = ListClientMetricsResourcesResponse::unpack($stream);

        foreach ($below->clientMetricsResources as $name => $resource) {
            self::assertSame($name, $resource->name, 'a version 0 entry is keyed by its name');
            self::assertSame(ListClientMetricsResourcesRequest::RESOURCE_TYPE_CLIENT_METRICS, $resource->resourceType);
        }
        self::assertSame(array_values($above->clientMetricsResources), $above->clientMetricsResources, 'a list');

        $stream->disconnect();
    }

    /**
     * A principal without `DESCRIBE_CONFIGS` on the cluster is refused with the 31
     */
    public function testAPrincipalThatMayNotDescribeTheClusterIsRefused(): void
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::saslBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
            ClientConfig::SECURITY_PROTOCOL         => SecurityProtocol::SASL_PLAINTEXT,
            ClientConfig::SASL_MECHANISM            => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME             => 'acltest',
            ClientConfig::SASL_PASSWORD             => 'acltest-secret',
        ];

        $this->expectException(ClusterAuthorizationFailedException::class);

        new AdminClient(Cluster::bootstrap($configuration), $configuration)->listConfigResources();
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }

    /**
     * Alters the configuration of a group with `kafka-configs.sh` in the container of the node
     */
    private static function groupConfig(string $group, string $alteration): void
    {
        $container = getenv('KAFKA_CONTAINER');
        $container = $container === false || trim($container) === '' ? 'kafka-4-3-1' : trim($container);

        exec(
            sprintf(
                'docker exec %s /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter'
                . ' --entity-type groups --entity-name %s %s 2>&1',
                escapeshellarg($container),
                escapeshellarg($group),
                $alteration
            ),
            $output,
            $exitCode
        );
        self::assertSame(0, $exitCode, "kafka-configs.sh could not alter the group {$group}: " . implode("\n", $output));
    }
}
