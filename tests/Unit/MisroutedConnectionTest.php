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

namespace Protocol\Kafka\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\RebootstrapRequiredException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * Tests what the clients do with the cluster id and node id of ApiVersions v5 (Kafka 4.4, KIP-1242), against scripted
 * brokers: both ids travel in the request whenever they are known, and the 129 `REBOOTSTRAP_REQUIRED` of a broker
 * that is not the node the metadata names at its address drops the connection, reloads the cluster from its
 * bootstrap servers and is thrown as {@see RebootstrapRequiredException}.
 *
 * @see docs/protocol/4.4.md, section "ApiVersions API (key 18, v0 to v5)"
 */
#[CoversClass(Client::class)]
#[CoversClass(AdminClient::class)]
#[CoversClass(Cluster::class)]
#[CoversClass(ApiVersionsRequest::class)]
final class MisroutedConnectionTest extends TestCase
{
    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    private const string FIRST_BROKER = 'tcp://kafka-1:9092';

    private const string SECOND_BROKER = 'tcp://kafka-2:9093';

    /**
     * Where the node 0 lives in the metadata a rebootstrap answers: the old address now leads to another broker
     */
    private const string MOVED_BROKER = 'tcp://kafka-9:9092';

    /**
     * The 129 a 4.4.0 node answers a v5 request whose ids are not its own: the layout of the version 5, empty api array
     */
    private const string REBOOTSTRAP_REQUIRED_BODY = '0081' . '01' . '00000000' . '00';

    private ScriptedConnections $brokers;

    /**
     * Cluster the client of the test was built on
     */
    private Cluster $cluster;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testTheRequestNamesTheClusterIdOfTheMetadataAndTheNodeItIsSentTo(): void
    {
        $broker = new BrokerConnection(self::apiVersions());
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection(self::clusterMetadata()))
            ->on(self::SECOND_BROKER, $broker)
            ->install();
        $client = $this->client();

        $answer = $client->apiVersions($this->cluster->nodeById(1) ?? self::fail('no node 1'));

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        $frame = $broker->getReceivedFrames()[0];
        self::assertSame('00120005', bin2hex(substr($frame, 0, 4)), 'ApiVersions v5');
        self::assertStringEndsWith(
            '17' . bin2hex(ResponseFrame::CLUSTER_ID) . '00000001' . '00',
            bin2hex($frame),
            'the compact cluster id of the metadata, the node id 1 and the tag buffer of the body'
        );
        self::assertSame(
            bin2hex($frame),
            bin2hex(substr((string) new ApiVersionsRequest(
                't1-misrouted',
                $broker->getReceivedCorrelationIds()[0],
                ResponseFrame::CLUSTER_ID,
                1
            ), 4))
        );
    }

    public function testANodeWithoutAnIdIsAskedWithNeitherId(): void
    {
        $broker = new BrokerConnection(self::apiVersions());
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection(self::clusterMetadata()))
            ->on(self::FIRST_BROKER, $broker)
            ->install();

        $bootstrapNode         = new Node();
        $bootstrapNode->nodeId = -1;
        $bootstrapNode->host   = 'kafka-1';
        $bootstrapNode->port   = 9092;
        $this->client()->apiVersions($bootstrapNode);

        self::assertStringEndsWith(
            '00' . 'ffffffff' . '00',
            bin2hex($broker->getReceivedFrames()[0]),
            'half of the pair would be answered the 42, so a negative node id sends the null cluster id and -1'
        );
    }

    /**
     * The 129 makes the cluster start over from its bootstrap servers, and the next call reaches the node where it is
     */
    public function testARebootstrapRequiredReloadsTheClusterAndIsThrown(): void
    {
        $misrouted = new BrokerConnection(self::rebootstrapRequired());
        $moved     = new BrokerConnection(self::apiVersions());
        $this->brokers
            ->on(
                self::BOOTSTRAP_ADDRESS,
                new BrokerConnection(self::clusterMetadata()),
                new BrokerConnection(self::clusterMetadata('kafka-9'))
            )
            ->on(self::FIRST_BROKER, $misrouted)
            ->on(self::MOVED_BROKER, $moved)
            ->install();
        $client  = $this->client();
        $cluster = $this->cluster;

        try {
            $client->apiVersions($cluster->nodeById(0) ?? self::fail('no node 0'));
            self::fail('a 129 must not be returned as an answer');
        } catch (RebootstrapRequiredException $expected) {
            self::assertSame(KafkaException::REBOOTSTRAP_REQUIRED, $expected->getCode());
            self::assertSame(0, $expected->getContext()['node']);
            self::assertSame('kafka-1:9092', $expected->getContext()['address']);
            self::assertSame(ResponseFrame::CLUSTER_ID, $expected->getContext()['clusterId']);
        }

        self::assertSame(2, $this->brokers->getConnectionCount(self::BOOTSTRAP_ADDRESS), 'the rebootstrap');
        self::assertSame('kafka-9', $cluster->nodeById(0)?->host, 'the metadata of the bootstrap servers is taken');

        $answer = $client->apiVersions($cluster->nodeById(0) ?? self::fail('no node 0'));

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertSame(1, $misrouted->getRequestCount(), 'the misrouted connection is not asked again');
        self::assertSame(1, $moved->getRequestCount());
    }

    /**
     * The connection that answered the 129 is dropped: the next request to the address opens a new one
     */
    public function testTheConnectionThatAnsweredTheRebootstrapRequiredIsDropped(): void
    {
        $this->brokers
            ->on(
                self::BOOTSTRAP_ADDRESS,
                new BrokerConnection(self::clusterMetadata()),
                new BrokerConnection(self::clusterMetadata())
            )
            ->on(
                self::FIRST_BROKER,
                new BrokerConnection(self::rebootstrapRequired()),
                new BrokerConnection(self::apiVersions())
            )
            ->install();
        $client = $this->client();
        $node   = $this->cluster->nodeById(0) ?? self::fail('no node 0');

        try {
            $client->apiVersions($node);
            self::fail('a 129 must be thrown');
        } catch (RebootstrapRequiredException) {
            // the address may lead to the right broker on a new connection - a load balancer, a reused address
        }
        $answer = $client->apiVersions($node);

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertSame(2, $this->brokers->getConnectionCount(self::FIRST_BROKER));
    }

    /**
     * A rebootstrap that no bootstrap server answers keeps the metadata there was, and the 129 is thrown all the same
     */
    public function testARebootstrapThatHearsNothingKeepsTheMetadata(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection(self::clusterMetadata()))
            ->on(self::FIRST_BROKER, new BrokerConnection(self::rebootstrapRequired()))
            ->install();
        $client  = $this->client();
        $cluster = $this->cluster;

        try {
            $client->apiVersions($cluster->nodeById(0) ?? self::fail('no node 0'));
            self::fail('a 129 must be thrown');
        } catch (RebootstrapRequiredException $expected) {
            self::assertSame(KafkaException::REBOOTSTRAP_REQUIRED, $expected->getCode());
        }

        self::assertSame([0, 1], array_keys($cluster->nodes()), 'the metadata the cluster had');
        self::assertSame(2, $this->brokers->getConnectionCount(self::BOOTSTRAP_ADDRESS), 'the rebootstrap was tried');
    }

    public function testTheAdminClientNamesBothIdsAndThrowsTheRebootstrapRequired(): void
    {
        $misrouted = new BrokerConnection(self::rebootstrapRequired());
        $this->brokers
            ->on(
                self::BOOTSTRAP_ADDRESS,
                new BrokerConnection(self::clusterMetadata()),
                new BrokerConnection(self::clusterMetadata('kafka-9'))
            )
            ->on(self::FIRST_BROKER, $misrouted)
            ->install();
        $configuration = self::configuration();
        $cluster       = Cluster::bootstrap($configuration);
        $admin         = new AdminClient($cluster, $configuration);

        try {
            $admin->getApiVersions($cluster->nodeById(0) ?? self::fail('no node 0'));
            self::fail('a 129 must be thrown');
        } catch (RebootstrapRequiredException $expected) {
            self::assertSame(0, $expected->getContext()['node']);
        }

        self::assertStringEndsWith(
            '17' . bin2hex(ResponseFrame::CLUSTER_ID) . '00000000' . '00',
            bin2hex($misrouted->getReceivedFrames()[0]),
            'the cluster id and the node id 0'
        );
        self::assertSame('kafka-9', $cluster->nodeById(0)?->host, 'the cluster was rebootstrapped');
    }

    /**
     * describeFeatures() asks the nodes in turn, but a 129 ends the search: the nodes of the loop are stale
     */
    public function testDescribeFeaturesStopsAtTheRebootstrapRequired(): void
    {
        $second = new BrokerConnection(self::apiVersions());
        $this->brokers
            ->on(
                self::BOOTSTRAP_ADDRESS,
                new BrokerConnection(self::clusterMetadata()),
                new BrokerConnection(self::clusterMetadata())
            )
            ->on(self::FIRST_BROKER, new BrokerConnection(self::rebootstrapRequired()))
            ->on(self::SECOND_BROKER, $second)
            ->install();
        $configuration = self::configuration();
        $admin         = new AdminClient(Cluster::bootstrap($configuration), $configuration);

        try {
            $admin->describeFeatures();
            self::fail('a 129 must be thrown');
        } catch (RebootstrapRequiredException $expected) {
            self::assertSame(0, $expected->getContext()['node']);
        }

        self::assertSame(0, $second->getRequestCount(), 'the second node of the stale metadata is not asked');
    }

    public function testDescribeFeaturesNamesTheNodeItAsks(): void
    {
        $first = new BrokerConnection(self::apiVersions());
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection(self::clusterMetadata()))
            ->on(self::FIRST_BROKER, $first)
            ->install();
        $configuration = self::configuration();

        $features = new AdminClient(Cluster::bootstrap($configuration), $configuration)->describeFeatures();

        self::assertSame(-1, $features->finalizedFeaturesEpoch);
        self::assertStringEndsWith(
            '17' . bin2hex(ResponseFrame::CLUSTER_ID) . '00000000' . '00',
            bin2hex($first->getReceivedFrames()[0])
        );
    }

    /**
     * A cluster of two brokers, node 0 at `$firstHost`:9092 and node 1 at kafka-2:9093
     */
    private static function clusterMetadata(string $firstHost = 'kafka-1'): string
    {
        return ResponseFrame::metadata(0, [[0, $firstHost, 9092], [1, 'kafka-2', 9093]]);
    }

    /**
     * The 129 answer, with a placeholder correlation id that {@see BrokerConnection} replaces
     */
    private static function rebootstrapRequired(): string
    {
        return ResponseFrame::of(0, (string) hex2bin(self::REBOOTSTRAP_REQUIRED_BODY));
    }

    /**
     * An ApiVersions v5 answer of a node that serves the four record apis and ApiVersions itself
     */
    private static function apiVersions(): string
    {
        return ResponseFrame::apiVersions(0, [
            ApiKeys::PRODUCE      => [0, 13],
            ApiKeys::FETCH        => [4, 18],
            ApiKeys::OFFSETS      => [1, 11],
            ApiKeys::METADATA     => [0, 13],
            ApiKeys::API_VERSIONS => [0, 5],
        ]);
    }

    /**
     * Builds a client on a cluster that the scripted brokers answer for, and keeps the cluster in `$this->cluster`
     */
    private function client(): Client
    {
        $configuration = self::configuration();
        $this->cluster = Cluster::bootstrap($configuration);

        return new Client($this->cluster, $configuration);
    }

    /**
     * @return array<string, mixed>
     */
    private static function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't1-misrouted',
            ClientConfig::REQUEST_TIMEOUT_MS        => 500,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::METADATA_MAX_AGE_MS       => 300000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
            ClientConfig::RETRIES                   => 0,
        ];
    }
}
