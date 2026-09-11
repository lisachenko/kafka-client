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
use Protocol\Kafka\Admin\ClusterDescription;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Protocol\Request\DescribeClusterRequest;
use Protocol\Kafka\Protocol\Request\DescribeClusterResponse;

/**
 * Exercises DescribeCluster (key 60, v0) of KIP-700 against a real Kafka 2.8.2 broker.
 *
 * The api carries nothing a Metadata answer did not already carry - the cluster id, the controller and the
 * brokers - and that is the point of it: until Kafka 2.8 a client that wanted those three had to send a request
 * about *topics* with an empty topic array. This class asserts that both routes answer the same thing, and
 * measures the one field that is new, the acl bit field of KIP-430.
 *
 * It creates nothing on the broker and therefore has nothing to clean up.
 *
 * @see docs/protocol/2.8.md, section "DescribeCluster API (key 60, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeClusterRequest::class)]
#[CoversClass(DescribeClusterResponse::class)]
#[CoversClass(ClusterDescription::class)]
final class DescribeClusterApiTest extends IntegrationTestCase
{
    /**
     * `AclEntry.supportedOperations(CLUSTER)` @ 2.8.2 on a broker that runs without an authorizer: the bits 5, 7,
     * 8, 9, 10, 11 and 12 of `AclOperation` - CREATE, ALTER, DESCRIBE, CLUSTER_ACTION, DESCRIBE_CONFIGS,
     * ALTER_CONFIGS and IDEMPOTENT_WRITE
     */
    private const int ALL_CLUSTER_OPERATIONS = 8096;

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $configuration = $this->configuration();
        $this->admin   = new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * The api answers what a Metadata request answers, without naming a topic
     */
    public function testItAnswersTheSameClusterAsAMetadataRequest(): void
    {
        $described = $this->admin->describeCluster();
        $metadata  = $this->admin->describeClusterFromMetadata();

        self::assertNotSame('', $described->clusterId);
        self::assertSame($metadata->clusterId, $described->clusterId);
        self::assertSame($metadata->controllerId, $described->controllerId);
        self::assertSame(array_keys($metadata->nodes), array_keys($described->nodes));

        $broker = $described->nodes[$described->controllerId] ?? null;

        self::assertNotNull($broker, 'the controller is one of the brokers of the answer');
        self::assertSame($broker, $described->controller());
        self::assertNotSame('', $broker->host);
        self::assertGreaterThan(0, $broker->port);
    }

    /**
     * Without the flag the acl bit field is `Integer.MIN_VALUE`, with it the operations the caller may perform
     */
    public function testTheAclBitFieldIsOnlyAnsweredWhenItIsAskedFor(): void
    {
        $silent  = $this->admin->describeCluster();
        $asking  = $this->admin->describeCluster(true);

        self::assertFalse($silent->hasAuthorizedOperations());
        self::assertSame(
            ClusterDescription::OPERATIONS_NOT_REQUESTED,
            $silent->authorizedOperations,
            'the default of the specification, not an error'
        );

        self::assertTrue($asking->hasAuthorizedOperations());
        self::assertSame(
            self::ALL_CLUSTER_OPERATIONS,
            $asking->authorizedOperations,
            'a broker without an authorizer allows every operation AclEntry.supportedOperations(CLUSTER) names'
        );
    }

    /**
     * The request is the smallest of this protocol: the two frames differ in exactly one byte
     */
    public function testTheTwoRequestsDifferInOneByte(): void
    {
        $silent = str_split((string) new DescribeClusterRequest(false, 'kafka-client-t1-cluster', 1));
        $asking = str_split((string) new DescribeClusterRequest(true, 'kafka-client-t1-cluster', 1));

        self::assertCount(count($silent), $asking, 'a boolean is one byte in both frames');

        $differing = array_keys(array_filter(
            $asking,
            static fn(string $byte, int $offset): bool => $byte !== $silent[$offset],
            ARRAY_FILTER_USE_BOTH
        ));

        self::assertCount(1, $differing, 'the flag is the only field of the request body');
        self::assertSame("\x00", $silent[$differing[0]]);
        self::assertSame("\x01", $asking[$differing[0]]);
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 'kafka-client-t1-cluster',
            ClientConfig::REQUEST_TIMEOUT_MS        => 20000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}
