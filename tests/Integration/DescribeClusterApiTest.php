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
use Protocol\Kafka\Admin\EndpointType;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\MismatchedEndpointTypeException;
use Protocol\Kafka\Common\Errors\UnsupportedEndpointTypeException;
use Protocol\Kafka\Protocol\Request\DescribeClusterRequest;
use Protocol\Kafka\Protocol\Request\DescribeClusterRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeClusterRequestV1;
use Protocol\Kafka\Protocol\Request\DescribeClusterResponse;
use Protocol\Kafka\Protocol\Request\DescribeClusterResponseV0;
use Protocol\Kafka\Protocol\Request\DescribeClusterResponseV1;

/**
 * Exercises DescribeCluster (key 60, v0 to v2) of KIP-700, KIP-919 and KIP-1073 against the 4.3.1 KRaft node.
 *
 * The api carries nothing a Metadata answer did not already carry - the cluster id, the controller and the
 * brokers - and that is the point of it: until Kafka 2.8 a client that wanted those three had to send a request
 * about *topics* with an empty topic array. This class asserts that both routes answer the same thing, and
 * measures the one field that is new, the acl bit field of KIP-430, and the `endpoint_type` that Kafka 3.7
 * appended to the version 1 - which half of a KRaft cluster the answer describes, and the two error codes 114 and
 * 115 that a server answers when the byte is one it will not serve - and the `include_fenced_brokers` of the
 * version 2 (Kafka 4.0), which a one-node cluster answers with its one broker, not fenced, because a fenced broker
 * is a registered broker that has lost its lease and the only broker of this node serves the request itself.
 *
 * It creates nothing on the broker and therefore has nothing to clean up.
 *
 * @see docs/protocol/4.3.md, section "DescribeCluster API (key 60, v0 to v2)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeClusterRequest::class)]
#[CoversClass(DescribeClusterResponse::class)]
#[CoversClass(DescribeClusterRequestV0::class)]
#[CoversClass(DescribeClusterResponseV0::class)]
#[CoversClass(DescribeClusterRequestV1::class)]
#[CoversClass(DescribeClusterResponseV1::class)]
#[CoversClass(EndpointType::class)]
#[CoversClass(ClusterDescription::class)]
final class DescribeClusterApiTest extends IntegrationTestCase
{
    /**
     * `AclEntry.supportedOperations(CLUSTER)` in full: the bits 5, 7, 8, 9, 10, 11 and 12 of `AclOperation` -
     * CREATE, ALTER, DESCRIBE, CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS and IDEMPOTENT_WRITE.
     *
     * The 2.8.2 broker answered it because it ran without an authorizer at all; the node answers the same value
     * because the principal of a PLAINTEXT connection is `ANONYMOUS`, one of its `super.users`. A principal the
     * `StandardAuthorizer` has no acl for - the SASL user `acltest` - is answered the code 0, the same broker
     * list and the bit field **0**, which is what a refusal of this api looks like.
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
            'a super user may perform every operation AclEntry.supportedOperations(CLUSTER) names'
        );
    }

    /**
     * The node answers which half of the cluster it described (KIP-919, version 1 and above)
     */
    public function testTheNodeAnswersTheBrokerEndpointType(): void
    {
        $described = $this->admin->describeCluster();

        self::assertSame(EndpointType::Broker, $described->endpointType);
        self::assertFalse($described->describesControllers());
        self::assertArrayHasKey(
            $described->controllerId,
            $described->nodes,
            'a KRaft node names a BROKER as the controller of a DescribeCluster answer, never its real controller'
        );
    }

    /**
     * A broker listener refuses the controller endpoints with the 114 that KIP-919 added for it
     */
    public function testAskingABrokerForTheControllersIsRefusedWithTheMismatchedEndpointType(): void
    {
        try {
            $this->admin->describeCluster(false, EndpointType::Controller);
            self::fail('a broker listener does not describe the controllers of the cluster');
        } catch (MismatchedEndpointTypeException $exception) {
            self::assertSame(KafkaException::MISMATCHED_ENDPOINT_TYPE, $exception->getCode());
            self::assertStringContainsString(
                'The request was sent to an endpoint of type BROKER, but we wanted an endpoint of type CONTROLLER',
                $exception->getMessage()
            );
        }
    }

    /**
     * An endpoint type the api does not define is the 115, and the refusal carries nothing of the cluster
     */
    public function testAnEndpointTypeTheApiDoesNotDefineIsRefusedWithTheUnsupportedEndpointType(): void
    {
        foreach ([0, 3] as $endpointType) {
            $stream = $this->connect();
            new DescribeClusterRequest(false, 'kafka-client-t1-cluster', 60 + $endpointType, $endpointType)
                ->writeTo($stream);
            $answer = DescribeClusterResponse::unpack($stream);

            self::assertSame(KafkaException::UNSUPPORTED_ENDPOINT_TYPE, $answer->errorCode);
            self::assertSame("Unsupported endpoint type {$endpointType}", $answer->errorMessage);
            self::assertSame('', $answer->clusterId, 'a refusal carries no cluster id');
            self::assertSame(-1, $answer->controllerId);
            self::assertSame([], $answer->brokers);
            self::assertSame(
                EndpointType::Broker->value,
                $answer->endpointType,
                'the schema default, never the type that was asked for'
            );
            self::assertInstanceOf(
                UnsupportedEndpointTypeException::class,
                KafkaException::fromCode($answer->errorCode, ['error' => (string) $answer->errorMessage])
            );
        }
    }

    /**
     * The node still serves the version 0, whose answer is the version 1 answer minus the endpoint type
     */
    public function testTheVersionBelowIsTheSameAnswerOneByteShorter(): void
    {
        $stream = $this->connect();
        new DescribeClusterRequestV0(false, 'kafka-client-t1-cluster', 61)->writeTo($stream);
        $below = DescribeClusterResponseV0::unpack($stream);

        $stream = $this->connect();
        new DescribeClusterRequestV1(false, 'kafka-client-t1-cluster', 62)->writeTo($stream);
        $above = DescribeClusterResponseV1::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $below->errorCode);
        self::assertSame($above->clusterId, $below->clusterId);
        self::assertSame($above->controllerId, $below->controllerId);
        self::assertSame(array_keys($above->brokers), array_keys($below->brokers));
        self::assertSame(
            $above->getMessageSize() - 1,
            $below->getMessageSize(),
            'the endpoint_type of KIP-919 is the only byte the version added'
        );
    }

    /**
     * The client sends the version 2, and every broker of its answer carries the `is_fenced` of KIP-1073
     */
    public function testTheVersionTwoAnswersTheFencedFlagOfEveryBroker(): void
    {
        foreach ([false, true] as $includeFencedBrokers) {
            $stream  = $this->connect();
            $request = new DescribeClusterRequest(
                false,
                'kafka-client-t1-cluster',
                63,
                EndpointType::Broker,
                $includeFencedBrokers
            );
            $request->writeTo($stream);
            $answer = DescribeClusterResponse::unpack($stream);

            self::assertSame(2, $request->getApiVersion());
            self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
            self::assertNotSame([], $answer->brokers);
            foreach ($answer->brokers as $broker) {
                self::assertFalse($broker->isFenced, 'the broker that answers is not fenced, asked for or not');
            }
        }

        $stream = $this->connect();
        new DescribeClusterRequestV1(false, 'kafka-client-t1-cluster', 64)->writeTo($stream);
        $version1 = DescribeClusterResponseV1::unpack($stream);

        self::assertSame(
            $version1->getMessageSize() + count($version1->brokers),
            $answer->getMessageSize(),
            'the version 2 answer is the version 1 answer plus the one flag of every broker'
        );
    }

    /**
     * The admin api asks for the fenced brokers, and a one-node cluster has none to report
     */
    public function testTheDescriptionListsNoFencedBrokerOnAOneNodeCluster(): void
    {
        $withFenced = $this->admin->describeCluster(true, EndpointType::Broker, true);
        $without    = $this->admin->describeCluster();

        self::assertSame(array_keys($without->nodes), array_keys($withFenced->nodes));
        self::assertSame([], $withFenced->fencedNodeIds);
        self::assertFalse($withFenced->isFenced($withFenced->controllerId));
        self::assertSame(self::ALL_CLUSTER_OPERATIONS, $withFenced->authorizedOperations);
    }

    /**
     * A broker listener refuses the controller endpoints with the 114, the fenced flag or not
     *
     * The Java admin client refuses `includeFencedBrokers` towards a controller endpoint on the client side
     * ("Cannot request fenced brokers from controller endpoint"); a broker listener answers the 114 of the
     * endpoint type before it looks at the flag.
     */
    public function testTheControllersWithTheFencedFlagAreTheMismatchedEndpointType(): void
    {
        $stream = $this->connect();
        new DescribeClusterRequest(false, 'kafka-client-t1-cluster', 65, EndpointType::Controller, true)
            ->writeTo($stream);
        $answer = DescribeClusterResponse::unpack($stream);

        self::assertSame(KafkaException::MISMATCHED_ENDPOINT_TYPE, $answer->errorCode);
        self::assertSame([], $answer->brokers);
    }

    /**
     * The acl flag is the only field of the version 0 body: the two frames differ in exactly one byte
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
