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

namespace Protocol\Kafka\Tests\Unit\Protocol\Request;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\ClusterDescription;
use Protocol\Kafka\Admin\EndpointType;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ClientMetricsResource;
use Protocol\Kafka\Protocol\Request\DescribeClusterRequest;
use Protocol\Kafka\Protocol\Request\DescribeClusterRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeClusterRequestV1;
use Protocol\Kafka\Protocol\Request\DescribeClusterResponse;
use Protocol\Kafka\Protocol\Request\DescribeClusterResponseV1;
use Protocol\Kafka\Protocol\Request\GetTelemetrySubscriptionsRequest;
use Protocol\Kafka\Protocol\Request\GetTelemetrySubscriptionsResponse;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesRequestV0;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesResponseV0;
use Protocol\Kafka\Protocol\Request\PushTelemetryRequest;
use Protocol\Kafka\Protocol\Request\PushTelemetryResponse;

/**
 * Byte-exact tests for what Kafka 3.7 added to this surface: the `endpoint_type` of DescribeCluster v1 (KIP-919)
 * and the three client-metrics apis of KIP-714, which this package carries as wire classes and nothing else.
 *
 * The frames are the ones the 3.9.2 KRaft node really sent and accepted; `tests/Compliance` replays them from
 * `docs/protocol/vectors/`, and this class asserts what a caller sees of them - the version a class sends, the
 * one byte that separates the two DescribeCluster versions, and the shape of every client-metrics message.
 *
 * @see docs/protocol/4.3.md, sections "The endpoint type of KIP-919 (v1)" and "Client metrics (KIP-714) — wire
 *      only"
 */
#[CoversClass(DescribeClusterRequest::class)]
#[CoversClass(DescribeClusterRequestV1::class)]
#[CoversClass(DescribeClusterResponse::class)]
#[CoversClass(DescribeClusterResponseV1::class)]
#[CoversClass(EndpointType::class)]
#[CoversClass(ClusterDescription::class)]
#[CoversClass(GetTelemetrySubscriptionsRequest::class)]
#[CoversClass(GetTelemetrySubscriptionsResponse::class)]
#[CoversClass(PushTelemetryRequest::class)]
#[CoversClass(PushTelemetryResponse::class)]
#[CoversClass(ListClientMetricsResourcesRequestV0::class)]
#[CoversClass(ListClientMetricsResourcesResponseV0::class)]
#[CoversClass(ClientMetricsResource::class)]
final class ClientMetricsAndEndpointTypeTest extends TestCase
{
    /**
     * The client instance id the node generated for the vectors of key 71.
     */
    private const string INSTANCE_ID_HEX = '70a047ee23ba439881180f585897cfb2';

    /**
     * The DescribeCluster **v1** answer of the node: the error code 0, the endpoint type 1, the cluster id, the
     * controller id 1 and the one broker.
     */
    private const string CLUSTER_RESPONSE_V1_HEX = '00000042'
        . '00000e75'
        . '00'
        . '00000000'
        . '0000'
        . '00'
        . '01'
        . '17' . '5f71364361375f615139796d61694f6a4e756e576b41'
        . '00000001'
        . '02'
        . '00000001' . '0a' . '3132372e302e302e31' . '00002384' . '00'
        . '00'
        . '80000000'
        . '00';

    /**
     * The **114** of an `endpoint_type` 2 asked on a broker listener: the refusal alone, with an empty cluster id,
     * the controller id -1 and no broker at all.
     */
    private const string CLUSTER_RESPONSE_V1_MISMATCHED_HEX = '00000078'
        . '00000e77'
        . '00'
        . '00000000'
        . '0072'
        . '61' . '5468652072657175657374207761732073656e7420746f20616e20656e64706f696e74206f662074'
        . '7970652042524f4b45522c206275742077652077616e74656420616e20656e64706f696e74206f662'
        . '07479706520434f4e54524f4c4c4552'
        . '01'
        . '01'
        . 'ffffffff'
        . '01'
        . '80000000'
        . '00';

    /**
     * The answer of key 71 for an instance no `client-metrics` resource matches.
     */
    private const string SUBSCRIPTIONS_RESPONSE_HEX = '0000002f'
        . '00000e88'
        . '00'
        . '00000000'
        . '0000'
        . self::INSTANCE_ID_HEX
        . '5e529f42'
        . '05' . '04' . '03' . '01' . '02'
        . '000493e0'
        . '00100000'
        . '01'
        . '01'
        . '00';

    /**
     * The answer of key 74 while one `client-metrics` resource exists.
     */
    private const string RESOURCES_RESPONSE_HEX = '0000001c'
        . '00000e7f'
        . '00'
        . '00000000'
        . '0000'
        . '02'
        . '0e' . '74312d33372d6d657472696373'
        . '00'
        . '00';

    /**
     * The `endpoint_type` is the only byte that separates the versions 0 and 1 (Kafka 4.0 appended a third)
     */
    public function testTheEndpointTypeIsTheOneByteVersionOneAdded(): void
    {
        $version0 = new DescribeClusterRequestV0(false, 'test', 9);
        $version1 = new DescribeClusterRequestV1(false, 'test', 9);

        self::assertSame(0, $version0->getApiVersion());
        self::assertSame(1, $version1->getApiVersion());
        self::assertSame(ApiKeys::DESCRIBE_CLUSTER, $version1->getApiKey());
        self::assertSame(
            1,
            strlen((string) $version1) - strlen((string) $version0),
            'the version 1 frame is the version 0 frame plus the endpoint type'
        );
        self::assertSame(EndpointType::Broker, $version1->getEndpointType());
        self::assertSame(1, $version1->getEndpointTypeId(), 'the default of the field is the brokers');
        self::assertStringEndsWith('0100', bin2hex((string) $version1), 'the type, then the tag buffer of the body');
    }

    /**
     * A caller may name the controllers, or any byte at all, and the request carries it verbatim
     */
    public function testTheRequestCarriesEveryEndpointTypeVerbatim(): void
    {
        $controllers = new DescribeClusterRequestV1(false, 'test', 9, EndpointType::Controller);
        $nonsense    = new DescribeClusterRequestV1(false, 'test', 9, 3);

        self::assertSame(2, $controllers->getEndpointTypeId());
        self::assertSame(EndpointType::Controller, $controllers->getEndpointType());
        self::assertStringEndsWith('0200', bin2hex((string) $controllers));

        self::assertSame(3, $nonsense->getEndpointTypeId());
        self::assertSame(
            EndpointType::Unknown,
            $nonsense->getEndpointType(),
            'EndpointType.fromId maps every other byte to UNKNOWN'
        );
        self::assertStringEndsWith('0300', bin2hex((string) $nonsense));
    }

    /**
     * @return iterable<string, array{0: int, 1: EndpointType}>
     */
    public static function endpointTypes(): iterable
    {
        yield 'unknown'    => [0, EndpointType::Unknown];
        yield 'broker'     => [1, EndpointType::Broker];
        yield 'controller' => [2, EndpointType::Controller];
        yield 'nonsense'   => [3, EndpointType::Unknown];
        yield 'negative'   => [-1, EndpointType::Unknown];
    }

    #[DataProvider('endpointTypes')]
    public function testTheEnumMapsEveryByteTheWayTheJavaClientDoes(int $id, EndpointType $expected): void
    {
        self::assertSame($expected, EndpointType::fromId($id));
    }

    /**
     * The version 1 answer names the type it really described, and a description says which half it holds
     */
    public function testTheAnswerCarriesTheDescribedEndpointType(): void
    {
        $response = DescribeClusterResponseV1::unpack(
            new StringStream((string) hex2bin(self::CLUSTER_RESPONSE_V1_HEX))
        );

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(1, $response->endpointType);
        self::assertSame(EndpointType::Broker, $response->getEndpointType());
        self::assertSame('_q6Ca7_aQ9ymaiOjNunWkA', $response->clusterId);
        self::assertSame(1, $response->controllerId);
        self::assertSame([1], array_keys($response->brokers));
        self::assertSame(self::CLUSTER_RESPONSE_V1_HEX, bin2hex((string) $response));

        $description = new ClusterDescription('c', 1, [], ClusterDescription::OPERATIONS_NOT_REQUESTED, EndpointType::Controller);

        self::assertTrue($description->describesControllers());
        self::assertFalse(new ClusterDescription('c', 1, [])->describesControllers());
    }

    /**
     * A refusal of KIP-919 carries the code and the message and nothing else of the cluster
     */
    public function testAMismatchedEndpointTypeIsAnsweredWithoutACluster(): void
    {
        $response = DescribeClusterResponseV1::unpack(
            new StringStream((string) hex2bin(self::CLUSTER_RESPONSE_V1_MISMATCHED_HEX))
        );

        self::assertSame(KafkaException::MISMATCHED_ENDPOINT_TYPE, $response->errorCode);
        self::assertSame(
            'The request was sent to an endpoint of type BROKER, but we wanted an endpoint of type CONTROLLER',
            $response->errorMessage
        );
        self::assertSame(1, $response->endpointType, 'the schema default, not the type that was asked for');
        self::assertSame('', $response->clusterId);
        self::assertSame(-1, $response->controllerId);
        self::assertSame([], $response->brokers);
        self::assertSame(self::CLUSTER_RESPONSE_V1_MISMATCHED_HEX, bin2hex((string) $response));
    }

    /**
     * The whole body of a GetTelemetrySubscriptions request is one uuid, and the first one is the zero uuid
     */
    public function testTheTelemetrySubscriptionRequestIsOneUuid(): void
    {
        $first = new GetTelemetrySubscriptionsRequest(Uuid::ZERO, 'test', 9);
        $next  = new GetTelemetrySubscriptionsRequest((string) hex2bin(self::INSTANCE_ID_HEX), 'test', 9);

        self::assertSame(ApiKeys::GET_TELEMETRY_SUBSCRIPTIONS, $first->getApiKey());
        self::assertSame(0, $first->getApiVersion());
        self::assertTrue(GetTelemetrySubscriptionsRequest::isFlexible());
        self::assertSame(Uuid::ZERO, $first->getClientInstanceId());
        self::assertStringEndsWith('00000000000000000000000000000000' . '00', bin2hex((string) $first));
        self::assertStringEndsWith(self::INSTANCE_ID_HEX . '00', bin2hex((string) $next));
        self::assertSame(strlen((string) $first), strlen((string) $next), 'a uuid is 16 bytes either way');
    }

    /**
     * The subscription of an instance no resource matches: the defaults of the broker, and no metric
     */
    public function testAnEmptySubscriptionIsTheDefaultsAndNoMetric(): void
    {
        $response = GetTelemetrySubscriptionsResponse::unpack(
            new StringStream((string) hex2bin(self::SUBSCRIPTIONS_RESPONSE_HEX))
        );

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(self::INSTANCE_ID_HEX, bin2hex($response->clientInstanceId));
        self::assertSame(1582473026, $response->subscriptionId, 'never a sentinel, always a checksum');
        self::assertSame([4, 3, 1, 2], $response->acceptedCompressionTypes, 'zstd, lz4, gzip, snappy');
        self::assertSame(300000, $response->pushIntervalMs);
        self::assertSame(1048576, $response->telemetryMaxBytes);
        self::assertTrue($response->deltaTemporality);
        self::assertSame([], $response->requestedMetrics, 'an empty array is "no metric", not "every metric"');
        self::assertSame(self::SUBSCRIPTIONS_RESPONSE_HEX, bin2hex((string) $response));
    }

    /**
     * A push carries the instance, the subscription it answers, the codec and the opaque blob
     */
    public function testAPushCarriesTheBlobAsPlainBytes(): void
    {
        $push = new PushTelemetryRequest(
            (string) hex2bin(self::INSTANCE_ID_HEX),
            1582473026,
            "\x0a\x00",
            false,
            PushTelemetryRequest::COMPRESSION_NONE,
            'test',
            9
        );

        self::assertSame(ApiKeys::PUSH_TELEMETRY, $push->getApiKey());
        self::assertSame(self::INSTANCE_ID_HEX, bin2hex($push->getClientInstanceId()));
        self::assertSame(1582473026, $push->getSubscriptionId());
        self::assertFalse($push->isTerminating());
        self::assertSame(0, $push->getCompressionType());
        self::assertSame("\x0a\x00", $push->getMetrics());
        self::assertStringEndsWith(
            self::INSTANCE_ID_HEX . '5e529f42' . '00' . '00' . '03' . '0a00' . '00',
            bin2hex((string) $push),
            'the compact byte field announces its length as 2 + 1'
        );

        $empty = new PushTelemetryRequest((string) hex2bin(self::INSTANCE_ID_HEX), 1582473026, '', true, 0, 'test', 9);

        self::assertTrue($empty->isTerminating());
        self::assertStringEndsWith('01' . '00' . '01' . '00', bin2hex((string) $empty), 'terminating, no codec, no bytes');
    }

    /**
     * The answer of a push is a throttle time and an error code, and nothing else
     */
    public function testThePushAnswerIsAThrottleTimeAndAnErrorCode(): void
    {
        $accepted = PushTelemetryResponse::unpack(new StringStream((string) hex2bin('0000000c00000e920000000000000000')));
        $refused  = PushTelemetryResponse::unpack(new StringStream((string) hex2bin('0000000c00000e940000000000007500')));

        self::assertSame(KafkaException::NO_ERROR, $accepted->errorCode);
        self::assertSame(0, $accepted->throttleTimeMs);
        self::assertSame(KafkaException::UNKNOWN_SUBSCRIPTION_ID, $refused->errorCode);
        self::assertSame(12, $accepted->getMessageSize(), 'twelve bytes behind the size field');
    }

    /**
     * ListClientMetricsResources v0 is the one request of this protocol without a body (v1 of Kafka 4.1 has one)
     */
    public function testTheResourceListRequestHasNoBodyAtAll(): void
    {
        $request = new ListClientMetricsResourcesRequestV0('test', 9);
        $header  = bin2hex((string) $request);

        self::assertSame(ApiKeys::LIST_CLIENT_METRICS_RESOURCES, $request->getApiKey());
        self::assertSame('00000010004a000000000009000474657374' . '00' . '00', $header);
        self::assertStringEndsWith('0000', $header, 'the tag buffer of the header and the one of the body');
    }

    /**
     * The answer names the resources and nothing about what they contain
     */
    public function testTheResourceListAnswersNamesOnly(): void
    {
        $response = ListClientMetricsResourcesResponseV0::unpack(
            new StringStream((string) hex2bin(self::RESOURCES_RESPONSE_HEX))
        );

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertSame(['t1-37-metrics'], array_keys($response->clientMetricsResources));
        self::assertSame('t1-37-metrics', $response->clientMetricsResources['t1-37-metrics']->name);
        self::assertSame(self::RESOURCES_RESPONSE_HEX, bin2hex((string) $response));

        $empty = ListClientMetricsResourcesResponseV0::unpack(
            new StringStream((string) hex2bin('0000000d00000e7e000000000000000100'))
        );

        self::assertSame([], $empty->clientMetricsResources, 'a cluster without a client-metrics resource');
    }
}
